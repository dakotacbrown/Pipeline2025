"""
db_loader.py

Abstract database loader interface + registry-based dispatcher.

Supports:
- DataFrame -> DB (SQLAlchemy for many DBs; Snowflake via snowflake-sqlalchemy)
- S3 -> Snowflake via COPY INTO (snowflake-connector-python)

The table dict includes database (authoritative).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Optional

import pandas as pd


class LoaderError(Exception):
    """Raised when a load operation fails."""


@dataclass(frozen=True)
class LoadConfig:
    db_type: str
    connection: Dict[str, Any]
    table: Dict[str, Any]
    source: Dict[str, Any]
    copy: Dict[str, Any]

    # IMPORTANT: table dict includes database and is authoritative
    @property
    def database(self) -> Optional[str]:
        return self.table.get("database") or self.connection.get("database")

    @property
    def schema(self) -> Optional[str]:
        return self.table.get("schema") or self.connection.get("schema")

    @property
    def table_name(self) -> str:
        t = self.table.get("table") or self.table.get("name")
        if not t:
            raise LoaderError("Missing table name: expected table.table or table.name")
        return str(t)

    @property
    def fq_table(self) -> str:
        db = self.database
        sch = self.schema
        if db and sch:
            return f"{db}.{sch}.{self.table_name}"
        if sch:
            return f"{sch}.{self.table_name}"
        return self.table_name

    @property
    def mode(self) -> str:
        return str(self.table.get("mode", "append")).lower()

    @property
    def chunksize(self) -> int:
        return int(self.table.get("chunksize", 10_000))

    @property
    def dtype(self) -> Optional[Dict[str, Any]]:
        return self.table.get("dtype")

    # ---- source helpers ----
    @property
    def source_type(self) -> Optional[str]:
        st = self.source.get("type")
        return str(st).lower() if st else None

    @property
    def s3_uri(self) -> str:
        uri = self.source.get("uri")
        if not uri or not str(uri).startswith("s3://"):
            raise LoaderError("source.uri must be an s3://... URI")
        return str(uri)

    @property
    def source_format(self) -> str:
        fmt = self.source.get("format")
        if not fmt:
            raise LoaderError("source.format is required (parquet|csv|json)")
        return str(fmt).lower()


class AbstractDatabaseLoader(ABC):
    """
    Base interface for all database loaders.

    Implementations can choose to support either or both:
      - load_dataframe
      - load_from_s3

    If not supported, raise LoaderError with a clear message.
    """

    @abstractmethod
    def load_dataframe(self, cfg: LoadConfig, df: pd.DataFrame) -> None:
        raise NotImplementedError

    @abstractmethod
    def load_from_s3(self, cfg: LoadConfig) -> None:
        raise NotImplementedError


class GenericDatabaseLoader:
    """
    Registry-based dispatcher.

    If df is provided -> dataframe load.
    If df is None -> file-based load using cfg.source.type (currently s3).
    """

    def __init__(self) -> None:
        self._registry: Dict[str, AbstractDatabaseLoader] = {}

    def register(self, db_type: str, loader: AbstractDatabaseLoader) -> None:
        self._registry[db_type.lower()] = loader

    def load(self, payload: Dict[str, Any], df: Optional[pd.DataFrame] = None) -> None:
        if "db_type" not in payload:
            raise LoaderError("payload must include db_type")

        cfg = LoadConfig(
            db_type=str(payload["db_type"]).lower(),
            connection=dict(payload.get("connection") or {}),
            table=dict(payload.get("table") or {}),
            source=dict(payload.get("source") or {}),
            copy=dict(payload.get("copy") or {}),
        )

        loader = self._registry.get(cfg.db_type)
        if not loader:
            raise LoaderError(
                f"Unsupported db_type='{cfg.db_type}'. Registered: {sorted(self._registry.keys())}"
            )

        if df is not None:
            if not isinstance(df, pd.DataFrame):
                raise LoaderError(f"df must be a pandas DataFrame, got {type(df)}")
            loader.load_dataframe(cfg, df)
            return

        if cfg.source_type == "s3":
            loader.load_from_s3(cfg)
            return

        raise LoaderError("No df provided and no supported source.type found (expected source.type='s3').")


# -------------------------
# SQLAlchemy DataFrame loaders (Postgres/MySQL/SQLite/etc.)
# -------------------------

class SqlAlchemyToSqlLoader(AbstractDatabaseLoader):
    """
    Generic DataFrame->DB loader using SQLAlchemy + pandas.to_sql.
    Subclasses implement build_sqlalchemy_url().
    """

    def build_sqlalchemy_url(self, cfg: LoadConfig) -> str:
        raise NotImplementedError

    def load_dataframe(self, cfg: LoadConfig, df: pd.DataFrame) -> None:
        try:
            import sqlalchemy as sa
        except ImportError as e:
            raise LoaderError("sqlalchemy is required for SqlAlchemyToSqlLoader") from e

        if cfg.mode not in {"append", "replace", "fail"}:
            raise LoaderError("Supported modes for dataframe->SQL are append|replace|fail")

        url = self.build_sqlalchemy_url(cfg)
        engine = sa.create_engine(url)

        try:
            with engine.begin() as conn:
                df.to_sql(
                    name=cfg.table_name,
                    con=conn,
                    schema=cfg.schema,
                    if_exists=cfg.mode,
                    index=False,
                    chunksize=cfg.chunksize,
                    dtype=cfg.dtype,
                    method="multi",
                )
        except Exception as e:
            raise LoaderError(f"Failed loading DataFrame to {cfg.db_type} {cfg.fq_table}") from e

    def load_from_s3(self, cfg: LoadConfig) -> None:
        raise LoaderError(f"{cfg.db_type}: S3 load not implemented for this loader.")


class PostgresLoader(SqlAlchemyToSqlLoader):
    def build_sqlalchemy_url(self, cfg: LoadConfig) -> str:
        c = cfg.connection
        host = c["host"]
        port = c.get("port", 5432)
        db = c["database"]
        user = c["user"]
        password = c["password"]
        return f"postgresql+psycopg://{user}:{password}@{host}:{port}/{db}"


class MySqlLoader(SqlAlchemyToSqlLoader):
    def build_sqlalchemy_url(self, cfg: LoadConfig) -> str:
        c = cfg.connection
        host = c["host"]
        port = c.get("port", 3306)
        db = c["database"]
        user = c["user"]
        password = c["password"]
        return f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}"


class SqliteLoader(SqlAlchemyToSqlLoader):
    def build_sqlalchemy_url(self, cfg: LoadConfig) -> str:
        c = cfg.connection
        path = c.get("path", ":memory:")
        return f"sqlite:///{path}"


# -------------------------
# Snowflake loader (DataFrame + S3 COPY)
# -------------------------

class SnowflakeLoader(AbstractDatabaseLoader):
    def load_dataframe(self, cfg: LoadConfig, df: pd.DataFrame) -> None:
        if not cfg.database:
            raise LoaderError("Snowflake dataframe load requires table.database")
        if not cfg.schema:
            raise LoaderError("Snowflake dataframe load requires table.schema")

        try:
            import sqlalchemy as sa  # noqa: F401
            import snowflake.sqlalchemy  # noqa: F401
        except ImportError as e:
            raise LoaderError(
                "Snowflake dataframe load requires sqlalchemy + snowflake-sqlalchemy.\n"
                "Install: sqlalchemy snowflake-sqlalchemy"
            ) from e

        c = cfg.connection
        for k in ("account", "user", "password"):
            if k not in c:
                raise LoaderError(f"Missing Snowflake connection key: {k}")

        account = c["account"]
        user = c["user"]
        password = c["password"]
        warehouse = c.get("warehouse")
        role = c.get("role")

        params = []
        if warehouse:
            params.append(f"warehouse={warehouse}")
        if role:
            params.append(f"role={role}")
        query = ("?" + "&".join(params)) if params else ""

        url = f"snowflake://{user}:{password}@{account}/{cfg.database}/{cfg.schema}{query}"

        if cfg.mode not in {"append", "replace", "fail"}:
            raise LoaderError("Snowflake dataframe load supports mode: append|replace|fail")

        try:
            import sqlalchemy as sa
            engine = sa.create_engine(url)
            with engine.begin() as conn:
                df.to_sql(
                    name=cfg.table_name,
                    con=conn,
                    schema=cfg.schema,
                    if_exists=cfg.mode,
                    index=False,
                    chunksize=cfg.chunksize,
                    dtype=cfg.dtype,
                    method="multi",
                )
        except Exception as e:
            raise LoaderError(f"Failed loading DataFrame to Snowflake {cfg.fq_table}") from e

    def load_from_s3(self, cfg: LoadConfig) -> None:
        if cfg.source_type != "s3":
            raise LoaderError(f"SnowflakeLoader only supports source.type='s3', got {cfg.source_type}")

        if not cfg.database:
            raise LoaderError("Snowflake S3 load requires table.database")
        if not cfg.schema:
            raise LoaderError("Snowflake S3 load requires table.schema")

        stage = cfg.copy.get("stage")
        if not stage:
            raise LoaderError("Snowflake S3 load requires copy.stage (e.g. '@MY_EXT_STAGE').")

        rel_path = _s3_uri_to_stage_path(cfg.s3_uri)

        file_format_name = cfg.copy.get("file_format_name")
        pattern = cfg.copy.get("pattern")
        on_error = cfg.copy.get("on_error", "ABORT_STATEMENT")

        if file_format_name:
            file_format_clause = f" FILE_FORMAT = (FORMAT_NAME = {file_format_name})"
        else:
            file_format_clause = f" FILE_FORMAT = ({_inline_snowflake_file_format(cfg)})"

        pattern_clause = f" PATTERN = '{pattern}'" if pattern else ""

        statements: list[str] = [
            f"USE DATABASE {cfg.database};",
            f"USE SCHEMA {cfg.schema};",
        ]

        if cfg.mode == "replace":
            statements.append(f"TRUNCATE TABLE {cfg.table_name};")
        elif cfg.mode == "fail":
            statements.append(
                f"""
                BEGIN
                  IF (SELECT COUNT(*) FROM {cfg.table_name}) > 0 THEN
                    RAISE STATEMENT_ERROR WITH MESSAGE = 'Target table not empty (mode=fail)';
                  END IF;
                END;
                """.strip()
            )
        elif cfg.mode != "append":
            raise LoaderError("Snowflake S3 load supports mode: append|replace|fail")

        copy_sql = f"""
        COPY INTO {cfg.table_name}
        FROM {stage}/{rel_path}
        {file_format_clause}
        {pattern_clause}
        ON_ERROR = {on_error}
        """.strip() + ";"

        statements.append(copy_sql)
        self._execute_snowflake_sql(cfg, statements)

    def _execute_snowflake_sql(self, cfg: LoadConfig, statements: list[str]) -> None:
        try:
            import snowflake.connector
        except ImportError as e:
            raise LoaderError("Snowflake S3 load requires snowflake-connector-python.") from e

        c = cfg.connection
        for k in ("account", "user", "password"):
            if k not in c:
                raise LoaderError(f"Missing Snowflake connection key: {k}")

        conn_kwargs: Dict[str, Any] = {
            "account": c["account"],
            "user": c["user"],
            "password": c["password"],
        }
        if c.get("warehouse"):
            conn_kwargs["warehouse"] = c["warehouse"]
        if c.get("role"):
            conn_kwargs["role"] = c["role"]

        try:
            with snowflake.connector.connect(**conn_kwargs) as conn:
                with conn.cursor() as cur:
                    for stmt in statements:
                        cur.execute(stmt)
        except Exception as e:
            raise LoaderError(f"Snowflake S3 COPY failed for {cfg.fq_table} from {cfg.s3_uri}") from e


# -------------------------
# Helpers
# -------------------------

def _s3_uri_to_stage_path(s3_uri: str) -> str:
    no_scheme = s3_uri.replace("s3://", "", 1)
    parts = no_scheme.split("/", 1)
    if len(parts) == 1:
        return ""
    return parts[1]


def _inline_snowflake_file_format(cfg: LoadConfig) -> str:
    fmt = cfg.source_format
    if fmt == "parquet":
        return "TYPE = PARQUET"
    if fmt == "csv":
        return "TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='\"' SKIP_HEADER=1"
    if fmt == "json":
        return "TYPE = JSON STRIP_OUTER_ARRAY=FALSE"
    raise LoaderError(f"Unsupported source.format='{fmt}' for Snowflake inline FILE_FORMAT")


# -------------------------
# Factory
# -------------------------

def build_default_loader(
    *,
    include_postgres: bool = True,
    include_mysql: bool = True,
    include_sqlite: bool = True,
) -> GenericDatabaseLoader:
    loader = GenericDatabaseLoader()
    loader.register("snowflake", SnowflakeLoader())

    if include_postgres:
        loader.register("postgres", PostgresLoader())
    if include_mysql:
        loader.register("mysql", MySqlLoader())
    if include_sqlite:
        loader.register("sqlite", SqliteLoader())

    return loader
