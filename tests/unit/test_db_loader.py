import sys
import types

import db_loader
import pandas as pd
import pytest
from db_loader import (
    GenericDatabaseLoader,
    LoadConfig,
    LoaderError,
    SnowflakeLoader,
)

# -------------------------
# Helpers / fakes
# -------------------------


class DummyLoader(db_loader.AbstractDatabaseLoader):
    def __init__(self):
        self.df_called = False
        self.s3_called = False
        self.last_cfg = None
        self.last_df = None

    def load_dataframe(self, cfg: LoadConfig, df: pd.DataFrame) -> None:
        self.df_called = True
        self.last_cfg = cfg
        self.last_df = df

    def load_from_s3(self, cfg: LoadConfig) -> None:
        self.s3_called = True
        self.last_cfg = cfg


def _base_payload(db_type="dummy"):
    return {
        "db_type": db_type,
        "connection": {},
        "table": {"database": "RAW", "schema": "PUBLIC", "table": "T"},
        "source": {},
        "copy": {},
    }


# -------------------------
# GenericDatabaseLoader tests
# -------------------------


def test_registry_and_dispatch_dataframe_calls_load_dataframe():
    loader = GenericDatabaseLoader()
    dummy = DummyLoader()
    loader.register("dummy", dummy)

    df = pd.DataFrame([{"a": 1}])
    payload = _base_payload("dummy")

    loader.load(payload, df=df)

    assert dummy.df_called is True
    assert dummy.s3_called is False
    assert isinstance(dummy.last_cfg, LoadConfig)
    assert dummy.last_cfg.db_type == "dummy"
    assert dummy.last_df.equals(df)


def test_registry_and_dispatch_s3_calls_load_from_s3():
    loader = GenericDatabaseLoader()
    dummy = DummyLoader()
    loader.register("dummy", dummy)

    payload = _base_payload("dummy")
    payload["source"] = {
        "type": "s3",
        "uri": "s3://b/p/x.parquet",
        "format": "parquet",
    }

    loader.load(payload)  # no df => file path

    assert dummy.s3_called is True
    assert dummy.df_called is False
    assert dummy.last_cfg.source_type == "s3"


def test_dispatch_unsupported_db_type_raises():
    loader = GenericDatabaseLoader()
    with pytest.raises(LoaderError, match="Unsupported db_type"):
        loader.load(_base_payload("missing"), df=pd.DataFrame([{"a": 1}]))


def test_dispatch_missing_db_type_key_raises():
    loader = GenericDatabaseLoader()
    with pytest.raises(LoaderError, match="payload must include db_type"):
        loader.load({"table": {}}, df=pd.DataFrame([{"a": 1}]))


def test_dispatch_no_df_and_no_supported_source_raises():
    loader = GenericDatabaseLoader()
    dummy = DummyLoader()
    loader.register("dummy", dummy)

    payload = _base_payload("dummy")
    payload["source"] = {}  # no source.type

    with pytest.raises(
        LoaderError, match="No df provided and no supported source.type"
    ):
        loader.load(payload)


# -------------------------
# LoadConfig tests
# -------------------------


def test_loadconfig_table_database_is_authoritative():
    cfg = LoadConfig(
        db_type="snowflake",
        connection={"database": "CONN_DB", "schema": "CONN_SCHEMA"},
        table={"database": "TABLE_DB", "schema": "TABLE_SCHEMA", "table": "X"},
        source={},
        copy={},
    )
    assert cfg.database == "TABLE_DB"
    assert cfg.schema == "TABLE_SCHEMA"
    assert cfg.fq_table == "TABLE_DB.TABLE_SCHEMA.X"


def test_loadconfig_missing_table_name_raises():
    cfg = LoadConfig(
        db_type="x",
        connection={},
        table={"database": "D", "schema": "S"},
        source={},
        copy={},
    )
    with pytest.raises(LoaderError, match="Missing table name"):
        _ = cfg.table_name


# -------------------------
# Helper function tests
# -------------------------


@pytest.mark.parametrize(
    "uri,expected",
    [
        ("s3://bucket/prefix/file.parquet", "prefix/file.parquet"),
        ("s3://bucket/prefix/", "prefix/"),
        ("s3://bucket", ""),
    ],
)
def test_s3_uri_to_stage_path(uri, expected):
    assert db_loader._s3_uri_to_stage_path(uri) == expected


@pytest.mark.parametrize(
    "fmt,expected_snippet",
    [
        ("parquet", "TYPE = PARQUET"),
        ("csv", "TYPE = CSV"),
        ("json", "TYPE = JSON"),
    ],
)
def test_inline_snowflake_file_format(fmt, expected_snippet):
    cfg = LoadConfig(
        db_type="snowflake",
        connection={},
        table={"database": "D", "schema": "S", "table": "T"},
        source={"type": "s3", "uri": "s3://b/p", "format": fmt},
        copy={},
    )
    assert expected_snippet in db_loader._inline_snowflake_file_format(cfg)


def test_inline_snowflake_file_format_unsupported_raises():
    cfg = LoadConfig(
        db_type="snowflake",
        connection={},
        table={"database": "D", "schema": "S", "table": "T"},
        source={"type": "s3", "uri": "s3://b/p", "format": "xml"},
        copy={},
    )
    with pytest.raises(LoaderError, match="Unsupported source.format"):
        db_loader._inline_snowflake_file_format(cfg)


# -------------------------
# SnowflakeLoader S3 COPY tests (no real snowflake dependency)
# -------------------------


def test_snowflake_load_from_s3_requires_database_and_schema():
    sfl = SnowflakeLoader()
    cfg = LoadConfig(
        db_type="snowflake",
        connection={"account": "a", "user": "u", "password": "p"},
        table={"schema": "PUBLIC", "table": "T"},  # missing database
        source={"type": "s3", "uri": "s3://b/p/x.parquet", "format": "parquet"},
        copy={"stage": "@STG"},
    )
    with pytest.raises(LoaderError, match="requires table.database"):
        sfl.load_from_s3(cfg)

    cfg2 = LoadConfig(
        db_type="snowflake",
        connection={"account": "a", "user": "u", "password": "p"},
        table={"database": "RAW", "table": "T"},  # missing schema
        source={"type": "s3", "uri": "s3://b/p/x.parquet", "format": "parquet"},
        copy={"stage": "@STG"},
    )
    with pytest.raises(LoaderError, match="requires table.schema"):
        sfl.load_from_s3(cfg2)


def test_snowflake_load_from_s3_requires_stage():
    sfl = SnowflakeLoader()
    cfg = LoadConfig(
        db_type="snowflake",
        connection={"account": "a", "user": "u", "password": "p"},
        table={"database": "RAW", "schema": "PUBLIC", "table": "T"},
        source={"type": "s3", "uri": "s3://b/p/x.parquet", "format": "parquet"},
        copy={},  # missing stage
    )
    with pytest.raises(LoaderError, match="requires copy.stage"):
        sfl.load_from_s3(cfg)


def test_snowflake_load_from_s3_executes_expected_statements(monkeypatch):
    # Stub snowflake.connector.connect and capture executed SQL
    executed = []

    class FakeCursor:
        def execute(self, stmt):
            executed.append(stmt)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeConn:
        def cursor(self):
            return FakeCursor()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def fake_connect(**kwargs):
        return FakeConn()

    fake_sf = types.SimpleNamespace(
        connector=types.SimpleNamespace(connect=fake_connect)
    )

    # Make import snowflake.connector work:
    monkeypatch.setitem(sys.modules, "snowflake", fake_sf)
    monkeypatch.setitem(sys.modules, "snowflake.connector", fake_sf.connector)

    sfl = SnowflakeLoader()
    cfg = LoadConfig(
        db_type="snowflake",
        connection={
            "account": "acct",
            "user": "usr",
            "password": "pwd",
            "warehouse": "WH",
            "role": "ROLE",
        },
        table={
            "database": "RAW",
            "schema": "PUBLIC",
            "table": "T",
            "mode": "append",
        },
        source={
            "type": "s3",
            "uri": "s3://my-bucket/some/prefix/file.parquet",
            "format": "parquet",
        },
        copy={
            "stage": "@MY_STAGE",
            "pattern": ".*\\.parquet",
            "on_error": "ABORT_STATEMENT",
        },
    )

    sfl.load_from_s3(cfg)

    # Verify the key pieces are issued in order
    assert executed[0].startswith("USE DATABASE RAW")
    assert executed[1].startswith("USE SCHEMA PUBLIC")
    assert "COPY INTO T" in executed[2]
    assert "FROM @MY_STAGE/some/prefix/file.parquet" in executed[2]
    assert "TYPE = PARQUET" in executed[2]
    assert "PATTERN = '.*\\.parquet'" in executed[2]
    assert "ON_ERROR = ABORT_STATEMENT" in executed[2]


def test_snowflake_load_from_s3_replace_truncates_first(monkeypatch):
    executed = []

    class FakeCursor:
        def execute(self, stmt):
            executed.append(stmt)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeConn:
        def cursor(self):
            return FakeCursor()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def fake_connect(**kwargs):
        return FakeConn()

    fake_sf = types.SimpleNamespace(
        connector=types.SimpleNamespace(connect=fake_connect)
    )

    monkeypatch.setitem(sys.modules, "snowflake", fake_sf)
    monkeypatch.setitem(sys.modules, "snowflake.connector", fake_sf.connector)

    sfl = SnowflakeLoader()
    cfg = LoadConfig(
        db_type="snowflake",
        connection={"account": "acct", "user": "usr", "password": "pwd"},
        table={
            "database": "RAW",
            "schema": "PUBLIC",
            "table": "T",
            "mode": "replace",
        },
        source={
            "type": "s3",
            "uri": "s3://b/p/file.parquet",
            "format": "parquet",
        },
        copy={"stage": "@STG"},
    )

    sfl.load_from_s3(cfg)

    assert any(stmt.startswith("TRUNCATE TABLE T") for stmt in executed)
    # TRUNCATE should occur before COPY
    truncate_idx = next(
        i for i, s in enumerate(executed) if s.startswith("TRUNCATE TABLE T")
    )
    copy_idx = next(i for i, s in enumerate(executed) if "COPY INTO T" in s)
    assert truncate_idx < copy_idx


# -------------------------
# SnowflakeLoader DataFrame tests (no real sqlalchemy dependency)
# -------------------------


def test_snowflake_load_dataframe_requires_database_and_schema():
    sfl = SnowflakeLoader()
    df = pd.DataFrame([{"a": 1}])

    cfg_missing_db = LoadConfig(
        db_type="snowflake",
        connection={"account": "a", "user": "u", "password": "p"},
        table={"schema": "PUBLIC", "table": "T"},
        source={},
        copy={},
    )
    with pytest.raises(LoaderError, match="requires table.database"):
        sfl.load_dataframe(cfg_missing_db, df)

    cfg_missing_schema = LoadConfig(
        db_type="snowflake",
        connection={"account": "a", "user": "u", "password": "p"},
        table={"database": "RAW", "table": "T"},
        source={},
        copy={},
    )
    with pytest.raises(LoaderError, match="requires table.schema"):
        sfl.load_dataframe(cfg_missing_schema, df)


def test_snowflake_load_dataframe_uses_sqlalchemy_engine(monkeypatch):
    # Create fake sqlalchemy + snowflake.sqlalchemy modules
    created_urls = []
    to_sql_calls = []

    class FakeBegin:
        def __enter__(self):
            return object()

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeEngine:
        def begin(self):
            return FakeBegin()

    def fake_create_engine(url):
        created_urls.append(url)
        return FakeEngine()

    fake_sqlalchemy = types.SimpleNamespace(create_engine=fake_create_engine)
    fake_snowflake_sqlalchemy = types.SimpleNamespace()

    monkeypatch.setitem(sys.modules, "sqlalchemy", fake_sqlalchemy)
    monkeypatch.setitem(
        sys.modules, "snowflake.sqlalchemy", fake_snowflake_sqlalchemy
    )

    # Monkeypatch DataFrame.to_sql to capture parameters
    original_to_sql = pd.DataFrame.to_sql

    def fake_to_sql(
        self,
        name,
        con,
        schema=None,
        if_exists=None,
        index=None,
        chunksize=None,
        dtype=None,
        method=None,
    ):
        to_sql_calls.append(
            {
                "name": name,
                "schema": schema,
                "if_exists": if_exists,
                "index": index,
                "chunksize": chunksize,
                "dtype": dtype,
                "method": method,
            }
        )
        return None

    monkeypatch.setattr(pd.DataFrame, "to_sql", fake_to_sql, raising=True)

    try:
        sfl = SnowflakeLoader()
        cfg = LoadConfig(
            db_type="snowflake",
            connection={
                "account": "acct",
                "user": "usr",
                "password": "pwd",
                "warehouse": "WH",
                "role": "ROLE",
            },
            table={
                "database": "RAW",
                "schema": "PUBLIC",
                "table": "T",
                "mode": "append",
                "chunksize": 123,
            },
            source={},
            copy={},
        )
        df = pd.DataFrame([{"a": 1}])

        sfl.load_dataframe(cfg, df)

        assert len(created_urls) == 1
        assert created_urls[0].startswith("snowflake://usr:pwd@acct/RAW/PUBLIC")
        assert "warehouse=WH" in created_urls[0]
        assert "role=ROLE" in created_urls[0]

        assert len(to_sql_calls) == 1
        call = to_sql_calls[0]
        assert call["name"] == "T"
        assert call["schema"] == "PUBLIC"
        assert call["if_exists"] == "append"
        assert call["index"] is False
        assert call["chunksize"] == 123
        assert call["method"] == "multi"
    finally:
        # restore DataFrame.to_sql
        monkeypatch.setattr(
            pd.DataFrame, "to_sql", original_to_sql, raising=True
        )
