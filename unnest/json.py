import json
from typing import Any, Optional

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


def unnest_json_column(
    df: pd.DataFrame,
    column: str,
    prefix: Optional[str] = None,
    keep_original: bool = False,
    errors: str = "raise",
    sep: str = "_",
    max_iters: int = 20,
) -> pd.DataFrame:
    """Fully unnest a JSON column (strings, dicts, lists) into flat columns.

    - Deeply flattens nested dicts using underscores (sep)
    - Explodes arrays (including arrays of structs) into multiple rows
    - Preserves rows with empty arrays via explode_outer-like behavior
    - Works with mixed types across rows

    Parameters
    ----------
    df : pd.DataFrame
    column : str
        Name of the column with JSON content.
    prefix : Optional[str]
        Optional prefix to add to generated JSON columns (e.g., "meta_").
    keep_original : bool
        If True, keep the original JSON column.
    errors : {"raise","ignore"}
        Behavior for invalid JSON strings in the column.
    sep : str
        Separator for nested keys (default "_").
    max_iters : int
        Safety cap to prevent infinite loops while flattening.

    Returns
    -------
    pd.DataFrame
    """
    if column not in df.columns:
        raise KeyError(f"Column '{column}' not found.")

    def _parse_cell(x: Any) -> Any:
        if isinstance(x, str):
            try:
                return json.loads(x)
            except Exception:
                if errors == "raise":
                    raise
                return None
        return x

    parsed = df[column].map(_parse_cell)

    # Top-level list handling: duplicate base rows for each top-level element
    records_idx, records_val = [], []
    for idx, val in parsed.items():
        if isinstance(val, list):
            if len(val) == 0:
                records_idx.append(idx)
                records_val.append(None)
            else:
                for item in val:
                    records_idx.append(idx)
                    records_val.append(item)
        else:
            records_idx.append(idx)
            records_val.append(val)

    def _as_dict(v: Any) -> dict:
        if isinstance(v, dict):
            return v
        if v is None:
            return {}
        # scalar -> "value" field so nothing is lost
        return {"value": v}

    dict_records = list(map(_as_dict, records_val))
    # Start with a normalized frame (flattens nested dict keys)
    jdf = pd.json_normalize(dict_records, sep=sep)

    # Track the original row index so we can merge back after any later explodes
    jdf["__src_idx"] = records_idx

    # Iteratively explode arrays and expand dicts until flat
    for _ in range(max_iters):
        changed = False

        # 1) Explode any list columns
        list_cols = [
            c
            for c in jdf.columns
            if jdf[c].map(lambda x: isinstance(x, list)).any()
        ]
        for c in list_cols:
            # Replace empty lists with [None] so we keep the row during explode
            jdf[c] = jdf[c].apply(
                lambda v: [None] if isinstance(v, list) and len(v) == 0 else v
            )
            jdf = jdf.explode(c, ignore_index=True)
            changed = True

        # 2) Expand any dict columns into separate columns
        dict_cols = [
            c
            for c in jdf.columns
            if jdf[c].map(lambda x: isinstance(x, dict)).any()
        ]
        for c in dict_cols:
            # Non-dict values -> {} so columns align
            sub = pd.json_normalize(
                jdf[c].apply(lambda v: v if isinstance(v, dict) else {}),
                sep=sep,
            )
            # Prefix with the parent column name
            sub.columns = [f"{c}{sep}{cc}" for cc in sub.columns]
            jdf = pd.concat([jdf.drop(columns=[c]), sub], axis=1)
            changed = True

        if not changed:
            break
    else:
        # Hit max iters
        raise RuntimeError(
            "Flattening exceeded max_iters; data may be too deeply nested or cyclic."
        )

    # Optional prefix for JSON-derived columns (do NOT prefix the tracking column)
    if prefix:
        rename_map = {
            c: f"{prefix}{c}" for c in jdf.columns if c != "__src_idx"
        }
        jdf = jdf.rename(columns=rename_map)

    # Merge back to base by source index so base columns stay aligned after late explodes
    base = df.copy()
    if not keep_original:
        base = base.drop(columns=[column])
    base_keyed = base.reset_index().rename(columns={"index": "__src_idx"})

    out = base_keyed.merge(jdf, on="__src_idx", how="right")
    out = out.drop(columns=["__src_idx"])
    return out


def _ensure_json_struct(
    df: DataFrame, col: str, schema: Optional[T.DataType]
) -> DataFrame:
    """If `col` is a string, parse it with from_json (schema required for best results).

    If `col` is already a struct/array, leave it as-is.
    If schema is None and type is string, fallback to from_json with MapType(StringType, StringType) to avoid failure.
    """
    dtype = dict(df.dtypes)[col]
    if dtype == "string":
        if schema is None:
            # Fallback: try a permissive map; user can supply a precise schema for better types
            schema = T.MapType(
                T.StringType(), T.StringType(), valueContainsNull=True
            )
        return df.withColumn(col, F.from_json(F.col(col), schema))
    return df


def _flatten_struct_once(df: DataFrame, col: str, sep: str) -> DataFrame:
    """Expand a top-level struct column into separate columns with prefix `col +
    sep`."""
    fields = df.schema[col].dataType
    if not isinstance(fields, T.StructType):
        return df
    selects = [c for c in df.columns if c != col]
    for f in fields.fields:
        selects.append(F.col(f"{col}.{f.name}").alias(f"{col}{sep}{f.name}"))
    return df.select(*[F.col(c) if isinstance(c, str) else c for c in selects])


def _first_array_col(df: DataFrame) -> Optional[str]:
    for f in df.schema.fields:
        if isinstance(f.dataType, T.ArrayType):
            return f.name
    return None


def _first_struct_col(df: DataFrame) -> Optional[str]:
    for f in df.schema.fields:
        if isinstance(f.dataType, T.StructType):
            return f.name
    return None


def _first_map_col(df: DataFrame) -> Optional[str]:
    for f in df.schema.fields:
        if isinstance(f.dataType, T.MapType):
            return f.name
    return None


def unnest_json_column_spark(
    df: DataFrame,
    column: str,
    *,
    schema: Optional[T.DataType] = None,
    prefix: Optional[str] = None,
    keep_original: bool = False,
    sep: str = "_",
    max_iters: int = 50,
) -> DataFrame:
    """
    Fully flatten a JSON column in Spark:
      - Parses strings with from_json (supply `schema` for best typing)
      - Recursively flattens nested structs
      - Sequentially explode_outer arrays (one at a time)
      - Converts maps -> array<struct<key,value>> -> explode -> flatten
    Produces columns WITHOUT the root prefix (e.g., 'a', 'b_c', 'arr_x' not 'payload_a').
    Empty/NULL arrays are preserved as a single NULL row.
    """
    if column not in df.columns:
        raise KeyError(f"Column '{column}' not found.")

    root_prefix = f"{column}{sep}"

    # Keep a copy of the original JSON so keep_original=True works
    base_cols = [c for c in df.columns if c != column]
    dfj = df.select(
        *base_cols, F.col(column).alias("__orig_json__"), F.col(column)
    )

    # Parse to structured type if needed
    dfj = _ensure_json_struct(dfj, column, schema)

    def _strip_root_prefix(d: DataFrame) -> DataFrame:
        # Rename any columns starting with "payload_" (or whatever root is) to drop that prefix
        renames = [c for c in d.columns if c.startswith(root_prefix)]
        for c in renames:
            d = d.withColumnRenamed(c, c[len(root_prefix) :])
        return d

    iter_count = 0
    while iter_count < max_iters:
        iter_count += 1

        progressed = False

        # 1) If the original root column still exists and is a struct, flatten it
        if column in dfj.columns and isinstance(
            dfj.schema[column].dataType, T.StructType
        ):
            dfj = _flatten_struct_once(dfj, column, sep)
            dfj = _strip_root_prefix(dfj)
            progressed = True

        # 2) Flatten any other struct
        if not progressed:
            struct_col = _first_struct_col(dfj)
            if struct_col:
                dfj = _flatten_struct_once(dfj, struct_col, sep)
                # no need to strip root here unless this struct was nested under the root name
                dfj = _strip_root_prefix(dfj)
                progressed = True

        # 3) Explode a single array (preserve [] and NULL as a single NULL row)
        if not progressed:
            arr_col = _first_array_col(dfj)
            if arr_col:
                dfj = dfj.withColumn(
                    arr_col,
                    F.when(F.col(arr_col).isNull(), F.array(F.lit(None)))
                    .when(F.size(F.col(arr_col)) == 0, F.array(F.lit(None)))
                    .otherwise(F.col(arr_col)),
                )
                dfj = dfj.withColumn(arr_col, F.explode_outer(F.col(arr_col)))
                dfj = _strip_root_prefix(dfj)
                progressed = True

        # 4) Map -> entries -> explode -> flatten to *_key, *_value
        if not progressed:
            map_col = _first_map_col(dfj)
            if map_col:
                dfj = dfj.withColumn(map_col, F.map_entries(F.col(map_col)))
                dfj = dfj.withColumn(map_col, F.explode_outer(F.col(map_col)))
                dfj = _flatten_struct_once(
                    dfj, map_col, sep
                )  # yields <map>_key, <map>_value
                dfj = _strip_root_prefix(dfj)
                progressed = True

        if not progressed:
            break
    else:
        raise RuntimeError("Exceeded max_iters during Spark flattening.")

    # Figure out which columns are JSON-derived (exclude base & the temp original)
    json_cols = [
        c for c in dfj.columns if c not in base_cols + ["__orig_json__", column]
    ]

    # Optional prefix for JSON-derived columns (after root stripping)
    if prefix:
        for c in json_cols:
            dfj = dfj.withColumnRenamed(c, f"{prefix}{c}")
        json_cols = [f"{prefix}{c}" for c in json_cols]

    # Build final projection
    select_cols = base_cols[:]  # always keep base
    if keep_original:
        # restore original JSON as the original column name
        dfj = dfj.withColumnRenamed("__orig_json__", column)
        select_cols.append(column)
    else:
        # drop the temp original
        dfj = dfj.drop("__orig_json__")

    # add JSON columns to the end
    select_cols.extend([c for c in dfj.columns if c not in select_cols])

    return dfj.select(*select_cols)
