from logging import Logger

import pandas as pd
from pyspark.sql import DataFrame


def remove_duplicates_pandas(
    df: pd.DataFrame,
    log: Logger,
    subset: list[str] | None = None,
    keep: str = "first",
) -> pd.DataFrame:
    """
    Remove duplicate rows from a pandas DataFrame and log results.

    Args:
        df (pd.DataFrame): Input pandas DataFrame.
        log (logging.Logger): Logger instance for recording info.
        subset (list[str] | None): Optional list of column names to consider for identifying duplicates.
                                   If None, all columns are considered.
        keep (str): Determines which duplicates (if any) to keep:
                    - 'first': Keep first occurrence.
                    - 'last': Keep last occurrence.
                    - False: Drop all duplicates.

    Returns:
        pd.DataFrame: DataFrame with duplicates removed.
    """
    initial_count = len(df)
    df_cleaned = df.drop_duplicates(subset=subset, keep=keep).reset_index(
        drop=True
    )
    final_count = len(df_cleaned)
    removed_count = initial_count - final_count

    log.info(
        f"Pandas deduplication complete — initial rows: {initial_count}, "
        f"final rows: {final_count}, duplicates removed: {removed_count}, "
        f"subset used: {subset or 'ALL COLUMNS'}"
    )

    return df_cleaned


def remove_duplicates_spark(
    df: DataFrame, log: Logger, subset: list[str] | None = None
) -> DataFrame:
    """
    Remove duplicate rows from a PySpark DataFrame and log results.

    Args:
        df (DataFrame): Input PySpark DataFrame.
        log (logging.Logger): Logger instance for recording info.
        subset (list[str] | None): Optional list of column names to consider for identifying duplicates.
                                   If None, all columns are considered.

    Returns:
        DataFrame: DataFrame with duplicates removed.
    """
    initial_count = df.count()
    df_cleaned = df.dropDuplicates(subset) if subset else df.distinct()
    final_count = df_cleaned.count()
    removed_count = initial_count - final_count

    log.info(
        f"Spark deduplication complete — initial rows: {initial_count}, "
        f"final rows: {final_count}, duplicates removed: {removed_count}, "
        f"subset used: {subset or 'ALL COLUMNS'}"
    )

    return df_cleaned
