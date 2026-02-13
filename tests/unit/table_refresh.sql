CREATE SCHEMA IF NOT EXISTS CADET.CONTROL;

CREATE TABLE IF NOT EXISTS CADET.CONTROL.EXTERNAL_TABLE_REFRESH_LIST (
  database_name STRING,
  schema_name   STRING,
  table_name    STRING,
  refresh_path  STRING,                 -- optional; NULL means full table refresh
  is_enabled    BOOLEAN DEFAULT TRUE,
  created_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  updated_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


MERGE INTO CADET.CONTROL.EXTERNAL_TABLE_REFRESH_LIST t
USING (
  SELECT
    'CADET' AS database_name,
    TABLE_SCHEMA AS schema_name,
    TABLE_NAME AS table_name,
    NULL::STRING AS refresh_path,
    TRUE AS is_enabled
  FROM CADET.INFORMATION_SCHEMA.TABLES
  WHERE TABLE_TYPE = 'EXTERNAL TABLE'
    AND TABLE_SCHEMA IN ('BRONZE_REVCLOUD', 'BRONZE_OTHER')
) s
ON  t.database_name = s.database_name
AND t.schema_name   = s.schema_name
AND t.table_name    = s.table_name
WHEN NOT MATCHED THEN
  INSERT (database_name, schema_name, table_name, refresh_path, is_enabled)
  VALUES (s.database_name, s.schema_name, s.table_name, s.refresh_path, s.is_enabled);


CREATE OR REPLACE PROCEDURE CADET.CONTROL.REFRESH_LISTED_EXTERNAL_TABLES_DAILY()
  RETURNS VARIANT
  LANGUAGE SQL
  EXECUTE AS OWNER
AS
$$
DECLARE
  rs RESULTSET;
  results ARRAY;
  stmt STRING;
BEGIN
  results := ARRAY_CONSTRUCT();

  rs := (
    SELECT database_name, schema_name, table_name, refresh_path
    FROM CADET.CONTROL.EXTERNAL_TABLE_REFRESH_LIST_DAILY
    WHERE is_enabled = TRUE
    ORDER BY database_name, schema_name, table_name
  );

  FOR r IN rs DO
    IF r.REFRESH_PATH IS NULL THEN
      stmt := 'ALTER EXTERNAL TABLE "' || r.DATABASE_NAME || '"."' || r.SCHEMA_NAME || '"."' || r.TABLE_NAME || '" REFRESH';
    ELSE
      stmt := 'ALTER EXTERNAL TABLE "' || r.DATABASE_NAME || '"."' || r.SCHEMA_NAME || '"."' || r.TABLE_NAME || '" REFRESH ''' || r.REFRESH_PATH || '''';
    END IF;

    BEGIN
      EXECUTE IMMEDIATE :stmt;

      results := ARRAY_APPEND(
        results,
        OBJECT_CONSTRUCT(
          'table', r.DATABASE_NAME || '.' || r.SCHEMA_NAME || '.' || r.TABLE_NAME,
          'status', 'success',
          'path', r.REFRESH_PATH
        )
      );
    EXCEPTION
      WHEN OTHER THEN
        results := ARRAY_APPEND(
          results,
          OBJECT_CONSTRUCT(
            'table', r.DATABASE_NAME || '.' || r.SCHEMA_NAME || '.' || r.TABLE_NAME,
            'status', 'failed',
            'path', r.REFRESH_PATH,
            'error', SQLERRM
          )
        );
    END;
  END FOR;

  RETURN OBJECT_CONSTRUCT(
    'refreshed_at', CURRENT_TIMESTAMP(),
    'results', results
  );
END;
$$;


CREATE OR REPLACE TASK CADET.CONTROL.REFRESH_EXTERNAL_TABLES_TASK
  WAREHOUSE = ETL_WH
  SCHEDULE = '15 MINUTE'
AS
  CALL CADET.CONTROL.REFRESH_LISTED_EXTERNAL_TABLES();


ALTER TASK CADET.CONTROL.REFRESH_EXTERNAL_TABLES_TASK RESUME;
