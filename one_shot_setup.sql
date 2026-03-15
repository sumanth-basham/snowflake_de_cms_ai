/***********************************************************************
 *  one_shot_setup.sql
 *
 *  A single-file deployment script that executes all 16 setup steps
 *  described in the README for the Snowflake Metadata-Driven File
 *  Ingestion Framework (GCP Platform).
 *
 *  Run as ACCOUNTADMIN (or SYSADMIN with appropriate privileges) in a
 *  Snowflake worksheet or via SnowSQL.
 *
 *  >>> BEFORE RUNNING — search for "TODO" and update all placeholder
 *      values (GCS bucket URLs, storage integration, user names, etc.).
 *
 ***********************************************************************/

----------------------------------------------------------------------
-- STEP 1 — Create Database, Warehouse, and Schemas
--   (ddl/00_database_and_schemas.sql)
----------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

CREATE DATABASE IF NOT EXISTS CMS_AI_DB
  DATA_RETENTION_TIME_IN_DAYS = 7
  COMMENT = 'Metadata-driven file ingestion framework';

CREATE WAREHOUSE IF NOT EXISTS CMS_AI_WH
  WITH WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME  = TRUE
  COMMENT = 'Compute warehouse for CMS AI ingestion framework';

USE DATABASE CMS_AI_DB;

CREATE SCHEMA IF NOT EXISTS RAW       COMMENT = 'Raw landing zone — Stage 1 outputs';
CREATE SCHEMA IF NOT EXISTS STG       COMMENT = 'Standardisation zone — Stage 2 outputs';
CREATE SCHEMA IF NOT EXISTS CURATED   COMMENT = 'Curated / presentation — Stage 3 outputs';
CREATE SCHEMA IF NOT EXISTS CONTROL   COMMENT = 'Control and metadata tables';
CREATE SCHEMA IF NOT EXISTS AUDIT     COMMENT = 'Audit logging tables';
CREATE SCHEMA IF NOT EXISTS REJECTS   COMMENT = 'Rejected / quarantined rows';
CREATE SCHEMA IF NOT EXISTS UTIL      COMMENT = 'Utility objects — UDFs, stored procedures, stages';

USE WAREHOUSE CMS_AI_WH;

----------------------------------------------------------------------
-- STEP 2 — Create Named File Formats
--   (ddl/01_file_formats.sql)
----------------------------------------------------------------------
USE SCHEMA UTIL;

CREATE OR REPLACE FILE FORMAT FMT_CSV
  TYPE            = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER     = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  EMPTY_FIELD_AS_NULL = TRUE
  NULL_IF         = ('NULL', 'null', '')
  COMMENT         = 'Standard CSV format with header skip';

CREATE OR REPLACE FILE FORMAT FMT_TXT
  TYPE            = CSV
  FIELD_DELIMITER = '|'
  SKIP_HEADER     = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  EMPTY_FIELD_AS_NULL = TRUE
  NULL_IF         = ('NULL', 'null', '')
  COMMENT         = 'Pipe-delimited TXT format';

CREATE OR REPLACE FILE FORMAT FMT_PARQUET
  TYPE            = PARQUET
  SNAPPY_COMPRESSION = TRUE
  COMMENT         = 'Standard Parquet format';

----------------------------------------------------------------------
-- STEP 3 — Create External Stages
--   (ddl/02_external_stages.sql)
--
--   TODO: Replace <YOUR_GCS_BUCKET> and <YOUR_STORAGE_INTEGRATION>
--         with your actual GCS bucket name and Snowflake storage
--         integration name.
----------------------------------------------------------------------
USE SCHEMA UTIL;

-- TODO: Uncomment and customise the storage integration if not yet created.
-- CREATE STORAGE INTEGRATION IF NOT EXISTS GCS_INT
--   TYPE                      = EXTERNAL_STAGE
--   STORAGE_PROVIDER          = 'GCS'
--   ENABLED                   = TRUE
--   STORAGE_ALLOWED_LOCATIONS = ('gcs://<YOUR_GCS_BUCKET>/');

CREATE OR REPLACE STAGE STG_RAW_INGEST
  STORAGE_INTEGRATION = GCS_INT          -- TODO: update integration name
  URL = 'gcs://<YOUR_GCS_BUCKET>/raw/'   -- TODO: update URL
  FILE_FORMAT = FMT_CSV
  COMMENT = 'External stage for raw file landing';

CREATE OR REPLACE STAGE STG_FW_CONFIGS
  STORAGE_INTEGRATION = GCS_INT          -- TODO: update integration name
  URL = 'gcs://<YOUR_GCS_BUCKET>/configs/' -- TODO: update URL
  FILE_FORMAT = (TYPE = CSV)
  COMMENT = 'Stage for framework YAML configuration files';

CREATE OR REPLACE STAGE STG_ARCHIVE
  STORAGE_INTEGRATION = GCS_INT          -- TODO: update integration name
  URL = 'gcs://<YOUR_GCS_BUCKET>/archive/' -- TODO: update URL
  FILE_FORMAT = FMT_CSV
  COMMENT = 'Archive stage for processed files';

----------------------------------------------------------------------
-- STEP 4 — Create Control Tables
--   (ddl/03_control_tables.sql)
----------------------------------------------------------------------
USE SCHEMA CONTROL;

CREATE TABLE IF NOT EXISTS PIPELINE_RUN_LOG (
  RUN_ID          VARCHAR(64)   NOT NULL,
  DATASET_NAME    VARCHAR(256)  NOT NULL,
  CONFIG_PATH     VARCHAR(512),
  RUN_MODE        VARCHAR(64),
  STATUS          VARCHAR(32)   DEFAULT 'STARTED',
  START_TS        TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  END_TS          TIMESTAMP_LTZ,
  ROW_COUNT       NUMBER(18,0),
  ERROR_MESSAGE   VARCHAR(4096),
  COMMENT         = 'Top-level pipeline execution log'
);

CREATE TABLE IF NOT EXISTS BATCH_LOG (
  BATCH_ID        VARCHAR(64)   NOT NULL,
  RUN_ID          VARCHAR(64)   NOT NULL,
  STAGE           VARCHAR(16)   NOT NULL,
  STATUS          VARCHAR(32)   DEFAULT 'STARTED',
  START_TS        TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  END_TS          TIMESTAMP_LTZ,
  ROW_COUNT       NUMBER(18,0),
  ERROR_MESSAGE   VARCHAR(4096),
  COMMENT         = 'Per-stage batch log'
);

CREATE TABLE IF NOT EXISTS FILE_LOG (
  FILE_LOG_ID     VARCHAR(64)   NOT NULL,
  BATCH_ID        VARCHAR(64)   NOT NULL,
  FILE_NAME       VARCHAR(512)  NOT NULL,
  FILE_SIZE       NUMBER(18,0),
  ROW_COUNT       NUMBER(18,0),
  STATUS          VARCHAR(32)   DEFAULT 'PENDING',
  LOADED_TS       TIMESTAMP_LTZ,
  ERROR_MESSAGE   VARCHAR(4096),
  COMMENT         = 'Individual file load tracking'
);

CREATE TABLE IF NOT EXISTS CHUNK_LOG (
  CHUNK_ID        VARCHAR(64)   NOT NULL,
  BATCH_ID        VARCHAR(64)   NOT NULL,
  CHUNK_INDEX     NUMBER(8,0),
  OFFSET_VALUE    VARCHAR(256),
  ROW_COUNT       NUMBER(18,0),
  STATUS          VARCHAR(32)   DEFAULT 'PENDING',
  START_TS        TIMESTAMP_LTZ,
  END_TS          TIMESTAMP_LTZ,
  COMMENT         = 'Delta load chunk tracking'
);

CREATE TABLE IF NOT EXISTS YAML_EXECUTION_LOG (
  EXEC_ID         VARCHAR(64)   NOT NULL,
  RUN_ID          VARCHAR(64)   NOT NULL,
  CONFIG_PATH     VARCHAR(512),
  VALIDATION_STATUS VARCHAR(32),
  PARSED_CONFIG   VARIANT,
  CREATED_TS      TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  ERROR_MESSAGE   VARCHAR(4096),
  COMMENT         = 'YAML parse and validation log'
);

CREATE TABLE IF NOT EXISTS SOURCE_SCHEMA_REGISTRY (
  DATASET_NAME    VARCHAR(256)  NOT NULL,
  SCHEMA_VERSION  NUMBER(8,0)   DEFAULT 1,
  COLUMN_DEFS     VARIANT,
  REGISTERED_TS   TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  ACTIVE          BOOLEAN       DEFAULT TRUE,
  COMMENT         = 'Source schema registry for schema evolution tracking'
);

CREATE TABLE IF NOT EXISTS FRAMEWORK_CONFIG (
  CONFIG_KEY      VARCHAR(256)  NOT NULL,
  CONFIG_VALUE    VARCHAR(4096),
  DESCRIPTION     VARCHAR(1024),
  UPDATED_TS      TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  COMMENT         = 'Key-value framework configuration store'
);

----------------------------------------------------------------------
-- STEP 5 — Create Audit Tables
--   (ddl/04_audit_tables.sql)
----------------------------------------------------------------------
USE SCHEMA AUDIT;

CREATE TABLE IF NOT EXISTS MERGE_AUDIT_LOG (
  AUDIT_ID        VARCHAR(64)   NOT NULL,
  RUN_ID          VARCHAR(64)   NOT NULL,
  TARGET_TABLE    VARCHAR(512),
  MERGE_ACTION    VARCHAR(32),
  ROWS_INSERTED   NUMBER(18,0)  DEFAULT 0,
  ROWS_UPDATED    NUMBER(18,0)  DEFAULT 0,
  ROWS_DELETED    NUMBER(18,0)  DEFAULT 0,
  EXECUTED_TS     TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  COMMENT         = 'Audit trail for MERGE / APPEND / OVERWRITE operations'
);

CREATE TABLE IF NOT EXISTS SCHEMA_DRIFT_LOG (
  DRIFT_ID        VARCHAR(64)   NOT NULL,
  RUN_ID          VARCHAR(64)   NOT NULL,
  DATASET_NAME    VARCHAR(256),
  COLUMN_NAME     VARCHAR(256),
  DRIFT_TYPE      VARCHAR(64),
  OLD_DEFINITION  VARCHAR(1024),
  NEW_DEFINITION  VARCHAR(1024),
  DETECTED_TS     TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  COMMENT         = 'Schema drift/evolution detection log'
);

CREATE TABLE IF NOT EXISTS STAGE3_ACTION_LOG (
  ACTION_ID       VARCHAR(64)   NOT NULL,
  RUN_ID          VARCHAR(64)   NOT NULL,
  ACTION_TYPE     VARCHAR(64),
  TARGET_TABLE    VARCHAR(512),
  SQL_TEXT        VARCHAR(16384),
  STATUS          VARCHAR(32),
  EXECUTED_TS     TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  ERROR_MESSAGE   VARCHAR(4096),
  COMMENT         = 'Stage 3 post-processing action log'
);

CREATE TABLE IF NOT EXISTS VALIDATION_LOG (
  VALIDATION_ID   VARCHAR(64)   NOT NULL,
  RUN_ID          VARCHAR(64)   NOT NULL,
  RULE_NAME       VARCHAR(256),
  COLUMN_NAME     VARCHAR(256),
  EXPECTED_VALUE  VARCHAR(1024),
  ACTUAL_VALUE    VARCHAR(1024),
  ROW_COUNT       NUMBER(18,0),
  STATUS          VARCHAR(32),
  VALIDATED_TS    TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  COMMENT         = 'Runtime data validation results'
);

----------------------------------------------------------------------
-- STEP 6 — Create Reject / Quarantine Tables
--   (ddl/05_reject_tables.sql)
----------------------------------------------------------------------
USE SCHEMA REJECTS;

CREATE TABLE IF NOT EXISTS REJECTED_ROWS (
  REJECT_ID       VARCHAR(64)   NOT NULL,
  RUN_ID          VARCHAR(64)   NOT NULL,
  SOURCE_FILE     VARCHAR(512),
  ROW_NUMBER      NUMBER(18,0),
  RAW_DATA        VARIANT,
  REJECT_REASON   VARCHAR(4096),
  REJECTED_TS     TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  COMMENT         = 'Rows that failed Stage 2 validation'
);

CREATE TABLE IF NOT EXISTS QUARANTINE_FILES (
  QUARANTINE_ID   VARCHAR(64)   NOT NULL,
  RUN_ID          VARCHAR(64)   NOT NULL,
  FILE_NAME       VARCHAR(512),
  ERROR_TYPE      VARCHAR(128),
  ERROR_MESSAGE   VARCHAR(4096),
  QUARANTINED_TS  TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  COMMENT         = 'Files that could not be processed'
);

----------------------------------------------------------------------
-- STEP 7 — Set Up RBAC Roles and Grants
--   (ddl/06_rbac.sql)
--
--   TODO: Adjust user grants to match your team members.
----------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

CREATE ROLE IF NOT EXISTS INGESTION_ADMIN
  COMMENT = 'Full admin access to the ingestion framework';
CREATE ROLE IF NOT EXISTS INGESTION_OPERATOR
  COMMENT = 'Operational access — run pipelines, view logs';
CREATE ROLE IF NOT EXISTS INGESTION_ANALYST
  COMMENT = 'Read-only access to curated outputs';

-- Grant database-level access
GRANT USAGE ON DATABASE CMS_AI_DB TO ROLE INGESTION_ADMIN;
GRANT USAGE ON DATABASE CMS_AI_DB TO ROLE INGESTION_OPERATOR;
GRANT USAGE ON DATABASE CMS_AI_DB TO ROLE INGESTION_ANALYST;

-- Grant warehouse access
GRANT USAGE ON WAREHOUSE CMS_AI_WH TO ROLE INGESTION_ADMIN;
GRANT USAGE ON WAREHOUSE CMS_AI_WH TO ROLE INGESTION_OPERATOR;
GRANT USAGE ON WAREHOUSE CMS_AI_WH TO ROLE INGESTION_ANALYST;

-- INGESTION_ADMIN — full control on all schemas
GRANT ALL PRIVILEGES ON SCHEMA CMS_AI_DB.RAW       TO ROLE INGESTION_ADMIN;
GRANT ALL PRIVILEGES ON SCHEMA CMS_AI_DB.STG       TO ROLE INGESTION_ADMIN;
GRANT ALL PRIVILEGES ON SCHEMA CMS_AI_DB.CURATED   TO ROLE INGESTION_ADMIN;
GRANT ALL PRIVILEGES ON SCHEMA CMS_AI_DB.CONTROL   TO ROLE INGESTION_ADMIN;
GRANT ALL PRIVILEGES ON SCHEMA CMS_AI_DB.AUDIT     TO ROLE INGESTION_ADMIN;
GRANT ALL PRIVILEGES ON SCHEMA CMS_AI_DB.REJECTS   TO ROLE INGESTION_ADMIN;
GRANT ALL PRIVILEGES ON SCHEMA CMS_AI_DB.UTIL      TO ROLE INGESTION_ADMIN;

-- INGESTION_OPERATOR — usage + read/write on operational schemas
GRANT USAGE ON SCHEMA CMS_AI_DB.RAW       TO ROLE INGESTION_OPERATOR;
GRANT USAGE ON SCHEMA CMS_AI_DB.STG       TO ROLE INGESTION_OPERATOR;
GRANT USAGE ON SCHEMA CMS_AI_DB.CONTROL   TO ROLE INGESTION_OPERATOR;
GRANT USAGE ON SCHEMA CMS_AI_DB.AUDIT     TO ROLE INGESTION_OPERATOR;
GRANT USAGE ON SCHEMA CMS_AI_DB.REJECTS   TO ROLE INGESTION_OPERATOR;
GRANT USAGE ON SCHEMA CMS_AI_DB.UTIL      TO ROLE INGESTION_OPERATOR;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA CMS_AI_DB.RAW     TO ROLE INGESTION_OPERATOR;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA CMS_AI_DB.STG     TO ROLE INGESTION_OPERATOR;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA CMS_AI_DB.CONTROL TO ROLE INGESTION_OPERATOR;
GRANT SELECT, INSERT         ON ALL TABLES IN SCHEMA CMS_AI_DB.AUDIT   TO ROLE INGESTION_OPERATOR;
GRANT SELECT, INSERT         ON ALL TABLES IN SCHEMA CMS_AI_DB.REJECTS TO ROLE INGESTION_OPERATOR;
GRANT USAGE ON ALL FUNCTIONS IN SCHEMA CMS_AI_DB.UTIL TO ROLE INGESTION_OPERATOR;
GRANT USAGE ON ALL PROCEDURES IN SCHEMA CMS_AI_DB.UTIL TO ROLE INGESTION_OPERATOR;

-- INGESTION_ANALYST — read-only on curated + audit
GRANT USAGE  ON SCHEMA CMS_AI_DB.CURATED TO ROLE INGESTION_ANALYST;
GRANT USAGE  ON SCHEMA CMS_AI_DB.AUDIT   TO ROLE INGESTION_ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA CMS_AI_DB.CURATED TO ROLE INGESTION_ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA CMS_AI_DB.AUDIT   TO ROLE INGESTION_ANALYST;

-- Role hierarchy: ADMIN → OPERATOR → ANALYST
GRANT ROLE INGESTION_ANALYST  TO ROLE INGESTION_OPERATOR;
GRANT ROLE INGESTION_OPERATOR TO ROLE INGESTION_ADMIN;
GRANT ROLE INGESTION_ADMIN    TO ROLE SYSADMIN;

-- TODO: Grant roles to your users
-- GRANT ROLE INGESTION_ADMIN    TO USER <your_admin_user>;
-- GRANT ROLE INGESTION_OPERATOR TO USER <your_operator_user>;
-- GRANT ROLE INGESTION_ANALYST  TO USER <your_analyst_user>;

----------------------------------------------------------------------
-- STEP 8 — (Optional) Set Up Snowpipe Auto-Ingestion via GCS Pub/Sub
--   (ddl/07_snowpipe_gcs_pubsub.sql)
--
--   TODO: Uncomment and update the notification integration and pipe
--         definitions if you want auto-ingestion from GCS.
----------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;
USE DATABASE CMS_AI_DB;
USE SCHEMA UTIL;

-- TODO: Uncomment and configure for Snowpipe auto-ingestion.
-- CREATE NOTIFICATION INTEGRATION IF NOT EXISTS GCS_PUBSUB_INT
--   ENABLED   = TRUE
--   TYPE      = QUEUE
--   NOTIFICATION_PROVIDER = GCP_PUBSUB
--   GCP_PUBSUB_SUBSCRIPTION_NAME = 'projects/<YOUR_GCP_PROJECT>/subscriptions/<YOUR_SUBSCRIPTION>';
--
-- CREATE OR REPLACE PIPE RAW.PIPE_AUTO_INGEST
--   AUTO_INGEST = TRUE
--   INTEGRATION = 'GCS_PUBSUB_INT'
--   AS
--   COPY INTO RAW.LANDING_TABLE
--   FROM @UTIL.STG_RAW_INGEST
--   FILE_FORMAT = (FORMAT_NAME = UTIL.FMT_CSV)
--   ON_ERROR = 'CONTINUE';

----------------------------------------------------------------------
-- STEP 9 — Deploy Python UDFs
--   (framework/python_udfs/yaml_parser.sql)
--   (framework/python_udfs/schema_validator.sql)
----------------------------------------------------------------------
USE ROLE SYSADMIN;
USE DATABASE CMS_AI_DB;
USE SCHEMA UTIL;
USE WAREHOUSE CMS_AI_WH;

-- 9a: YAML Parser UDF — converts YAML text to Snowflake VARIANT
CREATE OR REPLACE FUNCTION PARSE_YAML(YAML_TEXT VARCHAR)
  RETURNS VARIANT
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.8'
  PACKAGES = ('pyyaml')
  HANDLER = 'parse_yaml'
AS
$$
import yaml, json

def parse_yaml(yaml_text: str) -> str:
    """Parse a YAML string and return a JSON string for Snowflake VARIANT."""
    parsed = yaml.safe_load(yaml_text)
    return json.dumps(parsed)
$$;

-- 9b: Schema Validator UDF — validates a parsed config against the schema contract
CREATE OR REPLACE FUNCTION VALIDATE_YAML_SCHEMA(CONFIG_JSON VARCHAR, SCHEMA_JSON VARCHAR)
  RETURNS VARIANT
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.8'
  PACKAGES = ('jsonschema')
  HANDLER = 'validate_schema'
AS
$$
import json

def validate_schema(config_json: str, schema_json: str) -> str:
    """Validate a config dict against a JSON-schema and return result."""
    from jsonschema import validate, ValidationError
    config = json.loads(config_json)
    schema = json.loads(schema_json)
    try:
        validate(instance=config, schema=schema)
        return json.dumps({"valid": True, "errors": []})
    except ValidationError as e:
        return json.dumps({"valid": False, "errors": [str(e.message)]})
$$;

----------------------------------------------------------------------
-- STEP 10 — Deploy Utility Stored Procedures
--   (framework/utils/log_writer.sql)
--   (framework/utils/chunk_planner.sql)
--   (framework/utils/field_validator.sql)
--   (framework/utils/sql_generator.sql)
--   (framework/utils/merge_generator.sql)
----------------------------------------------------------------------

-- 10a: Log Writer — centralised logging helper
CREATE OR REPLACE PROCEDURE LOG_WRITER(
  P_TABLE_NAME VARCHAR,
  P_LOG_DATA   VARIANT
)
  RETURNS VARCHAR
  LANGUAGE SQL
  EXECUTE AS CALLER
AS
$$
BEGIN
  LET v_columns VARCHAR := '';
  LET v_values  VARCHAR := '';
  LET v_sql     VARCHAR := '';

  -- Build dynamic INSERT from the VARIANT keys
  FOR rec IN (SELECT KEY, VALUE FROM TABLE(FLATTEN(INPUT => P_LOG_DATA))) DO
    v_columns := v_columns || rec.KEY || ',';
    v_values  := v_values  || '''' || rec.VALUE::VARCHAR || ''',';
  END FOR;

  v_columns := RTRIM(v_columns, ',');
  v_values  := RTRIM(v_values, ',');
  v_sql     := 'INSERT INTO ' || P_TABLE_NAME || ' (' || v_columns || ') VALUES (' || v_values || ')';

  EXECUTE IMMEDIATE v_sql;
  RETURN 'LOG_WRITER: row inserted into ' || P_TABLE_NAME;
END;
$$;

-- 10b: Chunk Planner — plans delta load chunks based on watermark
CREATE OR REPLACE PROCEDURE CHUNK_PLANNER(
  P_RUN_ID        VARCHAR,
  P_BATCH_ID      VARCHAR,
  P_DATASET_NAME  VARCHAR,
  P_CHUNK_SIZE    NUMBER,
  P_WATERMARK_COL VARCHAR,
  P_SOURCE_TABLE  VARCHAR
)
  RETURNS VARCHAR
  LANGUAGE SQL
  EXECUTE AS CALLER
AS
$$
BEGIN
  LET v_min_val VARCHAR;
  LET v_max_val VARCHAR;
  LET v_sql     VARCHAR;

  -- Get the current watermark range
  v_sql := 'SELECT MIN(' || P_WATERMARK_COL || ')::VARCHAR, MAX(' || P_WATERMARK_COL || ')::VARCHAR FROM ' || P_SOURCE_TABLE;
  LET cur RESULTSET := (EXECUTE IMMEDIATE v_sql);
  LET c CURSOR FOR cur;
  OPEN c;
  FETCH c INTO v_min_val, v_max_val;
  CLOSE c;

  -- Insert a single chunk record (extend for multi-chunk logic as needed)
  INSERT INTO CONTROL.CHUNK_LOG (CHUNK_ID, BATCH_ID, CHUNK_INDEX, OFFSET_VALUE, STATUS, START_TS)
    VALUES (
      UUID_STRING(), :P_BATCH_ID, 1,
      :v_min_val || '|' || :v_max_val,
      'PLANNED', CURRENT_TIMESTAMP()
    );

  RETURN 'CHUNK_PLANNER: 1 chunk planned for ' || P_DATASET_NAME;
END;
$$;

-- 10c: Field Validator — validates that field references in the config resolve
CREATE OR REPLACE PROCEDURE FIELD_VALIDATOR(
  P_RUN_ID   VARCHAR,
  P_CONFIG   VARIANT,
  P_TABLE    VARCHAR
)
  RETURNS VARIANT
  LANGUAGE SQL
  EXECUTE AS CALLER
AS
$$
DECLARE
  v_missing ARRAY DEFAULT ARRAY_CONSTRUCT();
  v_col     VARCHAR;
BEGIN
  FOR rec IN (
    SELECT VALUE::VARCHAR AS COL_NAME
    FROM TABLE(FLATTEN(INPUT => P_CONFIG, PATH => 'columns'))
  ) DO
    v_col := rec.COL_NAME;
    -- Check if column exists in INFORMATION_SCHEMA
    IF (
      (SELECT COUNT(*)
       FROM INFORMATION_SCHEMA.COLUMNS
       WHERE TABLE_NAME = UPPER(SPLIT_PART(:P_TABLE, '.', -1))
         AND COLUMN_NAME = UPPER(:v_col)) = 0
    ) THEN
      v_missing := ARRAY_APPEND(v_missing, v_col);
    END IF;
  END FOR;

  RETURN OBJECT_CONSTRUCT(
    'valid', ARRAY_SIZE(v_missing) = 0,
    'missing_columns', v_missing
  );
END;
$$;

-- 10d: SQL Generator — generates type-conversion SELECT expressions
CREATE OR REPLACE PROCEDURE SQL_GENERATOR(
  P_COLUMN_DEFS VARIANT
)
  RETURNS VARCHAR
  LANGUAGE SQL
  EXECUTE AS CALLER
AS
$$
DECLARE
  v_select_list VARCHAR DEFAULT '';
BEGIN
  FOR rec IN (
    SELECT f.VALUE:name::VARCHAR       AS col_name,
           f.VALUE:source_type::VARCHAR AS src_type,
           f.VALUE:target_type::VARCHAR AS tgt_type
    FROM TABLE(FLATTEN(INPUT => P_COLUMN_DEFS)) f
  ) DO
    IF (v_select_list != '') THEN
      v_select_list := v_select_list || ', ';
    END IF;

    IF (rec.src_type != rec.tgt_type) THEN
      v_select_list := v_select_list || 'TRY_CAST(' || rec.col_name || ' AS ' || rec.tgt_type || ') AS ' || rec.col_name;
    ELSE
      v_select_list := v_select_list || rec.col_name;
    END IF;
  END FOR;

  RETURN v_select_list;
END;
$$;

-- 10e: Merge Generator — executes MERGE / APPEND / OVERWRITE into target
CREATE OR REPLACE PROCEDURE MERGE_GENERATOR(
  P_RUN_ID      VARCHAR,
  P_MODE        VARCHAR,   -- 'MERGE' | 'APPEND' | 'OVERWRITE'
  P_SOURCE      VARCHAR,
  P_TARGET      VARCHAR,
  P_KEY_COLUMNS VARIANT,
  P_ALL_COLUMNS VARIANT
)
  RETURNS VARCHAR
  LANGUAGE SQL
  EXECUTE AS CALLER
AS
$$
DECLARE
  v_sql VARCHAR;
BEGIN
  CASE P_MODE
    WHEN 'OVERWRITE' THEN
      v_sql := 'INSERT OVERWRITE INTO ' || P_TARGET || ' SELECT * FROM ' || P_SOURCE;
    WHEN 'APPEND' THEN
      v_sql := 'INSERT INTO ' || P_TARGET || ' SELECT * FROM ' || P_SOURCE;
    WHEN 'MERGE' THEN
      -- Build MERGE statement
      LET v_on_clause VARCHAR := '';
      FOR k IN (SELECT VALUE::VARCHAR AS key_col FROM TABLE(FLATTEN(INPUT => P_KEY_COLUMNS))) DO
        IF (v_on_clause != '') THEN
          v_on_clause := v_on_clause || ' AND ';
        END IF;
        v_on_clause := v_on_clause || 'tgt.' || k.key_col || ' = src.' || k.key_col;
      END FOR;

      LET v_update_set VARCHAR := '';
      LET v_insert_cols VARCHAR := '';
      LET v_insert_vals VARCHAR := '';
      FOR c IN (SELECT VALUE::VARCHAR AS col FROM TABLE(FLATTEN(INPUT => P_ALL_COLUMNS))) DO
        IF (v_update_set != '') THEN
          v_update_set  := v_update_set  || ', ';
          v_insert_cols := v_insert_cols || ', ';
          v_insert_vals := v_insert_vals || ', ';
        END IF;
        v_update_set  := v_update_set  || 'tgt.' || c.col || ' = src.' || c.col;
        v_insert_cols := v_insert_cols || c.col;
        v_insert_vals := v_insert_vals || 'src.' || c.col;
      END FOR;

      v_sql := 'MERGE INTO ' || P_TARGET || ' tgt USING ' || P_SOURCE || ' src ON ' || v_on_clause
            || ' WHEN MATCHED THEN UPDATE SET ' || v_update_set
            || ' WHEN NOT MATCHED THEN INSERT (' || v_insert_cols || ') VALUES (' || v_insert_vals || ')';
    ELSE
      RETURN 'ERROR: Unsupported mode ' || P_MODE;
  END CASE;

  EXECUTE IMMEDIATE v_sql;

  -- Log to MERGE_AUDIT_LOG
  INSERT INTO AUDIT.MERGE_AUDIT_LOG (AUDIT_ID, RUN_ID, TARGET_TABLE, MERGE_ACTION, EXECUTED_TS)
    VALUES (UUID_STRING(), :P_RUN_ID, :P_TARGET, :P_MODE, CURRENT_TIMESTAMP());

  RETURN 'MERGE_GENERATOR: ' || P_MODE || ' completed for ' || P_TARGET;
END;
$$;

----------------------------------------------------------------------
-- STEP 11 — Deploy Config Loader and Stage Handlers
--   (framework/config_loader.sql)
--   (framework/stage1_handler.sql)
--   (framework/stage2_handler.sql)
--   (framework/stage3_handler.sql)
----------------------------------------------------------------------

-- 11a: Config Loader — reads a YAML file from stage, parses, and validates it
CREATE OR REPLACE PROCEDURE CONFIG_LOADER(
  P_RUN_ID      VARCHAR,
  P_CONFIG_PATH VARCHAR
)
  RETURNS VARIANT
  LANGUAGE SQL
  EXECUTE AS CALLER
AS
$$
DECLARE
  v_yaml_text VARCHAR;
  v_config    VARIANT;
BEGIN
  -- Read YAML from the framework stage
  LET v_sql VARCHAR := 'SELECT $1 FROM @UTIL.STG_FW_CONFIGS/' || P_CONFIG_PATH || ' (FILE_FORMAT => ''UTIL.FMT_TXT'')';
  LET cur RESULTSET := (EXECUTE IMMEDIATE v_sql);
  LET c CURSOR FOR cur;
  OPEN c;
  FETCH c INTO v_yaml_text;
  CLOSE c;

  -- Parse YAML to VARIANT
  v_config := PARSE_JSON(UTIL.PARSE_YAML(v_yaml_text));

  -- Log the parsed config
  INSERT INTO CONTROL.YAML_EXECUTION_LOG (EXEC_ID, RUN_ID, CONFIG_PATH, VALIDATION_STATUS, PARSED_CONFIG, CREATED_TS)
    VALUES (UUID_STRING(), :P_RUN_ID, :P_CONFIG_PATH, 'PARSED', :v_config, CURRENT_TIMESTAMP());

  RETURN v_config;
END;
$$;

-- 11b: Stage 1 Handler — raw ingestion via COPY INTO
CREATE OR REPLACE PROCEDURE STAGE1_HANDLER(
  P_RUN_ID  VARCHAR,
  P_CONFIG  VARIANT
)
  RETURNS VARCHAR
  LANGUAGE SQL
  EXECUTE AS CALLER
AS
$$
DECLARE
  v_dataset   VARCHAR;
  v_stage     VARCHAR;
  v_target    VARCHAR;
  v_format    VARCHAR;
  v_batch_id  VARCHAR;
BEGIN
  v_dataset := P_CONFIG:dataset_name::VARCHAR;
  v_stage   := P_CONFIG:source:stage::VARCHAR;
  v_target  := 'RAW.' || P_CONFIG:source:raw_table::VARCHAR;
  v_format  := P_CONFIG:source:file_format::VARCHAR;
  v_batch_id := UUID_STRING();

  -- Log batch start
  INSERT INTO CONTROL.BATCH_LOG (BATCH_ID, RUN_ID, STAGE, STATUS, START_TS)
    VALUES (:v_batch_id, :P_RUN_ID, 'STAGE1', 'RUNNING', CURRENT_TIMESTAMP());

  -- COPY INTO raw table
  LET v_copy VARCHAR := 'COPY INTO ' || v_target
    || ' FROM @' || v_stage
    || ' FILE_FORMAT = (FORMAT_NAME = ''' || v_format || ''')'
    || ' ON_ERROR = ''CONTINUE''';
  EXECUTE IMMEDIATE v_copy;

  -- Log batch end
  UPDATE CONTROL.BATCH_LOG
    SET STATUS = 'SUCCESS', END_TS = CURRENT_TIMESTAMP()
    WHERE BATCH_ID = :v_batch_id;

  RETURN 'STAGE1_HANDLER: raw load complete for ' || v_dataset;
EXCEPTION
  WHEN OTHER THEN
    UPDATE CONTROL.BATCH_LOG
      SET STATUS = 'FAILED', END_TS = CURRENT_TIMESTAMP(), ERROR_MESSAGE = SQLERRM
      WHERE BATCH_ID = :v_batch_id;
    RETURN 'STAGE1_HANDLER ERROR: ' || SQLERRM;
END;
$$;

-- 11c: Stage 2 Handler — standardisation and validation
CREATE OR REPLACE PROCEDURE STAGE2_HANDLER(
  P_RUN_ID  VARCHAR,
  P_CONFIG  VARIANT
)
  RETURNS VARCHAR
  LANGUAGE SQL
  EXECUTE AS CALLER
AS
$$
DECLARE
  v_dataset   VARCHAR;
  v_raw_table VARCHAR;
  v_stg_table VARCHAR;
  v_batch_id  VARCHAR;
BEGIN
  v_dataset   := P_CONFIG:dataset_name::VARCHAR;
  v_raw_table := 'RAW.' || P_CONFIG:source:raw_table::VARCHAR;
  v_stg_table := 'STG.' || P_CONFIG:staging:stg_table::VARCHAR;
  v_batch_id  := UUID_STRING();

  -- Log batch start
  INSERT INTO CONTROL.BATCH_LOG (BATCH_ID, RUN_ID, STAGE, STATUS, START_TS)
    VALUES (:v_batch_id, :P_RUN_ID, 'STAGE2', 'RUNNING', CURRENT_TIMESTAMP());

  -- Generate typed SELECT via SQL_GENERATOR
  LET v_select VARCHAR;
  CALL SQL_GENERATOR(:P_CONFIG:staging:column_defs) INTO v_select;

  -- Insert valid rows into staging
  LET v_sql VARCHAR := 'INSERT INTO ' || v_stg_table || ' SELECT ' || v_select || ' FROM ' || v_raw_table;
  EXECUTE IMMEDIATE v_sql;

  -- Log batch end
  UPDATE CONTROL.BATCH_LOG
    SET STATUS = 'SUCCESS', END_TS = CURRENT_TIMESTAMP()
    WHERE BATCH_ID = :v_batch_id;

  RETURN 'STAGE2_HANDLER: standardisation complete for ' || v_dataset;
EXCEPTION
  WHEN OTHER THEN
    UPDATE CONTROL.BATCH_LOG
      SET STATUS = 'FAILED', END_TS = CURRENT_TIMESTAMP(), ERROR_MESSAGE = SQLERRM
      WHERE BATCH_ID = :v_batch_id;
    RETURN 'STAGE2_HANDLER ERROR: ' || SQLERRM;
END;
$$;

-- 11d: Stage 3 Handler — curated post-processing
CREATE OR REPLACE PROCEDURE STAGE3_HANDLER(
  P_RUN_ID  VARCHAR,
  P_CONFIG  VARIANT
)
  RETURNS VARCHAR
  LANGUAGE SQL
  EXECUTE AS CALLER
AS
$$
DECLARE
  v_dataset    VARCHAR;
  v_stg_table  VARCHAR;
  v_cur_table  VARCHAR;
  v_mode       VARCHAR;
  v_batch_id   VARCHAR;
BEGIN
  v_dataset   := P_CONFIG:dataset_name::VARCHAR;
  v_stg_table := 'STG.' || P_CONFIG:staging:stg_table::VARCHAR;
  v_cur_table := 'CURATED.' || P_CONFIG:curated:curated_table::VARCHAR;
  v_mode      := UPPER(P_CONFIG:curated:load_mode::VARCHAR);
  v_batch_id  := UUID_STRING();

  -- Log batch start
  INSERT INTO CONTROL.BATCH_LOG (BATCH_ID, RUN_ID, STAGE, STATUS, START_TS)
    VALUES (:v_batch_id, :P_RUN_ID, 'STAGE3', 'RUNNING', CURRENT_TIMESTAMP());

  -- Delegate to MERGE_GENERATOR
  LET v_result VARCHAR;
  CALL MERGE_GENERATOR(
    :P_RUN_ID,
    :v_mode,
    :v_stg_table,
    :v_cur_table,
    :P_CONFIG:curated:key_columns,
    :P_CONFIG:curated:all_columns
  ) INTO v_result;

  -- Log batch end
  UPDATE CONTROL.BATCH_LOG
    SET STATUS = 'SUCCESS', END_TS = CURRENT_TIMESTAMP()
    WHERE BATCH_ID = :v_batch_id;

  RETURN 'STAGE3_HANDLER: ' || v_mode || ' complete for ' || v_dataset || ' — ' || v_result;
EXCEPTION
  WHEN OTHER THEN
    UPDATE CONTROL.BATCH_LOG
      SET STATUS = 'FAILED', END_TS = CURRENT_TIMESTAMP(), ERROR_MESSAGE = SQLERRM
      WHERE BATCH_ID = :v_batch_id;
    RETURN 'STAGE3_HANDLER ERROR: ' || SQLERRM;
END;
$$;

----------------------------------------------------------------------
-- STEP 12 — Deploy the Master Runner
--   (framework/master_runner.sql)
----------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE UTIL.MASTER_RUNNER(
  P_DATASET_FILE  VARCHAR,
  P_CONFIG_PATH   VARCHAR,
  P_RUN_MODE      VARCHAR   -- e.g. 'file_ingestion'
)
  RETURNS VARCHAR
  LANGUAGE SQL
  EXECUTE AS CALLER
AS
$$
DECLARE
  v_run_id  VARCHAR;
  v_config  VARIANT;
  v_result  VARCHAR;
BEGIN
  v_run_id := UUID_STRING();

  -- Log pipeline start
  INSERT INTO CONTROL.PIPELINE_RUN_LOG (RUN_ID, DATASET_NAME, CONFIG_PATH, RUN_MODE, STATUS, START_TS)
    VALUES (:v_run_id, :P_DATASET_FILE, :P_CONFIG_PATH, :P_RUN_MODE, 'STARTED', CURRENT_TIMESTAMP());

  -- Load and parse the YAML config
  CALL CONFIG_LOADER(:v_run_id, :P_CONFIG_PATH) INTO v_config;

  -- Stage 1 — Raw ingestion
  CALL STAGE1_HANDLER(:v_run_id, :v_config) INTO v_result;
  IF (v_result LIKE '%ERROR%') THEN
    UPDATE CONTROL.PIPELINE_RUN_LOG SET STATUS = 'FAILED', END_TS = CURRENT_TIMESTAMP(), ERROR_MESSAGE = :v_result WHERE RUN_ID = :v_run_id;
    RETURN v_result;
  END IF;

  -- Stage 2 — Standardisation
  CALL STAGE2_HANDLER(:v_run_id, :v_config) INTO v_result;
  IF (v_result LIKE '%ERROR%') THEN
    UPDATE CONTROL.PIPELINE_RUN_LOG SET STATUS = 'FAILED', END_TS = CURRENT_TIMESTAMP(), ERROR_MESSAGE = :v_result WHERE RUN_ID = :v_run_id;
    RETURN v_result;
  END IF;

  -- Stage 3 — Curated output
  CALL STAGE3_HANDLER(:v_run_id, :v_config) INTO v_result;
  IF (v_result LIKE '%ERROR%') THEN
    UPDATE CONTROL.PIPELINE_RUN_LOG SET STATUS = 'FAILED', END_TS = CURRENT_TIMESTAMP(), ERROR_MESSAGE = :v_result WHERE RUN_ID = :v_run_id;
    RETURN v_result;
  END IF;

  -- Mark success
  UPDATE CONTROL.PIPELINE_RUN_LOG
    SET STATUS = 'SUCCESS', END_TS = CURRENT_TIMESTAMP()
    WHERE RUN_ID = :v_run_id;

  RETURN 'MASTER_RUNNER: pipeline completed successfully — RUN_ID=' || v_run_id;
END;
$$;

----------------------------------------------------------------------
-- STEP 13 — (Optional) Deploy Snowpark Python Stored Procedures
--
--   Snowpark stored procedures (framework/snowpark/*.py) must be
--   deployed through a Snowpark Python session, SnowSQL, or the
--   Snowflake VS Code extension — they cannot be included in this
--   pure SQL script.
--
--   See README Step 13 for details.
----------------------------------------------------------------------

----------------------------------------------------------------------
-- STEP 14 — Upload YAML Configs to the Framework Stage
--
--   Run these PUT commands from SnowSQL or a local Snowflake client
--   (PUT is not supported inside worksheets on all interfaces):
--
--   PUT file://configs/schema.yaml               @UTIL.STG_FW_CONFIGS/          AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
--   PUT file://configs/datasets/claims_txt.yaml   @UTIL.STG_FW_CONFIGS/datasets/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
--   PUT file://configs/datasets/orders_csv.yaml   @UTIL.STG_FW_CONFIGS/datasets/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
--   PUT file://configs/datasets/customers_parquet.yaml @UTIL.STG_FW_CONFIGS/datasets/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
----------------------------------------------------------------------

----------------------------------------------------------------------
-- STEP 15 — Run the Pipeline
--
--   Execute the master runner for each dataset:
----------------------------------------------------------------------
-- CALL UTIL.MASTER_RUNNER('claims_txt.yaml',       'datasets/claims_txt.yaml',       'file_ingestion');
-- CALL UTIL.MASTER_RUNNER('orders_csv.yaml',        'datasets/orders_csv.yaml',        'file_ingestion');
-- CALL UTIL.MASTER_RUNNER('customers_parquet.yaml', 'datasets/customers_parquet.yaml', 'file_ingestion');

----------------------------------------------------------------------
-- STEP 16 — (Optional) Set Up Orchestration — Snowflake Tasks
--   (orchestration/tasks_setup.sql)
----------------------------------------------------------------------

-- Task: Claims TXT delta load — runs every 15 minutes
CREATE OR REPLACE TASK UTIL.TASK_CLAIMS_TXT_DELTA
  WAREHOUSE = CMS_AI_WH
  SCHEDULE  = '15 MINUTE'
AS
  CALL UTIL.MASTER_RUNNER('claims_txt.yaml', 'datasets/claims_txt.yaml', 'file_ingestion');

-- Task: Orders CSV full load — runs daily at 06:00 UTC
CREATE OR REPLACE TASK UTIL.TASK_ORDERS_CSV_FULL
  WAREHOUSE = CMS_AI_WH
  SCHEDULE  = 'USING CRON 0 6 * * * UTC'
AS
  CALL UTIL.MASTER_RUNNER('orders_csv.yaml', 'datasets/orders_csv.yaml', 'file_ingestion');

-- Task: Customers Parquet delta load — runs every 30 minutes
CREATE OR REPLACE TASK UTIL.TASK_CUSTOMERS_PARQUET_DELTA
  WAREHOUSE = CMS_AI_WH
  SCHEDULE  = '30 MINUTE'
AS
  CALL UTIL.MASTER_RUNNER('customers_parquet.yaml', 'datasets/customers_parquet.yaml', 'file_ingestion');

-- Tasks are created in SUSPENDED state. Uncomment to resume:
-- ALTER TASK UTIL.TASK_CLAIMS_TXT_DELTA       RESUME;
-- ALTER TASK UTIL.TASK_ORDERS_CSV_FULL         RESUME;
-- ALTER TASK UTIL.TASK_CUSTOMERS_PARQUET_DELTA RESUME;

----------------------------------------------------------------------
-- DONE — All 16 steps have been executed.
--
--   Next steps:
--     1. Search this file for "TODO" and update all placeholders.
--     2. Upload YAML configs (Step 14) via SnowSQL.
--     3. Uncomment the CALL statements in Step 15 to run the pipeline.
--     4. Optionally resume the Tasks in Step 16.
----------------------------------------------------------------------
