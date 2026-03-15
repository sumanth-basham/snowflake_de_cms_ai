-- =============================================================================
-- 01_file_formats.sql
-- Purpose : Create named file formats for every supported source file type.
--           These formats are referenced by COPY INTO statements in Stage1.
--           All formats are created in the UTIL schema.
-- =============================================================================

USE DATABASE INGESTION_FW;
USE SCHEMA UTIL;

-- -----------------------------------------------------------------------
-- CSV  –  comma-delimited, double-quote enclosure
-- -----------------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT FW_CSV_FORMAT
  TYPE                        = 'CSV'
  FIELD_DELIMITER             = ','
  RECORD_DELIMITER            = '\n'
  SKIP_HEADER                 = 1
  FIELD_OPTIONALLY_ENCLOSED_BY= '"'
  ESCAPE_UNENCLOSED_FIELD     = '\\'
  NULL_IF                     = ('NULL', 'null', '')
  TRIM_SPACE                  = TRUE
  DATE_FORMAT                 = 'AUTO'
  TIMESTAMP_FORMAT            = 'AUTO'
  ENCODING                    = 'UTF8'
  EMPTY_FIELD_AS_NULL         = TRUE
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
  COMMENT = 'Default CSV file format – all columns land as strings in Stage1';

-- -----------------------------------------------------------------------
-- TXT  –  pipe-delimited (most common for claims / health data)
-- -----------------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT FW_TXT_PIPE_FORMAT
  TYPE                        = 'CSV'
  FIELD_DELIMITER             = '|'
  RECORD_DELIMITER            = '\n'
  SKIP_HEADER                 = 1
  NULL_IF                     = ('NULL', 'null', '', 'N/A')
  TRIM_SPACE                  = TRUE
  DATE_FORMAT                 = 'AUTO'
  TIMESTAMP_FORMAT            = 'AUTO'
  ENCODING                    = 'UTF8'
  EMPTY_FIELD_AS_NULL         = TRUE
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
  COMMENT = 'Pipe-delimited TXT file format – all columns land as strings in Stage1';

-- -----------------------------------------------------------------------
-- TXT  –  tab-delimited variant
-- -----------------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT FW_TXT_TAB_FORMAT
  TYPE                        = 'CSV'
  FIELD_DELIMITER             = '\t'
  RECORD_DELIMITER            = '\n'
  SKIP_HEADER                 = 1
  NULL_IF                     = ('NULL', 'null', '')
  TRIM_SPACE                  = TRUE
  DATE_FORMAT                 = 'AUTO'
  TIMESTAMP_FORMAT            = 'AUTO'
  ENCODING                    = 'UTF8'
  EMPTY_FIELD_AS_NULL         = TRUE
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
  COMMENT = 'Tab-delimited TXT file format – all columns land as strings in Stage1';

-- -----------------------------------------------------------------------
-- PARQUET  –  Snappy compressed, binary columns read as text strings
-- Note     : Parquet columns are read using $1:<col_name>::STRING to
--            preserve Stage1 all-as-string rule.
-- -----------------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT FW_PARQUET_FORMAT
  TYPE                   = 'PARQUET'
  SNAPPY_COMPRESSION     = TRUE
  BINARY_AS_TEXT         = TRUE
  COMMENT = 'Parquet file format – BINARY columns treated as UTF-8 strings';

-- -----------------------------------------------------------------------
-- PARQUET (uncompressed / gzip variant)
-- -----------------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT FW_PARQUET_GZIP_FORMAT
  TYPE                   = 'PARQUET'
  SNAPPY_COMPRESSION     = FALSE
  BINARY_AS_TEXT         = TRUE
  COMMENT = 'Gzip/uncompressed Parquet – BINARY columns treated as UTF-8 strings';
