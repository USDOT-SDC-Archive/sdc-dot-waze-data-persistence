
--DROP TABLE IF EXISTS {{ elt_schema_name }}.elt_run_stats; -- This line needs to be commented out in PROD
CREATE TABLE IF NOT EXISTS {{ elt_schema_name }}.elt_run_stats
(
  ELT_RUN_ID VARCHAR(50) ENCODE zstd,
  TABLE_ID    SMALLINT ENCODE zstd,
  TABLE_NAME VARCHAR(50) ENCODE zstd,
  INGESTED_ROWS           INT ENCODE zstd,
  PERSISTED_ROWS           INT ENCODE zstd,
  ELT_START_TIME  TIMESTAMP WITHOUT TIME ZONE ENCODE delta32k,
  ELT_END_TIME    TIMESTAMP WITHOUT TIME ZONE ENCODE delta32k
  )
DISTSTYLE ALL
SORTKEY ( ELT_RUN_ID );


--DROP TABLE IF EXISTS {{ elt_schema_name }}.elt_run_state_stats; -- This line needs to be commented out in PROD
CREATE TABLE IF NOT EXISTS {{ elt_schema_name }}.elt_run_state_stats
( ELT_RUN_ID VARCHAR(50) ENCODE zstd,
  TABLE_ID SMALLINT ENCODE zstd,
  TABLE_NAME VARCHAR(50) ENCODE zstd,
  STATE VARCHAR(10) ENCODE zstd,
  INGESTED_ROWS           INT ENCODE zstd,
  PERSISTED_ROWS           INT ENCODE zstd,
  ELT_START_TIME  TIMESTAMP WITHOUT TIME ZONE ENCODE delta32k,
  ELT_END_TIME    TIMESTAMP WITHOUT TIME ZONE ENCODE delta32k
  )
DISTSTYLE ALL
SORTKEY ( ELT_RUN_ID );


INSERT INTO {{ elt_schema_name }}.elt_run_stats
SELECT stg.elt_run_id,
       (select table_id from {{ elt_schema_name }}.DW_TBL_INFO where TABLE_NAME ilike 'alert') table_id,
       'alert' AS TABLE_NAME,
       stg.ingested_rows, 
       prs.persisted_rows 
FROM   (SELECT elt_run_id,
               Count(*) INGESTED_ROWS 
        FROM   {{ dw_schema_name }}.stage_alert_{{ batchIdValue }} tfi
        GROUP  BY elt_run_id) stg
       JOIN (SELECT elt_run_id,
                    Count(*) PERSISTED_ROWS 
             FROM   {{ dw_schema_name }}.int_alert_{{ batchIdValue }} tfi
             GROUP  BY elt_run_id) prs
         ON stg.elt_run_id = prs.elt_run_id ;

INSERT INTO {{ elt_schema_name }}.elt_run_state_stats
SELECT stg.elt_run_id,
       (select table_id from {{ elt_schema_name }}.DW_TBL_INFO where TABLE_NAME ilike 'alert') table_id,
       'alert' AS TABLE_NAME,
       stg.state, 
       stg.ingested_rows, 
       prs.persisted_rows 
FROM   (SELECT elt_run_id,
               state, 
               Count(*) INGESTED_ROWS 
        FROM   {{ dw_schema_name }}.stage_alert_{{ batchIdValue }} tfi
        GROUP  BY elt_run_id,
                  state) stg 
       JOIN (SELECT elt_run_id,
                    state, 
                    Count(*) PERSISTED_ROWS 
             FROM   {{ dw_schema_name }}.int_alert_{{ batchIdValue }} tfi
             GROUP  BY elt_run_id,
                       state) prs 
         ON stg.elt_run_id = prs.elt_run_id
            AND stg.state = prs.state ;

DROP TABLE if exists {{ dw_schema_name }}.int_alert_{{ batchIdValue }};
DROP TABLE if exists {{ dw_schema_name }}.stage_alert_{{ batchIdValue }};
DROP TABLE if exists {{ dw_schema_name }}.revised_alert_{{ batchIdValue }};

commit;

vacuum {{ dw_schema_name }}.alert;