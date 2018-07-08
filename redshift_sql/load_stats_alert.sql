
--DROP TABLE IF EXISTS etl_waze.elt_run_stats; -- This line needs to be commented out in PROD
CREATE TABLE IF NOT EXISTS elt_waze.elt_run_stats
(
  ETL_RUN_ID VARCHAR(50) ENCODE zstd,
  TABLE_ID    SMALLINT ENCODE zstd,
  TABLE_NAME VARCHAR(50) ENCODE zstd,
  INGESTED_ROWS           INT ENCODE zstd,
  PERSISTED_ROWS           INT ENCODE zstd,
  ELT_START_TIME  TIMESTAMP WITHOUT TIME ZONE ENCODE delta32k,
  ELT_END_TIME    TIMESTAMP WITHOUT TIME ZONE ENCODE delta32k
  )
DISTSTYLE ALL
SORTKEY ( ETL_RUN_ID );


--DROP TABLE IF EXISTS etl_waze.elt_run_state_stats; -- This line needs to be commented out in PROD
CREATE TABLE IF NOT EXISTS elt_waze.elt_run_state_stats
( ETL_RUN_ID VARCHAR(50) ENCODE zstd,
  TABLE_ID SMALLINT ENCODE zstd,
  TABLE_NAME VARCHAR(50) ENCODE zstd,
  STATE VARCHAR(10) ENCODE zstd,
  INGESTED_ROWS           INT ENCODE zstd,
  PERSISTED_ROWS           INT ENCODE zstd,
  ELT_START_TIME  TIMESTAMP WITHOUT TIME ZONE ENCODE delta32k,
  ELT_END_TIME    TIMESTAMP WITHOUT TIME ZONE ENCODE delta32k
  )
DISTSTYLE ALL
SORTKEY ( ETL_RUN_ID );


INSERT INTO etl_waze.elt_run_stats
SELECT stg.etl_run_id,
       (select table_id from etl_waze.DW_TBL_INFO where TABLE_NAME ilike 'alert') table_id,
       'alert' AS TABLE_NAME,
       stg.ingested_rows, 
       prs.persisted_rows 
FROM   (SELECT etl_run_id, 
               Count(*) INGESTED_ROWS 
        FROM   dw_waze.stage_alert_{{ batchIdValue }} tfi 
        GROUP  BY etl_run_id) stg 
       JOIN (SELECT etl_run_id, 
                    Count(*) PERSISTED_ROWS 
             FROM   dw_waze.int_alert_{{ batchIdValue }} tfi 
             GROUP  BY etl_run_id) prs 
         ON stg.etl_run_id = prs.etl_run_id ;

INSERT INTO etl_waze.elt_run_state_stats
SELECT stg.etl_run_id, 
       (select table_id from etl_waze.DW_TBL_INFO where TABLE_NAME ilike 'alert') table_id,
       'alert' AS TABLE_NAME,
       stg.state, 
       stg.ingested_rows, 
       prs.persisted_rows 
FROM   (SELECT etl_run_id, 
               state, 
               Count(*) INGESTED_ROWS 
        FROM   dw_waze.stage_alert_{{ batchIdValue }} tfi 
        GROUP  BY etl_run_id, 
                  state) stg 
       JOIN (SELECT etl_run_id, 
                    state, 
                    Count(*) PERSISTED_ROWS 
             FROM   dw_waze.int_alert_{{ batchIdValue }} tfi 
             GROUP  BY etl_run_id, 
                       state) prs 
         ON stg.etl_run_id = prs.etl_run_id 
            AND stg.state = prs.state ;

DROP TABLE if exists dw_waze.int_alert_{{ batchIdValue }};
DROP TABLE if exists dw_waze.stage_alert_{{ batchIdValue }};
DROP TABLE if exists dw_waze.revised_alert_{{ batchIdValue }};

commit;

vacuum dw_waze.alert;