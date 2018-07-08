
--DROP TABLE IF EXISTS elt_waze.elt_run_stats; -- This line needs to be commented out in PROD
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


INSERT INTO elt_waze.elt_run_stats
SELECT stg.elt_run_id,
       (select table_id from elt_waze.DW_TBL_INFO where TABLE_NAME ilike 'irregularity_alert') table_id,
       'irregularity_alert' AS TABLE_NAME,
       stg.ingested_rows, 
       prs.persisted_rows 
FROM   (SELECT elt_run_id, 
               Count(*) INGESTED_ROWS 
        FROM   dw_waze.stage_irregularity_alert_{{ batchIdValue }} tfi 
        GROUP  BY elt_run_id) stg 
       JOIN (SELECT elt_run_id, 
                    Count(*) PERSISTED_ROWS 
             FROM   dw_waze.int_irregularity_alert_{{ batchIdValue }} tfi 
             GROUP  BY elt_run_id) prs 
         ON stg.elt_run_id = prs.elt_run_id ;

DROP TABLE IF EXISTS stage_irregularity_alert_{{ batchIdValue }};
DROP TABLE IF EXISTS int_irregularity_alert_{{ batchIdValue }};

commit;