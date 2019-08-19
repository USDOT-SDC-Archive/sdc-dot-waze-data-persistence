update {{ dw_schema_name }}.stage_irregularity_alert_{{ batchIdValue }}  set elt_run_id={{ batchIdValue }};

--DROP TABLE IF EXISTS {{ elt_schema_name }}.elt_run_stats_realtime; -- Comment out this line in production
CREATE TABLE IF NOT EXISTS {{ elt_schema_name }}.elt_run_stats_realtime
(
  ELT_RUN_ID VARCHAR(50) ENCODE zstd,
  TABLE_ID    SMALLINT ENCODE zstd,
  TABLE_NAME VARCHAR(50) ENCODE zstd,
  INGESTED_ROWS           INT ENCODE zstd,
  ELT_LOAD_TIME  TIMESTAMP WITHOUT TIME ZONE ENCODE delta32k
  )
DISTSTYLE ALL
SORTKEY ( ELT_RUN_ID );


INSERT INTO {{ elt_schema_name }}.elt_run_stats_realtime
SELECT stg.elt_run_id,
       (select table_id from {{ elt_schema_name }}.DW_TBL_INFO where TABLE_NAME ilike 'irregularity_alert') table_id,
       'irregularity_alert' AS TABLE_NAME,
       stg.ingested_rows
FROM   (SELECT elt_run_id,
               Count(*) INGESTED_ROWS
        FROM   {{ dw_schema_name }}.stage_irregularity_alert_{{ batchIdValue }} tfi
        GROUP  BY elt_run_id) stg;

commit;
