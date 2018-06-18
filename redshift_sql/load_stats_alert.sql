
--DROP TABLE IF EXISTS etl_waze.elt_run_stats; -- This line needs to be commented out in PROD
CREATE TABLE IF NOT EXISTS etl_waze.elt_run_stats
(
  ETL_RUN_ID VARCHAR(50) ENCODE lzo,
  TABLE_ID    SMALLINT ENCODE lzo,
  TOTAL_ROWS_INGESTED           INT ENCODE lzo,
  TOTAL_DISTINCT           INT ENCODE lzo,
  ELT_START_TIME  TIMESTAMP WITHOUT TIME ZONE ENCODE lzo,
  ELT_END_TIME    TIMESTAMP WITHOUT TIME ZONE ENCODE lzo
  )
DISTSTYLE ALL
SORTKEY ( ETL_RUN_ID );


--DROP TABLE IF EXISTS etl_waze.elt_run_state_stats; -- This line needs to be commented out in PROD
CREATE TABLE IF NOT EXISTS etl_waze.elt_run_state_stats
( ETL_RUN_ID VARCHAR(50) ENCODE lzo,
  TABLE_ID SMALLINT ENCODE lzo,
  STATE VARCHAR(10) ENCODE lzo,
  TOTAL_ROWS_INGESTED           INT ENCODE lzo,
  TOTAL_DISTINCT           INT ENCODE lzo,
  ELT_START_TIME  TIMESTAMP WITHOUT TIME ZONE ENCODE lzo,
  ELT_END_TIME    TIMESTAMP WITHOUT TIME ZONE ENCODE lzo
  )
DISTSTYLE ALL
SORTKEY ( ETL_RUN_ID );
COMMIT;


INSERT INTO etl_waze.elt_run_stats
SELECT etl_run_id,
       (select table_id from etl_waze.DW_TBL_INFO where TABLE_NAME ilike 'alert') table_id,
       SUM(1) TOTAL_ROWS_INGESTED,
       COUNT(DISTINCT alert_uuid || alert_type || num_thumbsup || reliability || confidence || report_rating || location_lat || location_lon || pub_millis||pub_utc_timestamp) TOTAL_DISTINCT,
       getdate() ELT_START_TIME,
       getdate() ELT_END_TIME
       FROM dw_waze.stage_alert_{{ batchIdValue }} sa
GROUP BY etl_run_id;

INSERT INTO etl_waze.elt_run_state_stats
SELECT etl_run_id,
       (select table_id from etl_waze.DW_TBL_INFO where TABLE_NAME ilike 'alert') table_id,
       State,
       COUNT(1) TOTAL_ROWS_INGESTED,
       COUNT(DISTINCT alert_uuid || alert_type || num_thumbsup || reliability || confidence || report_rating || location_lat || location_lon || pub_millis||pub_utc_timestamp) TOTAL_DISTINCT
FROM dw_waze.stage_alert_{{ batchIdValue }} sa
GROUP BY etl_run_id,
state;

drop table IF EXISTS dw_waze.stage_alert_{{ batchIdValue }};
drop table IF EXISTS dw_waze.int_alert_{{ batchIdValue }};
drop table IF EXISTS dw_waze.revised_alert_{{ batchIdValue }};
commit;