
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
       (select table_id from etl_waze.DW_TBL_INFO where TABLE_NAME ilike 'irregularity') table_id,
       COUNT(1) TOTAL_ROWS_INGESTED,
       COUNT(DISTINCT id||
       irregularity_type||
       street||
       city||
       state||
       country||
       speed||
       regular_speed||
       seconds||
       delay_seconds||
       trend||
       num_thumbsup||
       irregularity_length||
       severity||
       jam_level||
       drivers_count||
       alerts_count||
       num_comments||
       highway||
       num_images||
       end_node||
       detection_date||
       detection_millis||
       detection_utc_timestamp||
       detection_utc_epoch_week||
       update_date||
       update_millis||
       update_utc_timestamp||
       update_utc_epoch_week) TOTAL_DISTINCT,
       getdate() ELT_START_TIME,
       getdate() ELT_END_TIME
       FROM dw_waze.stage_irregularity_{{ batchIdValue }} sa
GROUP BY etl_run_id;

INSERT INTO etl_waze.elt_run_state_stats
SELECT etl_run_id,
       (select table_id from etl_waze.DW_TBL_INFO where TABLE_NAME ilike 'irregularity') table_id,
       state,
       COUNT(1) TOTAL_ROWS_INGESTED,
       COUNT(DISTINCT id||
       irregularity_type||
       street||
       city||
       state||
       country||
       speed||
       regular_speed||
       seconds||
       delay_seconds||
       trend||
       num_thumbsup||
       irregularity_length||
       severity||
       jam_level||
       drivers_count||
       alerts_count||
       num_comments||
       highway||
       num_images||
       end_node||
       detection_date||
       detection_millis||
       detection_utc_timestamp||
       detection_utc_epoch_week||
       update_date||
       update_millis||
       update_utc_timestamp||
       update_utc_epoch_week) TOTAL_DISTINCT,
       getdate() ELT_START_TIME,
       getdate() ELT_END_TIME
       FROM dw_waze.stage_irregularity_{{ batchIdValue }} sa
GROUP BY etl_run_id,
state;
--Drop table
DROP TABLE IF EXISTS dw_waze.stage_irregularity_{{ batchIdValue }};

commit;