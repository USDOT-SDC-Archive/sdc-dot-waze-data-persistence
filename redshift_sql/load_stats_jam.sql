
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

INSERT INTO etl_waze.elt_run_stats
SELECT etl_run_id,
       (select table_id from etl_waze.DW_TBL_INFO where TABLE_NAME ilike 'jam') table_id,
        COUNT(*) AS TOTAL_ROWS_INGESTED,
        COUNT(DISTINCT jam_md5) AS TOTAL_DISTINCT,
        getdate() ELT_START_TIME,
        getdate() ELT_END_TIME
FROM (SELECT id,
             jam_uuid,
             jam_type,
             road_type,
             street,
             city,
             state,
             country,
             speed,
             start_node,
             end_node,
             level,
             jam_length,
             delay,
             turn_line,
             turn_type,
             blocking_alert_uuid,
             pub_millis,
             pub_utc_timestamp,
             pub_utc_epoch_week,
             etl_run_id,
             MD5(COALESCE(id,'') ||COALESCE (jam_type,'') ||COALESCE (road_type,'') ||COALESCE (state,'') ||COALESCE (country,'') ||COALESCE (speed,'') ||COALESCE (start_node,'') ||COALESCE (end_node,'') ||COALESCE (level,0) ||COALESCE (jam_length,'') ||COALESCE (delay,0) ||COALESCE (turn_line,'') ||COALESCE (turn_type,'') ||COALESCE (blocking_alert_uuid,'') ||COALESCE (pub_millis,'')) jam_md5
      FROM dw_waze.stage_jam_{{ batchIdValue }} )
      group by etl_run_id;



INSERT INTO etl_waze.elt_run_state_stats
SELECT etl_run_id,
       (select table_id from etl_waze.DW_TBL_INFO where TABLE_NAME ilike 'jam') table_id,
        state,
        COUNT(*) AS TOTAL_ROWS_INGESTED,
        COUNT(DISTINCT jam_md5) AS TOTAL_DISTINCT,
        getdate() ELT_START_TIME,
        getdate() ELT_END_TIME
FROM (SELECT id,
             jam_uuid,
             jam_type,
             road_type,
             street,
             city,
             state,
             country,
             speed,
             start_node,
             end_node,
             level,
             jam_length,
             delay,
             turn_line,
             turn_type,
             blocking_alert_uuid,
             pub_millis,
             pub_utc_timestamp,
             pub_utc_epoch_week,
             etl_run_id,
             MD5(COALESCE(id,'') ||COALESCE (jam_type,'') ||COALESCE (road_type,'') ||COALESCE (state,'') ||COALESCE (country,'') ||COALESCE (speed,'') ||COALESCE (start_node,'') ||COALESCE (end_node,'') ||COALESCE (level,0) ||COALESCE (jam_length,'') ||COALESCE (delay,0) ||COALESCE (turn_line,'') ||COALESCE (turn_type,'') ||COALESCE (blocking_alert_uuid,'') ||COALESCE (pub_millis,'')) jam_md5
      FROM dw_waze.stage_jam_{{ batchIdValue }} )
      group by etl_run_id,
      state;

--Drop table
DROP TABLE IF EXISTS dw_waze.stage_jam_{{ batchIdValue }};

commit;