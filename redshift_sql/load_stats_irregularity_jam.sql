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
       (select table_id from etl_waze.DW_TBL_INFO where TABLE_NAME ilike 'irregularity_jam') table_id,
       COUNT(1) TOTAL_ROWS_INGESTED,
       COUNT(DISTINCT sa.irregularity_id 
            ||sa.jam_uuid 
            ) TOTAL_DISTINCT,
       getdate() ELT_START_TIME,
       getdate() ELT_END_TIME
       FROM dw_waze.stage_irregularity_jam_{{ batchIdValue }}  sa
GROUP BY etl_run_id;


--Drop table
DROP TABLE IF EXISTS dw_waze.stage_irregularity_jam_{{ batchIdValue }} ;

commit;