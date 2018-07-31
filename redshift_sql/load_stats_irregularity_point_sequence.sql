
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


INSERT INTO {{ elt_schema_name }}.elt_run_stats
SELECT stg.elt_run_id,
       (select table_id from {{ elt_schema_name }}.DW_TBL_INFO where TABLE_NAME ilike 'irregularity') table_id,
       'irregularity_point_sequence' AS TABLE_NAME,
       stg.ingested_rows, 
       prs.persisted_rows 
FROM   (SELECT elt_run_id, 
               Count(*) INGESTED_ROWS 
        FROM   {{ dw_schema_name }}.stage_irregularity_point_sequence_{{ batchIdValue }} tfi
        GROUP  BY elt_run_id) stg 
       JOIN (SELECT elt_run_id, 
                    Count(*) PERSISTED_ROWS 
             FROM   {{ dw_schema_name }}.int_irregularity_point_sequence_{{ batchIdValue }} tfi
             GROUP  BY elt_run_id) prs 
         ON stg.elt_run_id = prs.elt_run_id ;

DROP TABLE IF EXISTS stage_irregularity_point_sequence_{{ batchIdValue }};
DROP TABLE IF EXISTS int_irregularity_point_sequence_{{ batchIdValue }};

commit;