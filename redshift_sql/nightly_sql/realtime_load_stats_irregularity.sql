INSERT INTO elt_waze.elt_run_stats_realtime
SELECT stg.elt_run_id,
       (select table_id from elt_waze.DW_TBL_INFO where TABLE_NAME ilike 'irregularity') table_id,
       'irregularity' AS TABLE_NAME,
       stg.ingested_rows
FROM   (SELECT elt_run_id, 
               Count(*) INGESTED_ROWS 
        FROM   dw_waze.stage_irregularity_{{ batchIdValue }} tfi 
        GROUP  BY elt_run_id) stg;

INSERT INTO elt_waze.elt_run_state_stats_realtime
SELECT stg.elt_run_id, 
       (select table_id from elt_waze.DW_TBL_INFO where TABLE_NAME ilike 'irregularity') table_id,
       'irregularity' AS TABLE_NAME,
       stg.state, 
       stg.ingested_rows 
FROM   (SELECT elt_run_id, 
               state, 
               Count(*) INGESTED_ROWS 
        FROM   dw_waze.stage_irregularity_{{ batchIdValue }} tfi 
        GROUP  BY elt_run_id, 
                  state) stg;

commit;
