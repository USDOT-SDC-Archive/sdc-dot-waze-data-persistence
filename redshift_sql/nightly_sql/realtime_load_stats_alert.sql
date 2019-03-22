INSERT INTO etl_waze.elt_run_stats_realtime
SELECT stg.etl_run_id,
       (select table_id from etl_waze.DW_TBL_INFO where TABLE_NAME ilike 'alert') table_id,
       'alert' AS TABLE_NAME,
       stg.ingested_rows
FROM   (SELECT etl_run_id, 
               Count(*) INGESTED_ROWS 
        FROM   dw_waze.stage_alert_{{ batchIdValue }} tfi 
        GROUP  BY etl_run_id) stg;

INSERT INTO etl_waze.elt_run_state_stats_realtime
SELECT stg.etl_run_id, 
       (select table_id from etl_waze.DW_TBL_INFO where TABLE_NAME ilike 'alert') table_id,
       'alert' AS TABLE_NAME,
       stg.state, 
       stg.ingested_rows 
FROM   (SELECT etl_run_id, 
               state, 
               Count(*) INGESTED_ROWS 
        FROM   dw_waze.stage_alert_{{ batchIdValue }} tfi 
        GROUP  BY etl_run_id, 
                  state) stg;

commit;
