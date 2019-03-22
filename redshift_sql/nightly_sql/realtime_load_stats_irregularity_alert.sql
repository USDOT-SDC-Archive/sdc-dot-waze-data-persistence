INSERT INTO elt_waze.elt_run_stats_realtime
SELECT stg.elt_run_id,
       (select table_id from elt_waze.DW_TBL_INFO where TABLE_NAME ilike 'irregularity_alert') table_id,
       'irregularity_alert' AS TABLE_NAME,
       stg.ingested_rows
FROM   (SELECT elt_run_id, 
               Count(*) INGESTED_ROWS 
        FROM   dw_waze.stage_irregularity_alert_{{ batchIdValue }} tfi 
        GROUP  BY elt_run_id) stg;


commit;