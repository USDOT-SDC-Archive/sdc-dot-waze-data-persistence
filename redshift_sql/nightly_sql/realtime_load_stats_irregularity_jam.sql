
INSERT INTO elt_waze.elt_run_stats_realtime
SELECT stg.elt_run_id,
       (select table_id from elt_waze.DW_TBL_INFO where TABLE_NAME ilike 'irregularity_jam') table_id,
       'irregularity_jam' AS TABLE_NAME,
       stg.ingested_rows
FROM   (SELECT elt_run_id, 
               Count(*) INGESTED_ROWS 
        FROM   dw_waze.stage_irregularity_jam_{{ batchIdValue }} tfi 
        GROUP  BY elt_run_id) stg;

commit;