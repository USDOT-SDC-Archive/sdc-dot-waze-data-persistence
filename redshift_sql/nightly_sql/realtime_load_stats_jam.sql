--Update elt_run_stats_realtime
INSERT INTO elt_waze.elt_run_stats_realtime
SELECT stg.elt_run_id, 
       ( 
              SELECT table_id 
              FROM   elt_waze.dw_tbl_info 
              WHERE  table_name ilike 'jam') table_id, 
       'jam' AS TABLE_NAME,
       stg.ingested_rows
FROM   ( 
                SELECT   elt_run_id, 
                         count(*) ingested_rows 
                FROM     dw_waze.stage_jam_{{ batchIdValue }} tfi 
                GROUP BY elt_run_id) stg;


--Update elt_run_state_stats_realtime
INSERT INTO elt_waze.elt_run_state_stats_realtime
SELECT stg.elt_run_id, 
       (select table_id from elt_waze.DW_TBL_INFO where TABLE_NAME ilike 'jam') table_id,
       'jam' AS TABLE_NAME,
       stg.state, 
       stg.ingested_rows
FROM   (SELECT elt_run_id, 
               state, 
               Count(*) INGESTED_ROWS 
        FROM   dw_waze.stage_jam_{{ batchIdValue }} tfi 
        GROUP  BY elt_run_id, 
                  state) stg;

commit;