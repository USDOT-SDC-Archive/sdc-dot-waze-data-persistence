--------------------------------------------------------------------
--Update Batch Id
--------------------------------------------------------------------
update dw_waze.stage_jam_{{ batchIdValue }}  set etl_run_id={{ batchIdValue }} ;
-------------------------------------------------
--INT ETL Load
-------------------------------------------------
INSERT INTO dw_waze.int_jam_{{ batchIdValue }}
SELECT dsj.id, 
       dsj.jam_uuid, 
       dsj.jam_type, 
       dsj.road_type, 
       dsj.street, 
       dsj.city, 
       dsj.state, 
       dsj.country, 
       dsj.speed, 
       dsj.start_node, 
       dsj.end_node, 
       dsj.level, 
       dsj.jam_length, 
       dsj.delay, 
       dsj.turn_line, 
       dsj.turn_type, 
       dsj.blocking_alert_uuid, 
       dsj.pub_millis, 
       dsj.pub_utc_timestamp, 
       dsj.pub_utc_epoch_week, 
       dsj.jam_md5 ,
       dsj.etl_run_id
FROM   (SELECT id, 
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
               jam_md5, 
               etl_run_id,
               Row_number() 
                 OVER ( 
                   partition BY jam_md5, id 
                   ORDER BY jam_md5, id) row_num 
        FROM   (SELECT id, 
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
                       Md5(COALESCE(id, '') 
                           || COALESCE(jam_type, '') 
                           || COALESCE(road_type, '')
                           || COALESCE(street, '') 
                           || COALESCE(city, '')
                           || COALESCE(state, '') 
                           || COALESCE(country, '') 
                           || COALESCE(speed, '') 
                           || COALESCE(start_node, '') 
                           || COALESCE(end_node, '') 
                           || COALESCE(level, 0) 
                           || COALESCE(jam_length, '')
                           || COALESCE(delay, 0) 
                           || COALESCE(turn_line, '') 
                           || COALESCE(turn_type, '') 
                           || COALESCE(blocking_alert_uuid, '') 
                           || COALESCE(pub_millis, '')) jam_md5,
                       etl_run_id
                FROM   dw_waze.stage_jam_{{ batchIdValue }})) dsj 
       LEFT JOIN dw_waze.jam dj 
              ON dj.id = dsj.id 
                 AND dsj.jam_md5 = dj.jam_md5 
WHERE  dj.id IS NULL 
       AND row_num = 1 ;


-------------------------------------------------
--TGT ETL Load
-------------------------------------------------
INSERT INTO dw_waze.jam
SELECT id, 
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
       jam_md5
FROM  dw_waze.int_jam_{{ batchIdValue }} ;

commit;