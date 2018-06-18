--------------------------------------------------------------------
--Update Batch Id
--------------------------------------------------------------------
update dw_waze.stage_jam_{{ batchIdValue }}  set etl_run_id={{ batchIdValue }} ;
-------------------------------------------------
--ETL Load
-------------------------------------------------
INSERT INTO dw_waze.jam_proto
select dsj.id,
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
       dsj.jam_md5    
from
(SELECT id,
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
        md5(COALESCE(id,'') ||
        COALESCE(jam_type,'') ||
       COALESCE(road_type,'') || 
        COALESCE(state,'') ||
        COALESCE(country,'') ||
        COALESCE(speed,'') ||
        COALESCE(start_node,'') ||
        COALESCE(end_node,'') ||
        COALESCE(level,0) ||
        COALESCE(jam_length,'') ||
        COALESCE(delay,0) ||
        COALESCE(turn_line,'') ||
        COALESCE(turn_type,'') ||
        COALESCE(blocking_alert_uuid,'') ||
        COALESCE(pub_millis,'') ) jam_md5
FROM dw_waze.stage_jam_{{ batchIdValue }} 
GROUP BY id,
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
       pub_utc_epoch_week) dsj left join dw_waze.jam dj on dj.id=dsj.id
       AND dsj.jam_md5=dj.jam_md5
       where dj.id is null;

commit;