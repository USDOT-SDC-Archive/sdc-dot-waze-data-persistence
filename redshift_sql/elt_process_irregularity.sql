--------------------------------------------------------------------
--Update Batch Id
--------------------------------------------------------------------
update {{ dw_schema_name }}.stage_irregularity_{{ batchIdValue }}  set elt_run_id={{ batchIdValue }} ;
-------------------------------------------------------
--INT ELT Load
-------------------------------------------------------

INSERT INTO {{ dw_schema_name }}.int_irregularity_{{ batchIdValue }}
SELECT dsi.id, 
       dsi.irregularity_type, 
       dsi.street, 
       dsi.city, 
       dsi.state, 
       dsi.country, 
       dsi.speed, 
       dsi.regular_speed, 
       dsi.seconds, 
       dsi.delay_seconds, 
       dsi.trend, 
       dsi.num_thumbsup, 
       dsi.irregularity_length, 
       dsi.severity, 
       dsi.jam_level, 
       dsi.drivers_count, 
       dsi.alerts_count, 
       dsi.num_comments, 
       dsi.highway, 
       dsi.num_images, 
       dsi.end_node, 
       dsi.detection_date, 
       dsi.detection_millis, 
       dsi.detection_utc_timestamp, 
       dsi.detection_utc_epoch_week, 
       dsi.update_date, 
       dsi.update_millis, 
       dsi.update_utc_timestamp, 
       dsi.update_utc_epoch_week, 
       dsi.irregularity_md5, 
       dsi.elt_run_id 
FROM   (SELECT id, 
               irregularity_type, 
               street, 
               city, 
               state, 
               country, 
               speed, 
               regular_speed, 
               seconds, 
               delay_seconds, 
               trend, 
               num_thumbsup, 
               irregularity_length, 
               severity, 
               jam_level, 
               drivers_count, 
               alerts_count, 
               num_comments, 
               highway, 
               num_images, 
               end_node, 
               detection_date, 
               detection_millis, 
               detection_utc_timestamp, 
               detection_utc_epoch_week, 
               update_date, 
               update_millis, 
               update_utc_timestamp, 
               update_utc_epoch_week, 
               irregularity_md5, 
               elt_run_id, 
               Row_number() 
                 OVER ( 
                   partition BY id, irregularity_md5 
                   ORDER BY id, irregularity_md5) row_num 
        FROM   (SELECT id, 
                       irregularity_type, 
                       street, 
                       city, 
                       state, 
                       country, 
                       speed, 
                       regular_speed, 
                       seconds, 
                       delay_seconds, 
                       trend, 
                       num_thumbsup, 
                       irregularity_length, 
                       severity, 
                       jam_level, 
                       drivers_count, 
                       alerts_count, 
                       num_comments, 
                       highway, 
                       num_images, 
                       end_node, 
                       detection_date, 
                       detection_millis, 
                       detection_utc_timestamp, 
                       detection_utc_epoch_week, 
                       update_date, 
                       update_millis, 
                       update_utc_timestamp, 
                       update_utc_epoch_week, 
                       Md5(COALESCE(id, '') 
                           || COALESCE(irregularity_type, '') 
                           || COALESCE(street, '') 
                           || COALESCE(city, '') 
                           || COALESCE(state, '') 
                           || COALESCE(country, '') 
                           || COALESCE(speed, '') 
                           || COALESCE(regular_speed, '') 
                           || COALESCE(seconds, 0) 
                           || COALESCE(delay_seconds, 0) 
                           || COALESCE(trend, '') 
                           || COALESCE(num_thumbsup, 0) 
                           || COALESCE(irregularity_length, '') 
                           || COALESCE(severity, 0) 
                           || COALESCE(jam_level, 0) 
                           || COALESCE(drivers_count, 0) 
                           || COALESCE(alerts_count, 0) 
                           || COALESCE(num_comments, '') 
                           || COALESCE(highway, '') 
                           || COALESCE(num_images, '') 
                           || COALESCE(end_node, '') 
                           || COALESCE(detection_date, '') 
                           || COALESCE(detection_millis, '') 
                           || COALESCE(detection_utc_timestamp) 
                           || COALESCE(update_date, '') 
                           || COALESCE(update_millis, '')) AS irregularity_md5, 
                       elt_run_id 
                FROM   {{ dw_schema_name }}.stage_irregularity_{{ batchIdValue }} dsi)) dsi
       LEFT JOIN {{ dw_schema_name }}.irregularity di
              ON dsi.id = di.id 
                 AND dsi.irregularity_md5 = di.irregularity_md5 
WHERE  di.id IS NULL 
       AND dsi.row_num = 1 ;

-------------------------------------------------------
--TGT ELT Load
-------------------------------------------------------

INSERT INTO {{ dw_schema_name }}.irregularity
SELECT dsi.id, 
       dsi.irregularity_type, 
       dsi.street, 
       dsi.city, 
       dsi.state, 
       dsi.country, 
       dsi.speed, 
       dsi.regular_speed, 
       dsi.seconds, 
       dsi.delay_seconds, 
       dsi.trend, 
       dsi.num_thumbsup, 
       dsi.irregularity_length, 
       dsi.severity, 
       dsi.jam_level, 
       dsi.drivers_count, 
       dsi.alerts_count, 
       dsi.num_comments, 
       dsi.highway, 
       dsi.num_images, 
       dsi.end_node, 
       dsi.detection_date, 
       dsi.detection_millis, 
       dsi.detection_utc_timestamp, 
       dsi.detection_utc_epoch_week, 
       dsi.update_date, 
       dsi.update_millis, 
       dsi.update_utc_timestamp, 
       dsi.update_utc_epoch_week, 
       dsi.irregularity_md5
FROM   {{ dw_schema_name }}.int_irregularity_{{ batchIdValue }} dsi;

commit;