--------------------------------------------------------------------
--Update Batch Id
--------------------------------------------------------------------
update dw_waze.stage_irregularity_{{ batchIdValue }}  set etl_run_id={{ batchIdValue }} ;
-------------------------------------------------------
--ETL Load
-------------------------------------------------------

INSERT INTO dw_waze.irregularity
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
       dsi.update_utc_epoch_week
FROM dw_waze.stage_irregularity_{{ batchIdValue }} dsi
  LEFT JOIN dw_waze.irregularity di
         ON dsi.id = dsi.id
        AND dsi.irregularity_type = di.irregularity_type
        AND dsi.street = di.street
        AND dsi.city = di.city
        AND dsi.state = di.state
        AND dsi.country = di.country
        AND dsi.speed = di.speed
        AND dsi.regular_speed = di.regular_speed
        AND dsi.seconds = di.seconds
        AND dsi.delay_seconds = di.delay_seconds
        AND dsi.trend = di.trend
        AND dsi.num_thumbsup = di.num_thumbsup
        AND dsi.irregularity_length = di.irregularity_length
        AND dsi.severity = di.severity
        AND dsi.jam_level = di.jam_level
        AND dsi.drivers_count = di.drivers_count
        AND dsi.alerts_count = di.alerts_count
        AND dsi.num_comments = di.num_comments
        AND dsi.highway = di.highway
        AND dsi.num_images = di.num_images
        AND dsi.end_node = di.end_node
        AND dsi.detection_date = di.detection_date
        AND dsi.detection_millis = di.detection_millis
        AND dsi.detection_utc_timestamp = di.detection_utc_timestamp
        AND dsi.detection_utc_epoch_week = di.detection_utc_epoch_week
        AND dsi.update_date = di.update_date
        AND dsi.update_millis = di.update_millis
        AND dsi.update_utc_timestamp = di.update_utc_timestamp
        AND dsi.update_utc_epoch_week = dsi.update_utc_epoch_week
        WHERE di.id is null
        GROUP BY dsi.id,
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
       dsi.update_utc_epoch_week;

commit;