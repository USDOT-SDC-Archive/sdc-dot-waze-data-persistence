--------------------------------------------------------------------
--Update Batch Id
--------------------------------------------------------------------
update dw_waze.stage_alert_{{ batchIdValue }}  set etl_run_id={{ batchIdValue }} /* Batch ID to be replaced programatically*/;

-------------------------------------------------------------------------------
analyze dw_waze.stage_alert_{{ batchIdValue }};
-------------------------------------------------------------------------------

INSERT INTO dw_waze.int_alert_{{ batchIdValue }}
WITH stg_trnsform
     AS (SELECT *
         FROM   (SELECT tfi.alert_uuid,
                        tfi.alert_type,
                        Count(*)
over (
PARTITION BY tfi.alert_uuid, tfi.alert_type, tfi.num_thumbsup, tfi.reliability, tfi.confidence, tfi.report_rating, tfi.location_lat, tfi.location_lon, tfi.pub_millis, tfi.pub_utc_timestamp, tfi.state,Coalesce(tfi.report_description,''),Coalesce(tfi.road_type,''),Coalesce(tfi.city,''),Coalesce(tfi.road_type,''),Coalesce(tfi.magvar,''),Coalesce(tfi.sub_type,''),Coalesce(tfi.street,''),Coalesce(tfi.jam_uuid,'')) total_counts,
Crc32(tfi.alert_uuid
      || tfi.alert_type
      || tfi.num_thumbsup
      || tfi.reliability
      || tfi.confidence
      || tfi.report_rating
      || tfi.location_lat
      || tfi.location_lon
      || tfi.pub_millis
      || tfi.pub_utc_timestamp
      || tfi.state
      ||Coalesce(tfi.report_description,'')
      ||Coalesce(tfi.road_type,'')
      ||Coalesce(tfi.city,'')
      ||Coalesce(tfi.road_type,'')
      ||Coalesce(tfi.magvar,'')
      ||Coalesce(tfi.sub_type,'')
      ||Coalesce(tfi.street,'')
      ||Coalesce(tfi.jam_uuid,'')
      )
                alert_char_crc,
tfi.sub_type,
tfi.street,
tfi.city,
tfi.state,
tfi.jam_uuid,
tfi.country,
tfi.num_thumbsup,
tfi.reliability,
tfi.confidence,
tfi.report_rating,
tfi.magvar,
tfi.report_description,
tfi.location_lat,
tfi.location_lon,
tfi.pub_millis,
tfi.pub_utc_timestamp,
tfi.pub_utc_epoch_week,
tfi.road_type,
tfi.etl_run_id,
Min(tfi.pub_millis::BIGINT)
  over (
    PARTITION BY tfi.alert_uuid)
                min_tf_millis,
Min(tfi.pub_utc_timestamp)
  over (
    PARTITION BY tfi.alert_uuid)
                min_tf_timestamp,
Min(dwai.start_pub_millis::BIGINT)
  over (
    PARTITION BY tfi.alert_uuid)
                alert_start_millis,
Min(dwai.start_pub_utc_timestamp)
  over (
    PARTITION BY tfi.alert_uuid)
                alert_start_pub_stamp,
CASE
  WHEN Coalesce(Min(dwai.pub_millis::BIGINT)
                  over (
                    PARTITION BY tfi.alert_uuid), 0) = 0 THEN
  Min(tfi.pub_millis::BIGINT)
  over (
    PARTITION BY tfi.alert_uuid)
  WHEN Coalesce(Min(dwai.pub_millis::BIGINT)
                  over (
                    PARTITION BY tfi.alert_uuid), 0) <> 0
       AND Min(tfi.pub_millis::BIGINT)
             over (
               PARTITION BY tfi.alert_uuid) <= Min(dwai.start_pub_millis::BIGINT)
                                                 over (
                                                   PARTITION BY tfi.alert_uuid)
THEN Min(tfi.pub_millis::BIGINT)
over (
  PARTITION BY tfi.alert_uuid)
  WHEN Coalesce(Min(dwai.pub_millis::BIGINT)
                  over (
                    PARTITION BY tfi.alert_uuid), 0) <> 0
       AND tfi.pub_millis::BIGINT > Min(dwai.start_pub_millis::BIGINT)
                              over (
                                PARTITION BY tfi.alert_uuid) THEN
  Min(dwai.start_pub_millis::BIGINT)
  over (
    PARTITION BY tfi.alert_uuid)
END
                final_start_pub_millis,
CASE
  WHEN Coalesce(Min(dwai.pub_millis::BIGINT)
                  over (
                    PARTITION BY tfi.alert_uuid), 0) = 0 THEN
  Min(tfi.pub_utc_timestamp)
  over (
    PARTITION BY tfi.alert_uuid)
  WHEN Coalesce(Min(dwai.pub_millis::BIGINT)
                  over (
                    PARTITION BY tfi.alert_uuid), 0) <> 0
       AND Min(tfi.pub_millis::BIGINT)
             over (
               PARTITION BY tfi.alert_uuid) <= Min(dwai.start_pub_millis::BIGINT)
                                                 over (
                                                   PARTITION BY tfi.alert_uuid)
THEN Min(tfi.pub_utc_timestamp)
over (
  PARTITION BY tfi.alert_uuid)
  WHEN Coalesce(Min(dwai.pub_millis::BIGINT)
                  over (
                    PARTITION BY tfi.alert_uuid), 0) <> 0
       AND tfi.pub_millis::BIGINT > Min(dwai.start_pub_millis::BIGINT)
                              over (
                                PARTITION BY tfi.alert_uuid) THEN
  Min(dwai.start_pub_utc_timestamp)
  over (
    PARTITION BY tfi.alert_uuid)
END
                final_start_pub_utc_timestamp,
CASE
  WHEN Coalesce(Min(dwai.pub_millis::BIGINT)
                  over (
                    PARTITION BY tfi.alert_uuid), 0) <> 0
       AND ( Min(tfi.pub_millis::BIGINT)
               over (
                 PARTITION BY tfi.alert_uuid) < Min(dwai.start_pub_millis::BIGINT)
                                                  over (
                                                    PARTITION BY
           tfi.alert_uuid)  ) THEN 1
  ELSE 0
END
                revision_flag
 FROM   dw_waze.stage_alert_{{ batchIdValue }} tfi
        left join dw_waze.alert dwai
               ON dwai.alert_uuid = tfi.alert_uuid
               )
 GROUP  BY revision_flag,
           final_start_pub_utc_timestamp,
           final_start_pub_millis,
           alert_start_pub_stamp,
           alert_uuid,
           alert_type,
           total_counts,
           alert_char_crc,
           sub_type,
           street,
           city,
           state,
           jam_uuid,
           country,
           num_thumbsup,
           reliability,
           confidence,
           report_rating,
           magvar,
           report_description,
           location_lat,
           location_lon,
           pub_millis,
           pub_utc_timestamp,
           pub_utc_epoch_week,
           road_type,
           etl_run_id,
           min_tf_millis,
           min_tf_timestamp,
           alert_start_millis),
     transformed
     AS (SELECT CASE
                  WHEN fa.alert_uuid IS NULL
                       AND fa.alert_char_crc IS NULL
                       AND ers.etl_run_id IS NULL THEN 'I'
                  WHEN fa.alert_uuid IS NOT NULL
                       AND fa.alert_char_crc IS NOT NULL
                       AND ers.etl_run_id IS NULL THEN 'U'
                  WHEN ers.etl_run_id IS NOT NULL THEN 'IG'
                END operation,
                CASE
                  WHEN fa.alert_uuid IS NULL
                       AND fa.alert_char_crc IS NULL THEN
                  Coalesce((SELECT Max(uuid_version) FROM dw_waze.alert ifa
                  WHERE
                  ifa.alert_uuid
                  =
                  stg_trnsform.alert_uuid GROUP BY ifa.alert_uuid), 0)
                  + Row_number() over (PARTITION BY stg_trnsform.alert_uuid
                  ORDER BY
                  stg_trnsform.pub_millis::BIGINT, stg_trnsform.alert_char_crc)
                  WHEN fa.alert_uuid IS NOT NULL
                       AND fa.alert_char_crc IS NOT NULL THEN
                  (SELECT DISTINCT ifa.uuid_version
                   FROM   dw_waze.alert ifa
                   WHERE
                  ifa.alert_uuid = stg_trnsform.alert_uuid
                  AND ifa.alert_char_crc = stg_trnsform.alert_char_crc)
                END uuid_version,
                stg_trnsform.*
         FROM   stg_trnsform
                left join dw_waze.alert fa
                       ON stg_trnsform.alert_uuid = fa.alert_uuid
                          AND stg_trnsform.alert_char_crc = fa.alert_char_crc
                left join etl_waze.elt_run_stats ers
                       ON ers.etl_run_id = stg_trnsform.etl_run_id
                       AND ers.table_id =1001
                       )
SELECT tf.alert_char_crc,
       tf.alert_uuid,
       CASE
         WHEN tf.operation = 'I'
              AND Coalesce((SELECT Max(uuid_version)
                            FROM   dw_waze.alert
                            WHERE  alert_uuid = tf.alert_uuid
                            GROUP  BY alert_uuid), 0) > 0 THEN
         tf.uuid_version - (SELECT Count(alert_uuid)
                            FROM   transformed ta
                            WHERE  ta.alert_uuid = tf.alert_uuid
                                   AND tf.operation = 'U') - 1
         WHEN tf.operation = 'I'
              AND Coalesce((SELECT Max(uuid_version)
                            FROM   dw_waze.alert
                            WHERE  alert_uuid = tf.alert_uuid
                            GROUP  BY alert_uuid), 0) = 0 THEN
         tf.uuid_version - (SELECT Count(alert_uuid)
                            FROM   transformed ta
                            WHERE  ta.alert_uuid = tf.alert_uuid
                                   AND tf.operation = 'U')
         ELSE tf.uuid_version
       END
       uuid_version,
       tf.etl_run_id,
       tf.alert_type,
       tf.sub_type,
       tf.street,
       tf.city,
       tf.state,
       tf.country,
       tf.num_thumbsup,
       tf.reliability,
       tf.confidence,
       tf.report_rating,
       tf.magvar,
       tf.report_description,
       tf.location_lat,
       tf.location_lon,
       tf.jam_uuid,
       Coalesce(tf.final_start_pub_millis, tf.alert_start_millis)           AS
       start_pub_millis,
       Coalesce(tf.final_start_pub_utc_timestamp, tf.alert_start_pub_stamp) AS
       start_pub_utc_timestamp,
       tf.pub_millis,
       tf.pub_utc_timestamp,
       tf.pub_utc_epoch_week,
       tf.road_type,
       tf.total_counts,
       tf.operation                                                         AS
       load_operation,
       CASE
         WHEN Max(CASE
                    WHEN tf.operation = 'I' THEN tf.uuid_version - 1
                    ELSE tf.uuid_version
                  END)
                over (
                  PARTITION BY tf.alert_uuid) = CASE
                                                  WHEN tf.operation = 'I' THEN
                                                  tf.uuid_version - 1
                                                  ELSE tf.uuid_version
                                                END THEN 1
         ELSE 0
       END
       current_flag,
       tf.revision_flag
FROM   transformed tf
       left join etl_waze.elt_run_stats ers
              ON ers.etl_run_id = tf.etl_run_id
              AND ers.table_id=1001
               ;
------------------------------------------------------------------------------------------------------
-- Update ETL current flag if there is new identical record of the alert comming in a different batch
------------------------------------------------------------------------------------------------------

UPDATE dw_waze.alert
   SET etl_current_flag = 0
FROM dw_waze.int_alert_{{ batchIdValue }} ifa
WHERE dw_waze.alert.alert_uuid = ifa.alert_uuid
AND   dw_waze.alert.alert_char_crc = ifa.alert_char_crc
AND   dw_waze.alert.uuid_version = ifa.uuid_version
AND   dw_waze.alert.etl_run_id <> ifa.etl_run_id
AND (revision_flag=0 or load_operation ilike 'u');
commit;

-----------------------------------------------------------------------------------------------------
--Update current_flags if there is a new version of Alert
-----------------------------------------------------------------------------------------------------

UPDATE dw_waze.alert
   SET current_flag = 0 from (SELECT MIN(uuid_version) min_version,
                                     alert_uuid
                              FROM dw_waze.int_alert_{{ batchIdValue }}
                              where load_operation='I'
                              and revision_flag=0
                              GROUP BY alert_uuid) ifa WHERE dw_waze.alert.alert_uuid = ifa.alert_uuid
AND dw_waze.alert.uuid_version < ifa.min_version;

-----------------------------------------------------------------------------------------------------
--Temporarily stage revisions in revised_alert table
-----------------------------------------------------------------------------------------------------
INSERT INTO dw_waze.revised_alert_{{ batchIdValue }}
select alert_char_crc,
       alert_uuid,
        uuid_version,
        case when max(uuid_version) over (partition by alert_uuid)=uuid_version THEN 1
        else 0
        END current_flag,
       etl_run_id,
       etl_current_flag,
       total_occurences,
       alert_type,
       sub_type,
       street,
       city,
       state,
       country,
       num_thumbsup,
       reliability,
       confidence,
       report_rating,
       magvar,
       report_description,
       location_lat,
       location_lon,
       jam_uuid,
       start_pub_millis,
       start_pub_utc_timestamp,
       pub_millis,
       pub_utc_timestamp,
       pub_utc_epoch_week,
       road_type
from
(SELECT alert_char_crc,
       alert_uuid,
       Dense_rank() OVER(partition by alert_uuid order by pub_millis::BIGINT,alert_char_crc) uuid_version,
       etl_run_id,
       etl_current_flag,
       total_occurences,
       alert_type,
       sub_type,
       street,
       city,
       state,
       country,
       num_thumbsup,
       reliability,
       confidence,
       report_rating,
       magvar,
       report_description,
       location_lat,
       location_lon,
       jam_uuid,
       MIN(start_pub_millis::BIGINT) OVER (PARTITION BY rvs.alert_uuid) start_pub_millis,
       MIN(start_pub_utc_timestamp) OVER (PARTITION BY rvs.alert_uuid) start_pub_utc_timestamp,
       pub_millis,
       pub_utc_timestamp,
       pub_utc_epoch_week,
       road_type
FROM (SELECT dwa.alert_char_crc,
             dwa.alert_uuid,
             dwa.uuid_version,
             dwa.current_flag,
             dwa.etl_run_id,
             dwa.etl_current_flag,
             dwa.total_occurences,
             dwa.alert_type,
             dwa.sub_type,
             dwa.street,
             dwa.city,
             dwa.state,
             dwa.country,
             dwa.num_thumbsup,
             dwa.reliability,
             dwa.confidence,
             dwa.report_rating,
             dwa.magvar,
             dwa.report_description,
             dwa.location_lat,
             dwa.location_lon,
             dwa.jam_uuid,
             dwa.start_pub_millis,
             dwa.start_pub_utc_timestamp,
             dwa.pub_millis,
             dwa.pub_utc_timestamp,
             dwa.pub_utc_epoch_week,
             dwa.road_type,
             'l' load_operation,
             'tgt' t_type
      FROM (SELECT alert_uuid
            FROM dw_waze.int_alert_{{ batchIdValue }}
            WHERE revision_flag = 1
            GROUP BY alert_uuid) rvs
        JOIN dw_waze.alert dwa ON rvs.alert_uuid = dwa.alert_uuid
      UNION ALL
      SELECT alert_char_crc,
             alert_uuid,
             uuid_version,
             current_flag,
             etl_run_id,
             1 AS etl_current_flag,
             total_occurences,
             alert_type,
             sub_type,
             street,
             city,
             state,
             country,
             num_thumbsup,
             reliability,
             confidence,
             report_rating,
             magvar,
             report_description,
             location_lat,
             location_lon,
             jam_uuid,
             start_pub_millis,
             start_pub_utc_timestamp,
             pub_millis,
             pub_utc_timestamp,
             pub_utc_epoch_week,
             road_type,
             load_operation,
             'int' t_type
      FROM dw_waze.int_alert_{{ batchIdValue }}
      WHERE revision_flag = 1)rvs) ;



---------------------------------------------------------------------------
-- Delete Revised Rows on the Target table (alert)
---------------------------------------------------------------------------
delete from dw_waze.alert
using (select alert_uuid from dw_waze.revised_alert_{{ batchIdValue }} group by alert_uuid) rva
where rva.alert_uuid=dw_waze.alert.alert_uuid;

---------------------------------------------------------------------------
--Load Revised data to target alert table
---------------------------------------------------------------------------
INSERT INTO dw_waze.alert
SELECT alert_char_crc,
             alert_uuid,
             uuid_version,
             current_flag,
             etl_run_id,
             etl_current_flag,
             total_occurences,
             alert_type,
             sub_type,
             street,
             city,
             state,
             country,
             num_thumbsup,
             reliability,
             confidence,
             report_rating,
             magvar,
             report_description,
             location_lat,
             location_lon,
             jam_uuid,
             start_pub_millis,
             start_pub_utc_timestamp,
             pub_millis,
             pub_utc_timestamp,
             pub_utc_epoch_week,
             road_type from dw_waze.revised_alert_{{ batchIdValue }};

---------------------------------------------------------------------------------------------
--Load transformed Intermediate table to alert table
---------------------------------------------------------------------------------------------
INSERT INTO dw_waze.alert
SELECT alert_char_crc,
       alert_uuid,
       uuid_version,
       current_flag,
       etl_run_id,
       1 AS etl_current_flag,
       total_occurences,
       alert_type,
       sub_type,
       street,
       city,
       state,
       country,
       num_thumbsup,
       reliability,
       confidence,
       report_rating,
       magvar,
       report_description,
       location_lat,
       location_lon,
       jam_uuid,
       start_pub_millis,
       start_pub_utc_timestamp,
       pub_millis,
       pub_utc_timestamp,
       pub_utc_epoch_week,
       road_type
FROM dw_waze.int_alert_{{ batchIdValue }}
WHERE load_operation IN ('I','U')
AND   revision_flag = 0
GROUP BY alert_char_crc,
         alert_uuid,
         uuid_version,
         current_flag,
         etl_run_id,
         total_occurences,
         alert_type,
         sub_type,
         street,
         city,
         state,
         country,
         num_thumbsup,
         reliability,
         confidence,
         report_rating,
         magvar,
         report_description,
         location_lat,
         location_lon,
         jam_uuid,
         start_pub_millis,
         start_pub_utc_timestamp,
         pub_millis,
         pub_utc_timestamp,
         pub_utc_epoch_week,
         road_type;


commit;