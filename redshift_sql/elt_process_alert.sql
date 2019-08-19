--------------------------------------------------------------------
--Update Batch Id
--------------------------------------------------------------------
update {{ dw_schema_name }}.stage_alert_{{ batchIdValue }}  set elt_run_id={{ batchIdValue }} /* Batch ID to be replaced programatically*/;

-------------------------------------------------------------------------------
analyze {{ dw_schema_name }}.stage_alert_{{ batchIdValue }};
analyze {{ dw_schema_name }}.alert;
-------------------------------------------------------------------------------
INSERT INTO {{ dw_schema_name }}.int_alert_{{ batchIdValue }}
WITH stg_trnsform 
     AS (SELECT st.*, 
                cnt.total_counts 
         FROM   (SELECT tfi.alert_uuid, 
                        tfi.alert_type, 
                        Crc32(Coalesce(tfi.alert_uuid, '') 
                              || Coalesce(tfi.alert_type, '') 
                              || Coalesce(tfi.num_thumbsup, 0) 
                              || Coalesce(tfi.reliability, 0) 
                              || Coalesce(tfi.confidence, 0) 
                              || Coalesce(tfi.report_rating, 0) 
                              || Coalesce(tfi.location_lat, '') 
                              || Coalesce(tfi.location_lon, '') 
                              || Coalesce(tfi.pub_millis, '') 
                              || tfi.pub_utc_timestamp 
                              || Coalesce(tfi.state, '') 
                              ||Coalesce(tfi.report_description, '') 
                              ||Coalesce(tfi.road_type, 0)
                              ||Coalesce(tfi.city, '') 
                              ||Coalesce(tfi.road_type, 0)
                              ||Coalesce(tfi.magvar, '') 
                              ||Coalesce(tfi.sub_type, '') 
                              ||Coalesce(tfi.street, '') 
                              ||Coalesce(tfi.jam_uuid, '')) 
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
                        tfi.elt_run_id, 
                        Min(tfi.pub_millis :: bigint) 
                          over ( 
                            PARTITION BY tfi.alert_uuid) 
                        min_tf_millis, 
                        Min(tfi.pub_utc_timestamp) 
                          over ( 
                            PARTITION BY tfi.alert_uuid) 
                        min_tf_timestamp, 
                        Min(dwai.start_pub_millis :: bigint) 
                          over ( 
                            PARTITION BY tfi.alert_uuid) 
                        alert_start_millis 
                        , 
                        Min(dwai.start_pub_utc_timestamp) 
                          over ( 
                            PARTITION BY tfi.alert_uuid) 
                        alert_start_pub_stamp, 
                        CASE 
                          WHEN Coalesce(Min(dwai.pub_millis :: bigint) 
                                          over ( 
                                            PARTITION BY tfi.alert_uuid), 0) = 0 
                        THEN 
                          Min( 
                          tfi.pub_millis :: bigint) 
                          over ( 
                            PARTITION BY tfi.alert_uuid) 
                          WHEN Coalesce(Min(dwai.pub_millis :: bigint) 
                                          over ( 
                                            PARTITION BY tfi.alert_uuid), 0) <> 
                               0 
                               AND Min(tfi.pub_millis :: bigint) 
                                     over ( 
                                       PARTITION BY tfi.alert_uuid) <= Min( 
                                   dwai.start_pub_millis :: bigint) 
                                   over ( 
                                     PARTITION BY tfi.alert_uuid) 
                        THEN Min(tfi.pub_millis :: bigint) 
                        over ( 
                          PARTITION BY tfi.alert_uuid) 
                          WHEN Coalesce(Min(dwai.pub_millis :: bigint) 
                                          over ( 
                                            PARTITION BY tfi.alert_uuid), 0) <> 
                               0 
                               AND tfi.pub_millis :: bigint > Min( 
                                   dwai.start_pub_millis :: bigint) 
                                   over ( 
                                     PARTITION BY tfi.alert_uuid) 
                                THEN Min( 
                          dwai.start_pub_millis :: bigint) 
                        over ( 
                          PARTITION BY tfi.alert_uuid) 
                        END 
                        final_start_pub_millis, 
                        CASE 
                          WHEN Coalesce(Min(dwai.pub_millis :: bigint) 
                                          over ( 
                                            PARTITION BY tfi.alert_uuid), 0) = 0 
                        THEN 
                          Min(tfi.pub_utc_timestamp) 
                          over ( 
                            PARTITION BY tfi.alert_uuid) 
                          WHEN Coalesce(Min(dwai.pub_millis :: bigint) 
                                          over ( 
                                            PARTITION BY tfi.alert_uuid), 0) <> 
                               0 
                               AND Min(tfi.pub_millis :: bigint) 
                                     over ( 
                                       PARTITION BY tfi.alert_uuid) <= Min( 
                                   dwai.start_pub_millis :: bigint) 
                                   over ( 
                                     PARTITION BY tfi.alert_uuid) 
                        THEN Min(tfi.pub_utc_timestamp) 
                        over ( 
                          PARTITION BY tfi.alert_uuid) 
                          WHEN Coalesce(Min(dwai.pub_millis :: bigint) 
                                          over ( 
                                            PARTITION BY tfi.alert_uuid), 0) <> 
                               0 
                               AND tfi.pub_millis :: bigint > Min( 
                                   dwai.start_pub_millis :: bigint) 
                                   over ( 
                                     PARTITION BY tfi.alert_uuid) 
                                THEN Min( 
                          dwai.start_pub_utc_timestamp) 
                        over ( 
                          PARTITION BY tfi.alert_uuid) 
                        END 
                                final_start_pub_utc_timestamp, 
                        CASE 
                          WHEN Coalesce(Min(dwai.pub_millis :: bigint) 
                                          over ( 
                                            PARTITION BY tfi.alert_uuid), 0) <> 
                               0 
                               AND ( Min(tfi.pub_millis :: bigint) 
                                       over ( 
                                         PARTITION BY tfi.alert_uuid) < Min( 
                                     dwai.start_pub_millis :: bigint) 
                                     over ( 
                                       PARTITION BY tfi.alert_uuid) 
                                   ) THEN 1 
                          ELSE 0 
                        END 
                        revision_flag, 
                        Coalesce(Max(dwai.uuid_version) 
                                   over ( 
                                     PARTITION BY tfi.alert_uuid), 0) 
                        max_uuid_version 
                 FROM   {{ dw_schema_name }}.stage_alert_{{ batchIdValue }} tfi
                        left join {{ dw_schema_name }}.alert dwai
                               ON dwai.alert_uuid = tfi.alert_uuid) st 
                join (SELECT alert_uuid, 
                             alert_char_crc, 
                             Count(*) total_counts 
                      FROM   (SELECT alert_uuid, 
                                     Crc32(Coalesce(tfi.alert_uuid, '') 
                                           || Coalesce(tfi.alert_type, '') 
                                           || Coalesce(tfi.num_thumbsup, 0) 
                                           || Coalesce(tfi.reliability, 0) 
                                           || Coalesce(tfi.confidence, 0) 
                                           || Coalesce(tfi.report_rating, 0) 
                                           || Coalesce(tfi.location_lat, '') 
                                           || Coalesce(tfi.location_lon, '') 
                                           || Coalesce(tfi.pub_millis, '') 
                                           || tfi.pub_utc_timestamp 
                                           || Coalesce(tfi.state, '') 
||Coalesce(tfi.report_description, '') 
||Coalesce(tfi.road_type, 0)
||Coalesce(tfi.city, '') 
||Coalesce(tfi.road_type, 0)
||Coalesce(tfi.magvar, '') 
||Coalesce(tfi.sub_type, '') 
||Coalesce(tfi.street, '') 
||Coalesce(tfi.jam_uuid, '')) 
alert_char_crc 
FROM   {{ dw_schema_name }}.stage_alert_{{ batchIdValue }} tfi)
GROUP  BY alert_uuid, 
alert_char_crc) cnt 
ON cnt.alert_uuid = st.alert_uuid 
AND cnt.alert_char_crc = st.alert_char_crc), 
     transformed 
     AS (SELECT CASE 
                  WHEN fa.alert_uuid IS NULL 
                       AND fa.alert_char_crc IS NULL 
                       AND ers.elt_run_id IS NULL THEN 'I' 
                  WHEN fa.alert_uuid IS NOT NULL 
                       AND fa.alert_char_crc IS NOT NULL 
                       AND ers.elt_run_id IS NULL THEN 'U' 
                  WHEN ers.elt_run_id IS NOT NULL THEN 'IG' 
                END operation, 
                CASE 
                  WHEN fa.alert_uuid IS NULL 
                       AND fa.alert_char_crc IS NULL THEN 
                  max_uuid_version 
                  + Dense_rank() over (PARTITION BY stg_trnsform.alert_uuid 
                  ORDER BY 
                  stg_trnsform.pub_utc_timestamp, stg_trnsform.alert_char_crc) 
                END uuid_version, 
                Row_number() 
                  over ( 
                    PARTITION BY 
                  stg_trnsform.alert_uuid, stg_trnsform.alert_char_crc 
                    ORDER BY stg_trnsform.alert_uuid, 
                  stg_trnsform.alert_char_crc) 
                    row_num, 
                stg_trnsform.* 
         FROM   stg_trnsform 
                left join {{ dw_schema_name }}.alert fa
                       ON stg_trnsform.alert_uuid = fa.alert_uuid 
                          AND stg_trnsform.alert_char_crc = fa.alert_char_crc 
                left join {{ elt_schema_name }}.elt_run_stats ers
                       ON ers.elt_run_id = stg_trnsform.elt_run_id 
                          AND ers.table_id = 1001 
         WHERE  fa.alert_uuid IS NULL) 
SELECT tf.alert_char_crc, 
       tf.alert_uuid, 
       tf.uuid_version, 
       tf.elt_run_id, 
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
       tf.total_counts                                                      AS 
       total_occurences, 
       tf.operation                                                         AS 
       load_operation, 
       CASE 
         WHEN load_operation = 'I' 
              AND Max(tf.uuid_version) 
                    over ( 
                      PARTITION BY tf.alert_uuid) = uuid_version THEN 1 
         ELSE 0 
       END 
       current_flag, 
       tf.revision_flag 
FROM   transformed tf 
       left join {{ elt_schema_name }}.elt_run_stats ers
              ON ers.elt_run_id = tf.elt_run_id 
                 AND ers.table_id = 1001 
WHERE  row_num = 1 ;

------------------------------------------------------------------------------------------------------
-- Update ELT current flag if there is new identical record of the alert comming in a different batch
------------------------------------------------------------------------------------------------------

/*UPDATE {{ dw_schema_name }}.alert
   SET elt_current_flag = 0
FROM {{ dw_schema_name }}.int_alert_{{ batchIdValue }} ifa
WHERE {{ dw_schema_name }}.alert.alert_uuid = ifa.alert_uuid
AND   {{ dw_schema_name }}.alert.alert_char_crc = ifa.alert_char_crc
AND   {{ dw_schema_name }}.alert.uuid_version = ifa.uuid_version
AND   {{ dw_schema_name }}.alert.elt_run_id <> ifa.elt_run_id
AND (revision_flag=0 or load_operation ilike 'u');
commit;*/

-----------------------------------------------------------------------------------------------------
--Update current_flags if there is a new version of Alert
-----------------------------------------------------------------------------------------------------

UPDATE {{ dw_schema_name }}.alert
   SET current_flag = 0 from (SELECT MIN(uuid_version) min_version,
                                     alert_uuid
                              FROM {{ dw_schema_name }}.int_alert_{{ batchIdValue }}
                              where load_operation='I'
                              and revision_flag=0
                              GROUP BY alert_uuid) ifa WHERE {{ dw_schema_name }}.alert.alert_uuid = ifa.alert_uuid
AND {{ dw_schema_name }}.alert.uuid_version = ifa.min_version-1;

-----------------------------------------------------------------------------------------------------
--Temporarily stage revisions in revised_alert table
-----------------------------------------------------------------------------------------------------
INSERT INTO {{ dw_schema_name }}.revised_alert_{{ batchIdValue }}
select alert_char_crc,
       alert_uuid,
        uuid_version,
        case when max(uuid_version) over (partition by alert_uuid)=uuid_version THEN 1
        else 0
        END current_flag,
       elt_run_id,
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
       elt_run_id,
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
             dwa.elt_run_id,
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
            FROM {{ dw_schema_name }}.int_alert_{{ batchIdValue }}
            WHERE revision_flag = 1
            GROUP BY alert_uuid) rvs
        JOIN {{ dw_schema_name }}.alert dwa ON rvs.alert_uuid = dwa.alert_uuid
      UNION ALL
      SELECT alert_char_crc,
             alert_uuid,
             uuid_version,
             current_flag,
             elt_run_id,
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
      FROM {{ dw_schema_name }}.int_alert_{{ batchIdValue }}
      WHERE revision_flag = 1)rvs) ;



---------------------------------------------------------------------------
-- Delete Revised Rows on the Target table (alert)
---------------------------------------------------------------------------
delete from {{ dw_schema_name }}.alert
using (select alert_uuid from {{ dw_schema_name }}.revised_alert_{{ batchIdValue }} group by alert_uuid) rva
where rva.alert_uuid={{ dw_schema_name }}.alert.alert_uuid;

---------------------------------------------------------------------------
--Load Revised data to target alert table
---------------------------------------------------------------------------
INSERT INTO {{ dw_schema_name }}.alert
SELECT alert_char_crc,
             alert_uuid,
             uuid_version,
             current_flag,
             elt_run_id,
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
             road_type from {{ dw_schema_name }}.revised_alert_{{ batchIdValue }} order by pub_utc_timestamp;

---------------------------------------------------------------------------------------------
--Load transformed from Intermediate table to alert table
---------------------------------------------------------------------------------------------
INSERT INTO {{ dw_schema_name }}.alert
SELECT alert_char_crc,
       alert_uuid,
       uuid_version,
       current_flag,
       elt_run_id,
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
FROM {{ dw_schema_name }}.int_alert_{{ batchIdValue }}
WHERE load_operation IN ('I')
AND   revision_flag = 0
;


commit;