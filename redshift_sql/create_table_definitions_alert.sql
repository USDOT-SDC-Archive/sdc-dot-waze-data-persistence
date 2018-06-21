----------------------------------------------------------------------------------------------------------------------------------------------------
--table definitions
----------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------
---Create TEMP Stage Alert table DDL
--------------------------------------------------------------------------------
create schema IF NOT EXISTS dw_waze;
DROP TABLE IF EXISTS dw_waze.stage_alert_{{ batchIdValue }};
CREATE TABLE IF NOT EXISTS dw_waze.stage_alert_{{ batchIdValue }}
( alert_uuid           VARCHAR(50) ENCODE lzo,
  alert_type           VARCHAR(25) ENCODE lzo,
  sub_type             VARCHAR(70) ENCODE lzo,
  street               VARCHAR(100) ENCODE lzo,
  city                 VARCHAR(500) ENCODE lzo,
  state                VARCHAR(10) ENCODE lzo,
  country              VARCHAR(25) ENCODE lzo,
  num_thumbsup         SMALLINT ENCODE lzo,
  reliability          SMALLINT ENCODE lzo,
  confidence           SMALLINT ENCODE lzo,
  report_rating        SMALLINT ENCODE lzo,
  magvar               VARCHAR(50) ENCODE lzo,
  report_description   VARCHAR(1000) ENCODE lzo,
  location_lat         VARCHAR(50) ENCODE lzo,
  location_lon         VARCHAR(50) ENCODE lzo,
  jam_uuid             VARCHAR(100) ENCODE lzo,
  pub_millis           VARCHAR(50) ENCODE lzo,
  pub_utc_timestamp    TIMESTAMP WITHOUT TIME ZONE ENCODE lzo,
  pub_utc_epoch_week   SMALLINT ENCODE lzo,
  road_type            VARCHAR(25) ENCODE lzo,
  etl_run_id           VARCHAR(50)

)
DISTSTYLE KEY DISTKEY (alert_uuid);
--------------------------------------------------------------------------------
---Create TEMP Intermediate Alert table DDL
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS dw_waze.int_alert_{{ batchIdValue }};
CREATE TABLE IF NOT EXISTS dw_waze.int_alert_{{ batchIdValue }}
(
  alert_char_crc VARCHAR(10) ENCODE lzo,
  alert_uuid           VARCHAR(50) ENCODE lzo,
  uuid_version         SMALLINT ENCODE lzo,
  etl_run_id           VARCHAR(50) ENCODE lzo,
  alert_type           VARCHAR(25) ENCODE lzo,
  sub_type             VARCHAR(70) ENCODE lzo,
  street               VARCHAR(100) ENCODE lzo,
  city                 VARCHAR(500) ENCODE lzo,
  state                VARCHAR(10) ENCODE lzo,
  country              VARCHAR(25) ENCODE lzo,
  num_thumbsup         SMALLINT ENCODE lzo,
  reliability          SMALLINT ENCODE lzo,
  confidence           SMALLINT ENCODE lzo,
  report_rating        SMALLINT ENCODE lzo,
  magvar               VARCHAR(50) ENCODE lzo,
  report_description   VARCHAR(1000) ENCODE lzo,
  location_lat         VARCHAR(50) ENCODE lzo,
  location_lon         VARCHAR(50) ENCODE lzo,
  jam_uuid             VARCHAR(100) ENCODE lzo,
  start_pub_millis           VARCHAR(50) ENCODE lzo,
  start_pub_utc_timestamp    TIMESTAMP WITHOUT TIME ZONE ENCODE lzo,
  pub_millis           VARCHAR(50) ENCODE lzo,
  pub_utc_timestamp    TIMESTAMP WITHOUT TIME ZONE ENCODE lzo,
  pub_utc_epoch_week   SMALLINT ENCODE lzo,
  road_type            VARCHAR(25) ENCODE lzo,
  total_occurences    SMALLINT ENCODE lzo,
  load_operation       VARCHAR(2) ENCODE lzo,
  current_flag         SMALLINT ENCODE lzo,
  revision_flag         SMALLINT ENCODE lzo
)
DISTSTYLE KEY DISTKEY (alert_uuid);
--------------------------------------------------------------------------------
---Create TEMP Revised table
--------------------------------------------------------------------------------

DROP TABLE IF EXISTS dw_waze.revised_alert_{{ batchIdValue }};
CREATE TABLE IF NOT EXISTS dw_waze.revised_alert_{{ batchIdValue }}
(
  alert_char_crc VARCHAR(10) ENCODE lzo,
  alert_uuid           VARCHAR(50) ENCODE lzo,
  uuid_version         SMALLINT ENCODE lzo,
  current_flag         SMALLINT ENCODE lzo,
  etl_run_id           VARCHAR(50) ENCODE lzo,
  etl_current_flag     SMALLINT ENCODE lzo,
  total_occurences         SMALLINT ENCODE lzo,
  alert_type           VARCHAR(25) ENCODE lzo,
  sub_type             VARCHAR(70) ENCODE lzo,
  street               VARCHAR(100) ENCODE lzo,
  city                 VARCHAR(500) ENCODE lzo,
  state                VARCHAR(10) ENCODE lzo,
  country              VARCHAR(25) ENCODE lzo,
  num_thumbsup         SMALLINT ENCODE lzo,
  reliability          SMALLINT ENCODE lzo,
  confidence           SMALLINT ENCODE lzo,
  report_rating        SMALLINT ENCODE lzo,
  magvar               VARCHAR(50) ENCODE lzo,
  report_description   VARCHAR(1000) ENCODE lzo,
  location_lat         VARCHAR(50) ENCODE lzo,
  location_lon         VARCHAR(50) ENCODE lzo,
  jam_uuid             VARCHAR(100) ENCODE lzo,
  start_pub_millis           VARCHAR(50) ENCODE lzo,
  start_pub_utc_timestamp    TIMESTAMP WITHOUT TIME ZONE ENCODE lzo,
  pub_millis           VARCHAR(50) ENCODE lzo,
  pub_utc_timestamp    TIMESTAMP WITHOUT TIME ZONE ENCODE lzo,
  pub_utc_epoch_week   SMALLINT ENCODE lzo,
  road_type            VARCHAR(25) ENCODE lzo

)
DISTSTYLE KEY DISTKEY (alert_uuid)
 ;

--------------------------------------------------------------------------------
---Alert table DDL
--------------------------------------------------------------------------------
--DROP TABLE dw_waze.alert;
CREATE TABLE IF NOT EXISTS dw_waze.alert
(
  alert_char_crc VARCHAR(10) ENCODE lzo,
  alert_uuid           VARCHAR(50) ENCODE lzo,
  uuid_version         SMALLINT ENCODE lzo,
  current_flag         SMALLINT ENCODE lzo,
  etl_run_id           VARCHAR(50) ENCODE lzo,
  etl_current_flag     SMALLINT ENCODE lzo,
  total_occurences         SMALLINT ENCODE lzo,
  alert_type           VARCHAR(25) ENCODE lzo,
  sub_type             VARCHAR(70) ENCODE lzo,
  street               VARCHAR(100) ENCODE lzo,
  city                 VARCHAR(500) ENCODE lzo,
  state                VARCHAR(10) ENCODE lzo,
  country              VARCHAR(25) ENCODE lzo,
  num_thumbsup         SMALLINT ENCODE lzo,
  reliability          SMALLINT ENCODE lzo,
  confidence           SMALLINT ENCODE lzo,
  report_rating        SMALLINT ENCODE lzo,
  magvar               VARCHAR(50) ENCODE lzo,
  report_description   VARCHAR(1000) ENCODE lzo,
  location_lat         VARCHAR(50) ENCODE lzo,
  location_lon         VARCHAR(50) ENCODE lzo,
  jam_uuid             VARCHAR(100) ENCODE lzo,
  start_pub_millis           VARCHAR(50) ENCODE lzo,
  start_pub_utc_timestamp    TIMESTAMP WITHOUT TIME ZONE ENCODE lzo,
  pub_millis           VARCHAR(50) ENCODE lzo,
  pub_utc_timestamp    TIMESTAMP WITHOUT TIME ZONE ENCODE lzo,
  pub_utc_epoch_week   SMALLINT ENCODE lzo,
  road_type            VARCHAR(25) ENCODE lzo,
  load_date datetime default sysdate

)
DISTSTYLE KEY DISTKEY (alert_uuid)
SORTKEY (pub_utc_timestamp) ;
analyze dw_waze.alert;
COMMIT;