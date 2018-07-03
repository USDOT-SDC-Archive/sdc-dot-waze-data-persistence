----------------------------------------------------------------------------------------------------------------------------------------------------
--table definitions
----------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------
---Create TEMP Stage Alert table DDL
--------------------------------------------------------------------------------
create schema IF NOT EXISTS dw_waze;
DROP TABLE IF EXISTS dw_waze.stage_alert_{{ batchIdValue }};
CREATE TABLE IF NOT EXISTS dw_waze.stage_alert_{{ batchIdValue }}
( alert_uuid           VARCHAR(50) ENCODE zstd,
  alert_type           VARCHAR(25) ENCODE zstd,
  sub_type             VARCHAR(70) ENCODE zstd,
  street               VARCHAR(100) ENCODE zstd,
  city                 VARCHAR(500) ENCODE zstd,
  state                VARCHAR(10) ENCODE zstd,
  country              VARCHAR(25) ENCODE zstd,
  num_thumbsup         SMALLINT ENCODE zstd,
  reliability          SMALLINT ENCODE zstd,
  confidence           SMALLINT ENCODE zstd,
  report_rating        SMALLINT ENCODE zstd,
  magvar               VARCHAR(50) ENCODE zstd,
  report_description   VARCHAR(1000) ENCODE zstd,
  location_lat         VARCHAR(50) ENCODE zstd,
  location_lon         VARCHAR(50) ENCODE zstd,
  jam_uuid             VARCHAR(100) ENCODE zstd,
  pub_millis           VARCHAR(50) ENCODE zstd,
  pub_utc_timestamp    TIMESTAMP WITHOUT TIME ZONE ENCODE zstd,
  pub_utc_epoch_week   SMALLINT ENCODE zstd,
  road_type            VARCHAR(25) ENCODE zstd,
  etl_run_id           VARCHAR(50)

)
DISTSTYLE KEY DISTKEY (alert_uuid);
--------------------------------------------------------------------------------
---Create TEMP Intermediate Alert table DDL
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS dw_waze.int_alert_{{ batchIdValue }};
CREATE TABLE IF NOT EXISTS dw_waze.int_alert_{{ batchIdValue }}
(
  alert_char_crc VARCHAR(10) ENCODE zstd,
  alert_uuid           VARCHAR(50) ENCODE zstd,
  uuid_version         SMALLINT ENCODE zstd,
  etl_run_id           VARCHAR(50) ENCODE zstd,
  alert_type           VARCHAR(25) ENCODE zstd,
  sub_type             VARCHAR(70) ENCODE zstd,
  street               VARCHAR(100) ENCODE zstd,
  city                 VARCHAR(500) ENCODE zstd,
  state                VARCHAR(10) ENCODE zstd,
  country              VARCHAR(25) ENCODE zstd,
  num_thumbsup         SMALLINT ENCODE zstd,
  reliability          SMALLINT ENCODE zstd,
  confidence           SMALLINT ENCODE zstd,
  report_rating        SMALLINT ENCODE zstd,
  magvar               VARCHAR(50) ENCODE zstd,
  report_description   VARCHAR(1000) ENCODE zstd,
  location_lat         VARCHAR(50) ENCODE zstd,
  location_lon         VARCHAR(50) ENCODE zstd,
  jam_uuid             VARCHAR(100) ENCODE zstd,
  start_pub_millis           VARCHAR(50) ENCODE zstd,
  start_pub_utc_timestamp    TIMESTAMP WITHOUT TIME ZONE ENCODE zstd,
  pub_millis           VARCHAR(50) ENCODE zstd,
  pub_utc_timestamp    TIMESTAMP WITHOUT TIME ZONE ENCODE zstd,
  pub_utc_epoch_week   SMALLINT ENCODE zstd,
  road_type            VARCHAR(25) ENCODE zstd,
  total_occurences    INT ENCODE zstd,
  load_operation       VARCHAR(2) ENCODE zstd,
  current_flag         SMALLINT ENCODE zstd,
  revision_flag         SMALLINT ENCODE zstd
)
DISTSTYLE KEY DISTKEY (alert_uuid)
SORTKEY (pub_utc_timestamp);
--------------------------------------------------------------------------------
---Create TEMP Revised table
--------------------------------------------------------------------------------

DROP TABLE IF EXISTS dw_waze.revised_alert_{{ batchIdValue }};
CREATE TABLE IF NOT EXISTS dw_waze.revised_alert_{{ batchIdValue }}
(
  alert_char_crc VARCHAR(10) ENCODE zstd,
  alert_uuid           VARCHAR(50) ENCODE zstd,
  uuid_version         SMALLINT ENCODE zstd,
  current_flag         SMALLINT ENCODE zstd,
  etl_run_id           VARCHAR(50) ENCODE zstd,
  etl_current_flag     SMALLINT ENCODE zstd,
  total_occurences         SMALLINT ENCODE zstd,
  alert_type           VARCHAR(25) ENCODE zstd,
  sub_type             VARCHAR(70) ENCODE zstd,
  street               VARCHAR(100) ENCODE zstd,
  city                 VARCHAR(500) ENCODE zstd,
  state                VARCHAR(10) ENCODE zstd,
  country              VARCHAR(25) ENCODE zstd,
  num_thumbsup         SMALLINT ENCODE zstd,
  reliability          SMALLINT ENCODE zstd,
  confidence           SMALLINT ENCODE zstd,
  report_rating        SMALLINT ENCODE zstd,
  magvar               VARCHAR(50) ENCODE zstd,
  report_description   VARCHAR(1000) ENCODE zstd,
  location_lat         VARCHAR(50) ENCODE zstd,
  location_lon         VARCHAR(50) ENCODE zstd,
  jam_uuid             VARCHAR(100) ENCODE zstd,
  start_pub_millis           VARCHAR(50) ENCODE zstd,
  start_pub_utc_timestamp    TIMESTAMP WITHOUT TIME ZONE ENCODE zstd,
  pub_millis           VARCHAR(50) ENCODE zstd,
  pub_utc_timestamp    TIMESTAMP WITHOUT TIME ZONE ENCODE zstd,
  pub_utc_epoch_week   SMALLINT ENCODE zstd,
  road_type            VARCHAR(25) ENCODE zstd

)
DISTSTYLE KEY DISTKEY (alert_uuid)
SORTKEY (pub_utc_timestamp) ;

--------------------------------------------------------------------------------
---Alert table DDL
--------------------------------------------------------------------------------
--DROP TABLE dw_waze.alert;
CREATE TABLE IF NOT EXISTS dw_waze.alert
(
  alert_char_crc VARCHAR(10) ENCODE zstd,
  alert_uuid           VARCHAR(50) ENCODE zstd,
  uuid_version         SMALLINT ENCODE zstd,
  current_flag         SMALLINT ENCODE zstd,
  etl_run_id           VARCHAR(50) ENCODE zstd,
  total_occurences         SMALLINT ENCODE zstd,
  alert_type           VARCHAR(25) ENCODE zstd,
  sub_type             VARCHAR(70) ENCODE zstd,
  street               VARCHAR(100) ENCODE zstd,
  city                 VARCHAR(500) ENCODE zstd,
  state                VARCHAR(10) ENCODE zstd,
  country              VARCHAR(25) ENCODE zstd,
  num_thumbsup         SMALLINT ENCODE zstd,
  reliability          SMALLINT ENCODE zstd,
  confidence           SMALLINT ENCODE zstd,
  report_rating        SMALLINT ENCODE zstd,
  magvar               VARCHAR(50) ENCODE zstd,
  report_description   VARCHAR(1000) ENCODE zstd,
  location_lat         VARCHAR(50) ENCODE zstd,
  location_lon         VARCHAR(50) ENCODE zstd,
  jam_uuid             VARCHAR(100) ENCODE zstd,
  start_pub_millis           VARCHAR(50) ENCODE zstd,
  start_pub_utc_timestamp    TIMESTAMP WITHOUT TIME ZONE ENCODE zstd,
  pub_millis           VARCHAR(50) ENCODE zstd,
  pub_utc_timestamp    TIMESTAMP WITHOUT TIME ZONE ENCODE zstd,
  pub_utc_epoch_week   SMALLINT ENCODE zstd,
  road_type            VARCHAR(25) ENCODE zstd
)
DISTSTYLE KEY DISTKEY (alert_uuid)
SORTKEY (pub_utc_timestamp) ;
analyze dw_waze.alert;
COMMIT;