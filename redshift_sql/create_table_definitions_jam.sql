-------------------------------------------------------------------------
-- Jam Work load
-----------------------------------------------------------------------
create schema IF NOT EXISTS dw_waze;
DROP TABLE IF  EXISTS {{ dw_schema_name }}.stage_jam_{{ batchIdValue }} ;
CREATE TABLE IF NOT EXISTS {{ dw_schema_name }}.stage_jam_{{ batchIdValue }}
(
  id                    VARCHAR(50) ENCODE zstd,
  jam_uuid              VARCHAR(50) ENCODE zstd,
  jam_type              VARCHAR(10) ENCODE zstd,
  road_type             SMALLINT ENCODE zstd,
  street                VARCHAR(200) ENCODE zstd,
  city                  VARCHAR(200) ENCODE zstd,
  state                 VARCHAR(10) ENCODE zstd,
  country               VARCHAR(10) ENCODE zstd,
  speed                 VARCHAR(100) ENCODE zstd,
  start_node            VARCHAR(100) ENCODE zstd,
  end_node              VARCHAR(100) ENCODE zstd,
  "level"               SMALLINT ENCODE zstd,
  jam_length            VARCHAR(100) ENCODE zstd,
  delay                 INT ENCODE zstd,
  turn_line             VARCHAR(100) ENCODE zstd,
  turn_type             VARCHAR(25) ENCODE zstd,
  blocking_alert_uuid   VARCHAR(50) ENCODE zstd,
  pub_millis            VARCHAR(50) ENCODE zstd,
  pub_utc_timestamp     TIMESTAMP WITHOUT TIME ZONE ENCODE delta32k,
  pub_utc_epoch_week    SMALLINT ENCODE zstd,
  elt_run_id            VARCHAR(50) ENCODE zstd
)
DISTSTYLE KEY DISTKEY (id) SORTKEY (pub_utc_timestamp);


--------------------------------------------------------------------
--Intermediate Jam Table
--------------------------------------------------------------------
DROP TABLE IF EXISTS {{ dw_schema_name }}.int_jam_{{ batchIdValue }};
CREATE TABLE IF NOT EXISTS {{ dw_schema_name }}.int_jam_{{ batchIdValue }}
( id                    VARCHAR(50) ENCODE zstd,
  jam_uuid              VARCHAR(50) ENCODE zstd,
  jam_type              VARCHAR(10) ENCODE zstd,
  road_type             SMALLINT ENCODE zstd,
  street                VARCHAR(200) ENCODE zstd,
  city                  VARCHAR(200) ENCODE zstd,
  state                 VARCHAR(10) ENCODE zstd,
  country               VARCHAR(10) ENCODE zstd,
  speed                 VARCHAR(100) ENCODE zstd,
  start_node            VARCHAR(100) ENCODE zstd,
  end_node              VARCHAR(100) ENCODE zstd,
  "level"               SMALLINT ENCODE zstd,
  jam_length            VARCHAR(100) ENCODE zstd,
  delay                 INT ENCODE zstd,
  turn_line             VARCHAR(100) ENCODE zstd,
  turn_type             VARCHAR(25) ENCODE zstd,
  blocking_alert_uuid   VARCHAR(50) ENCODE zstd,
  pub_millis            VARCHAR(50) ENCODE zstd,
  pub_utc_timestamp     TIMESTAMP WITHOUT TIME ZONE ENCODE delta32k,
  pub_utc_epoch_week    SMALLINT ENCODE zstd,
  jam_md5               VARCHAR(100) ENCODE zstd,
  elt_run_id            VARCHAR(50) ENCODE zstd
)
DISTSTYLE KEY DISTKEY (id) SORTKEY (pub_utc_timestamp);

--------------------------------------------------------------------
--Jam Table
--------------------------------------------------------------------
--DROP TABLE IF EXISTS {{ dw_schema_name }}.jam;
CREATE TABLE IF NOT EXISTS {{ dw_schema_name }}.jam
( id                    VARCHAR(50) ENCODE zstd,
  jam_uuid              VARCHAR(50) ENCODE zstd,
  jam_type              VARCHAR(10) ENCODE zstd,
  road_type             SMALLINT ENCODE zstd,
  street                VARCHAR(200) ENCODE zstd,
  city                  VARCHAR(200) ENCODE zstd,
  state                 VARCHAR(10) ENCODE zstd,
  country               VARCHAR(10) ENCODE zstd,
  speed                 VARCHAR(100) ENCODE zstd,
  start_node            VARCHAR(100) ENCODE zstd,
  end_node              VARCHAR(100) ENCODE zstd,
  "level"               SMALLINT ENCODE zstd,
  jam_length            VARCHAR(100) ENCODE zstd,
  delay                 INT ENCODE zstd,
  turn_line             VARCHAR(100) ENCODE zstd,
  turn_type             VARCHAR(25) ENCODE zstd,
  blocking_alert_uuid   VARCHAR(50) ENCODE zstd,
  pub_millis            VARCHAR(50) ENCODE zstd,
  pub_utc_timestamp     TIMESTAMP WITHOUT TIME ZONE ENCODE delta32k,
  pub_utc_epoch_week    SMALLINT ENCODE zstd,
  jam_md5               VARCHAR(100) ENCODE zstd
)
DISTSTYLE KEY DISTKEY (id) SORTKEY (pub_utc_timestamp);