---------------------------------------------------------------------
--irregularity table DDL
---------------------------------------------------------------------
DROP TABLE IF EXISTS dw_waze.stage_irregularity_{{ batchIdValue }} ;
CREATE TABLE IF NOT EXISTS dw_waze.stage_irregularity_{{ batchIdValue }}
(
  id                         VARCHAR(50) ENCODE zstd,
  irregularity_type          VARCHAR(25) ENCODE zstd,
  street                     VARCHAR(200) ENCODE zstd,
  city                       VARCHAR(200) ENCODE zstd,
  state                      VARCHAR(10) ENCODE zstd,
  country                    VARCHAR(10) ENCODE zstd,
  speed                      VARCHAR(100) ENCODE zstd,
  regular_speed              VARCHAR(100) ENCODE zstd,
  seconds                    INT ENCODE zstd,
  delay_seconds              INT ENCODE zstd,
  trend                      VARCHAR(100) ENCODE zstd,
  num_thumbsup               SMALLINT ENCODE zstd,
  irregularity_length        VARCHAR(100) ENCODE zstd,
  severity                   INT ENCODE zstd,
  jam_level                  INT ENCODE zstd,
  drivers_count              INT ENCODE zstd,
  alerts_count               INT ENCODE zstd,
  num_comments               VARCHAR(1000) ENCODE zstd,
  highway                    VARCHAR(100) ENCODE zstd,
  num_images                 VARCHAR(100) ENCODE zstd,
  end_node                   VARCHAR(100) ENCODE zstd,
  detection_date             VARCHAR(50) ENCODE zstd,
  detection_millis           VARCHAR(100) ENCODE zstd,
  detection_utc_timestamp    TIMESTAMP WITHOUT TIME ZONE ENCODE delta32k,
  detection_utc_epoch_week   SMALLINT ENCODE zstd,
  update_date                VARCHAR(50) ENCODE zstd,
  update_millis              VARCHAR(50) ENCODE zstd,
  update_utc_timestamp       TIMESTAMP WITHOUT TIME ZONE ENCODE delta32k,
  update_utc_epoch_week      SMALLINT ENCODE zstd,
  etl_run_id                 VARCHAR(50) ENCODE zstd
)
DISTSTYLE KEY DISTKEY (id) SORTKEY (detection_utc_timestamp);


CREATE TABLE IF NOT EXISTS dw_waze.irregularity 
(
  id                         VARCHAR(50) ENCODE zstd,
  irregularity_type          VARCHAR(25) ENCODE zstd,
  street                     VARCHAR(200) ENCODE zstd,
  city                       VARCHAR(200) ENCODE zstd,
  state                      VARCHAR(10) ENCODE zstd,
  country                    VARCHAR(10) ENCODE zstd,
  speed                      VARCHAR(100) ENCODE zstd,
  regular_speed              VARCHAR(100) ENCODE zstd,
  seconds                    INT ENCODE zstd,
  delay_seconds              INT ENCODE zstd,
  trend                      VARCHAR(100) ENCODE zstd,
  num_thumbsup               SMALLINT ENCODE zstd,
  irregularity_length        VARCHAR(100) ENCODE zstd,
  severity                   INT ENCODE zstd,
  jam_level                  INT ENCODE zstd,
  drivers_count              INT ENCODE zstd,
  alerts_count               INT ENCODE zstd,
  num_comments               VARCHAR(1000) ENCODE zstd,
  highway                    VARCHAR(100) ENCODE zstd,
  num_images                 VARCHAR(100) ENCODE zstd,
  end_node                   VARCHAR(100) ENCODE zstd,
  detection_date             VARCHAR(50) ENCODE zstd,
  detection_millis           VARCHAR(100) ENCODE zstd,
  detection_utc_timestamp    TIMESTAMP WITHOUT TIME ZONE ENCODE delta32k,
  detection_utc_epoch_week   SMALLINT ENCODE zstd,
  update_date                VARCHAR(50) ENCODE zstd,
  update_millis              VARCHAR(50) ENCODE zstd,
  update_utc_timestamp       TIMESTAMP WITHOUT TIME ZONE ENCODE delta32k,
  update_utc_epoch_week      SMALLINT ENCODE zstd
)
DISTSTYLE KEY DISTKEY (id) SORTKEY (detection_utc_timestamp);

commit;
