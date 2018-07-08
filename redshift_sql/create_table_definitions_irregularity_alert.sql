

DROP TABLE IF EXISTS dw_waze.stage_irregularity_alert_{{ batchIdValue }} ;
CREATE TABLE IF NOT EXISTS dw_waze.stage_irregularity_alert_{{ batchIdValue }} 
(
	irregularity_id VARCHAR(50)   ENCODE zstd
	,alert_uuid VARCHAR(50)   ENCODE zstd
	,etl_run_id VARCHAR(50)  ENCODE zstd
)
DISTSTYLE ALL
;

CREATE TABLE IF NOT EXISTS dw_waze.int_irregularity_alert_{{ batchIdValue }} 
(
	irregularity_id VARCHAR(50)   ENCODE zstd
	,alert_uuid VARCHAR(50)   ENCODE zstd
	,etl_run_id VARCHAR(50)  ENCODE zstd
)
DISTSTYLE ALL
;


--DROP TABLE IF EXISTS dw_waze.irregularity_alert;
CREATE TABLE IF NOT EXISTS dw_waze.irregularity_alert
(
	irregularity_id VARCHAR(50)   ENCODE zstd
	,alert_uuid VARCHAR(50)   ENCODE zstd
)
DISTSTYLE ALL
;

commit;
