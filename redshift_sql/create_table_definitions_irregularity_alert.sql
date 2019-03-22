

DROP TABLE IF EXISTS {{ dw_schema_name }}.stage_irregularity_alert_{{ batchIdValue }} ;
CREATE TABLE IF NOT EXISTS {{ dw_schema_name }}.stage_irregularity_alert_{{ batchIdValue }}
(
	irregularity_id VARCHAR(50)   ENCODE zstd
	,alert_uuid VARCHAR(50)   ENCODE zstd
	,elt_run_id VARCHAR(50)  ENCODE zstd
)
DISTSTYLE ALL
;

DROP TABLE IF EXISTS {{ dw_schema_name }}.int_irregularity_alert_{{ batchIdValue }} ;
CREATE TABLE IF NOT EXISTS {{ dw_schema_name }}.int_irregularity_alert_{{ batchIdValue }}
(
	irregularity_id VARCHAR(50)   ENCODE zstd
	,alert_uuid VARCHAR(50)   ENCODE zstd
	,elt_run_id VARCHAR(50)  ENCODE zstd
)
DISTSTYLE ALL
;


--DROP TABLE IF EXISTS {{ dw_schema_name }}.irregularity_alert;
CREATE TABLE IF NOT EXISTS {{ dw_schema_name }}.irregularity_alert
(
	irregularity_id VARCHAR(50)   ENCODE zstd
	,alert_uuid VARCHAR(50)   ENCODE zstd
)
DISTSTYLE ALL
;

commit;
