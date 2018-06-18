DROP TABLE IF EXISTS dw_waze.stage_irregularity_jam_{{ batchIdValue }} ;
CREATE TABLE IF NOT EXISTS dw_waze.stage_irregularity_jam_{{ batchIdValue }} 
(
	irregularity_id VARCHAR(50)   ENCODE zstd
	,jam_uuid VARCHAR(50)   ENCODE zstd
	,etl_run_id VARCHAR(50) ENCODE zstd
)
DISTSTYLE ALL
;

--DROP TABLE IF EXISTS dw_waze.irregularity_jam;
CREATE TABLE IF NOT EXISTS dw_waze.irregularity_jam
(
	irregularity_id VARCHAR(50)   ENCODE zstd
	,jam_uuid VARCHAR(50)   ENCODE zstd

)
DISTSTYLE ALL
;

commit;