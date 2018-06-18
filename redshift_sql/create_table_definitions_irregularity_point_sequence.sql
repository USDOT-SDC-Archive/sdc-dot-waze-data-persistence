DROP TABLE IF EXISTS dw_waze.stage_irregularity_point_sequence_{{ batchIdValue }} ;
CREATE TABLE IF NOT EXISTS dw_waze.stage_irregularity_point_sequence_{{ batchIdValue }} 
(
	irregularity_id VARCHAR(50)   ENCODE zstd
	,location_x VARCHAR(50)   ENCODE zstd
	,location_y VARCHAR(50)   ENCODE zstd
	,sequence_order SMALLINT   ENCODE zstd
	,etl_run_id VARCHAR(50)
)
DISTSTYLE ALL
;

DROP TABLE IF EXISTS dw_waze.irregularity_point_sequence;
CREATE TABLE IF NOT EXISTS dw_waze.irregularity_point_sequence
(
	irregularity_id VARCHAR(50)   ENCODE zstd
	,location_x VARCHAR(50)   ENCODE zstd
	,location_y VARCHAR(50)   ENCODE zstd
	,sequence_order SMALLINT   ENCODE zstd
	
)
DISTSTYLE ALL
;
commit;