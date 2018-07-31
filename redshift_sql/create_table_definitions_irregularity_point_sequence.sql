DROP TABLE IF EXISTS {{ dw_schema_name }}.stage_irregularity_point_sequence_{{ batchIdValue }} ;
CREATE TABLE IF NOT EXISTS {{ dw_schema_name }}.stage_irregularity_point_sequence_{{ batchIdValue }}
(
	irregularity_id VARCHAR(50)   ENCODE zstd
	,location_x VARCHAR(50)   ENCODE zstd
	,location_y VARCHAR(50)   ENCODE zstd
	,sequence_order SMALLINT   ENCODE zstd
	,elt_run_id VARCHAR(50)
)
DISTSTYLE ALL
;

DROP TABLE IF EXISTS {{ dw_schema_name }}.int_irregularity_point_sequence_{{ batchIdValue }} ;
CREATE TABLE IF NOT EXISTS {{ dw_schema_name }}.int_irregularity_point_sequence_{{ batchIdValue }}
(
	irregularity_id VARCHAR(50)   ENCODE zstd
	,location_x VARCHAR(50)   ENCODE zstd
	,location_y VARCHAR(50)   ENCODE zstd
	,sequence_order SMALLINT   ENCODE zstd
	,elt_run_id VARCHAR(50)
)
DISTSTYLE ALL
;

DROP TABLE IF EXISTS {{ dw_schema_name }}.irregularity_point_sequence;
CREATE TABLE IF NOT EXISTS {{ dw_schema_name }}.irregularity_point_sequence
(
	irregularity_id VARCHAR(50)   ENCODE zstd
	,location_x VARCHAR(50)   ENCODE zstd
	,location_y VARCHAR(50)   ENCODE zstd
	,sequence_order SMALLINT   ENCODE zstd
	
)
DISTSTYLE ALL
;
commit;