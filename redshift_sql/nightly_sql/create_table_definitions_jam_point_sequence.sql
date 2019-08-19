-------------------------------------
--Jam point sequence DDL
-------------------------------------
DROP TABLE IF EXISTS  {{ dw_schema_name }}.stage_jam_point_sequence_{{ batchIdValue }};
CREATE TABLE IF NOT EXISTS {{ dw_schema_name }}.stage_jam_point_sequence_{{ batchIdValue }}
(
	jam_id VARCHAR(50)   ENCODE zstd
	,location_x VARCHAR(50)   ENCODE zstd
	,location_y VARCHAR(50)   ENCODE zstd
	,sequence_order SMALLINT   ENCODE zstd
	,elt_run_id VARCHAR(50)   ENCODE zstd
)
DISTSTYLE KEY DISTKEY (jam_id);
;

--DROP TABLE {{ dw_schema_name }}.stage_jam_point_sequence;
CREATE TABLE IF NOT EXISTS {{ dw_schema_name }}.jam_point_sequence
(
	jam_id VARCHAR(50)   ENCODE zstd
	,location_x VARCHAR(50)   ENCODE zstd
	,location_y VARCHAR(50)   ENCODE zstd
	,sequence_order SMALLINT   ENCODE zstd
)
DISTSTYLE KEY DISTKEY (jam_id);
;
commit;