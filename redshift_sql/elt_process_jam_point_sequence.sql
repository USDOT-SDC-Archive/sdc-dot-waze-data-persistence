
--------------------------------------------------------------------
--Update Batch Id
--------------------------------------------------------------------
update {{ dw_schema_name }}.stage_jam_point_sequence_{{ batchIdValue }}  set elt_run_id={{ batchIdValue }} ;

----------------------------------------------------
--ELT Load
----------------------------------------------------

INSERT INTO {{ dw_schema_name }}.int_jam_point_sequence_{{ batchIdValue }}
select sjps.jam_id 
	,sjps.location_x 
	,sjps.location_y 
	,sjps.sequence_order
	,sjps.elt_run_id
from {{ dw_schema_name }}.stage_jam_point_sequence_{{ batchIdValue }} sjps
left join {{ dw_schema_name }}.jam_point_sequence jps on
jps.jam_id=sjps.jam_id
AND jps.location_x=sjps.location_x
AND jps.location_y=sjps.location_y
AND jps.sequence_order=sjps.sequence_order
where jps.jam_id  is null
GROUP BY sjps.jam_id 
	,sjps.location_x 
	,sjps.location_y 
	,sjps.sequence_order
	,sjps.elt_run_id;


INSERT INTO {{ dw_schema_name }}.jam_point_sequence
select ijps.jam_id 
	,ijps.location_x 
	,ijps.location_y 
	,ijps.sequence_order
from {{ dw_schema_name }}.int_jam_point_sequence_{{ batchIdValue }} ijps;
commit;