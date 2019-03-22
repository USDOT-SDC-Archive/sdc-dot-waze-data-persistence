
--------------------------------------------------------------------
--Update Batch Id
--------------------------------------------------------------------
update {{ dw_schema_name }}.stage_irregularity_alert_{{ batchIdValue }}  set elt_run_id={{ batchIdValue }} ;

---------------------------------------
--ELT Load
---------------------------------------
INSERT INTO {{ dw_schema_name }}.int_irregularity_alert_{{ batchIdValue }}
select sia.irregularity_id,
sia.alert_uuid,
sia.elt_run_id
from {{ dw_schema_name }}.stage_irregularity_alert_{{ batchIdValue }} sia
left join {{ dw_schema_name }}.irregularity_alert ia
ON sia.irregularity_id=ia.irregularity_id
AND sia.alert_uuid=ia.alert_uuid
where ia.irregularity_id is null
GROUP BY sia.irregularity_id,
sia.alert_uuid,
sia.elt_run_id;

--------------------------------------
--Load to target irregularity_alert
--------------------------------------

INSERT INTO {{ dw_schema_name }}.irregularity_alert
select sia.irregularity_id,
sia.alert_uuid
from {{ dw_schema_name }}.int_irregularity_alert_{{ batchIdValue }} sia;

commit;