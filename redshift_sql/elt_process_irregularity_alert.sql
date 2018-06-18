
--------------------------------------------------------------------
--Update Batch Id
--------------------------------------------------------------------
update dw_waze.stage_irregularity_alert_{{ batchIdValue }}  set etl_run_id={{ batchIdValue }} ;

---------------------------------------
--ETL Load
---------------------------------------
INSERT INTO dw_waze.irregularity_alert
select sia.irregularity_id,
sia.alert_uuid
from dw_waze.stage_irregularity_alert_{{ batchIdValue }} sia
left join dw_waze.irregularity_alert ia 
   
ON sia.irregularity_id=ia.irregularity_id
AND sia.alert_uuid=ia.alert_uuid
where ia.irregularity_id is null
GROUP BY sia.irregularity_id,
sia.alert_uuid;
commit;