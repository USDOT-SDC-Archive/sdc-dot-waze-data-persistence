--------------------------------------------------------------------
--Update Batch Id
--------------------------------------------------------------------
update dw_waze.stage_irregularity_point_sequence_{{ batchIdValue }}  set etl_run_id={{ batchIdValue }} ;


----------------------------------------
--ETL Load
----------------------------------------
INSERT INTO dw_waze.irregularity_point_sequence 
SELECT sips.irregularity_id,
       sips.location_x,
       sips.location_y,
       sips.sequence_order
FROM dw_waze.stage_irregularity_point_sequence_{{ batchIdValue }} sips
  LEFT JOIN dw_waze.irregularity_point_sequence ips
         ON sips.irregularity_id = ips.irregularity_id
        AND sips.location_x = ips.location_x
        AND sips.location_y = ips.location_y
        AND sips.sequence_order = ips.sequence_order
WHERE ips.irregularity_id is null
GROUP BY sips.irregularity_id,
       sips.location_x,
       sips.location_y,
       sips.sequence_order;

commit;