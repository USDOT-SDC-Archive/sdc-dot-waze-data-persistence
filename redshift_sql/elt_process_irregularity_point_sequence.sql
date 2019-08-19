--------------------------------------------------------------------
--Update Batch Id
--------------------------------------------------------------------
update {{ dw_schema_name }}.stage_irregularity_point_sequence_{{ batchIdValue }}  set elt_run_id={{ batchIdValue }} ;
----------------------------------------
--ELT Load
----------------------------------------
INSERT INTO {{ dw_schema_name }}.int_irregularity_point_sequence_{{ batchIdValue }}
SELECT sips.irregularity_id,
       sips.location_x,
       sips.location_y,
       sips.sequence_order,
       sips.elt_run_id
FROM {{ dw_schema_name }}.stage_irregularity_point_sequence_{{ batchIdValue }} sips
  LEFT JOIN {{ dw_schema_name }}.irregularity_point_sequence ips
         ON sips.irregularity_id = ips.irregularity_id
        AND sips.location_x = ips.location_x
        AND sips.location_y = ips.location_y
        AND sips.sequence_order = ips.sequence_order
WHERE ips.irregularity_id is null
GROUP BY sips.irregularity_id,
       sips.location_x,
       sips.location_y,
       sips.sequence_order,
       sips.elt_run_id;


INSERT INTO {{ dw_schema_name }}.irregularity_point_sequence
SELECT sips.irregularity_id,
       sips.location_x,
       sips.location_y,
       sips.sequence_order
FROM {{ dw_schema_name }}.int_irregularity_point_sequence_{{ batchIdValue }} sips;

commit;