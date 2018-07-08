--------------------------------------------------------------------
--Update Batch Id
--------------------------------------------------------------------
update dw_waze.stage_irregularity_jam_{{ batchIdValue }}  set elt_run_id={{ batchIdValue }} ;

-------------------------------------
--ETL Load
-------------------------------------
INSERT INTO dw_waze.int_irregularity_jam_{{ batchIdValue }}
SELECT sij.irregularity_id,
       sij.jam_uuid,
       sij.elt_run_id
FROM dw_waze.stage_irregularity_jam_{{ batchIdValue }} sij
  LEFT JOIN dw_waze.irregularity_jam ij
         ON sij.irregularity_id = ij.irregularity_id
        AND sij.jam_uuid = ij.jam_uuid
WHERE sij.irregularity_id is  null
GROUP BY sij.irregularity_id,
         sij.jam_uuid,
       sij.elt_run_id;


INSERT INTO dw_waze.irregularity_jam
SELECT sij.irregularity_id,
       sij.jam_uuid
FROM dw_waze.int_stage_irregularity_jam_{{ batchIdValue }} sij;
commit;