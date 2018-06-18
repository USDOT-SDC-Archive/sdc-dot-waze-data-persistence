COPY dw_waze.stage_irregularity_jam_{{ batchIdValue }}
FROM 's3://{{ curated_bucket_name }}/{{ manifest_curated_key }}'
IAM_ROLE '{{ redshift_role_arn }}'
REGION '{{ region_name }}'
GZIP
DELIMITER ','
CSV
QUOTE '"'
NULL as ''
FILLRECORD
MANIFEST;
