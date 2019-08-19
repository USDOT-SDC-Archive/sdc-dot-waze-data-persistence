COPY {{ dw_schema_name }}.stage_alert_nightly
FROM 's3://{{ curated_bucket_name }}/{{ manifest_curated_key }}'
IAM_ROLE '{{ redshift_role_arn }}'
REGION '{{ region_name }}'
GZIP
DELIMITER ','
CSV
QUOTE '"'
NULL as ''
FILLRECORD;
