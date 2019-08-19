create schema IF NOT EXISTS dw_waze;
create schema IF NOT EXISTS elt_waze;
CREATE TABLE IF NOT EXISTS elt_waze.DW_TBL_INFO
( TABLE_ID    INT ENCODE lzo,
  TABLE_NAME VARCHAR(50) ENCODE lzo,
  SCHEMANAME VARCHAR(50) ENCODE lzo,
  TABLE_TYPE SMALLINT
  )
DISTSTYLE ALL
SORTKEY ( TABLE_ID );

insert into elt_waze.DW_TBL_INFO (TABLE_ID ,TABLE_NAME ,SCHEMANAME,TABLE_TYPE ) VALUES (1001, 'alert','dw_waze',2);
insert into elt_waze.DW_TBL_INFO (TABLE_ID ,TABLE_NAME ,SCHEMANAME,TABLE_TYPE ) VALUES (1002, 'jam','dw_waze',1);
insert into elt_waze.DW_TBL_INFO (TABLE_ID ,TABLE_NAME ,SCHEMANAME,TABLE_TYPE ) VALUES (1003, 'irregularity','dw_waze',1);
insert into elt_waze.DW_TBL_INFO (TABLE_ID ,TABLE_NAME ,SCHEMANAME,TABLE_TYPE ) VALUES (1004, 'irregularity_point_sequence','dw_waze',1);
insert into elt_waze.DW_TBL_INFO (TABLE_ID ,TABLE_NAME ,SCHEMANAME,TABLE_TYPE ) VALUES (1005, 'irregularity_alert','dw_waze',1);
insert into elt_waze.DW_TBL_INFO (TABLE_ID ,TABLE_NAME ,SCHEMANAME,TABLE_TYPE ) VALUES (1006, 'irregularity_jam','dw_waze',1);
insert into elt_waze.DW_TBL_INFO (TABLE_ID ,TABLE_NAME ,SCHEMANAME,TABLE_TYPE ) VALUES (1007, 'jam_point_sequence','dw_waze',1);
commit;