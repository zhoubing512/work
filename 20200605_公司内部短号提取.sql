
   
SELECT * 
FROM datamart.data_dw_group_meb 
WHERE dy = '2020' AND dm = '02' 
--and unit_name like '%中国移动%德阳%'  
AND unit_id = '280RNWAVBS'
limit 5;

SELECT DISTINCT id_no,unit_id,unit_name
FROM datamart.data_dw_group_meb 
WHERE dy = '2020' AND dm = '02' 
AND unit_name LIKE '%中国移动%德阳%'  AND id_no = '2780qsELVVMIcd'
--AND unit_id = '280zHdQGxO'
limit 5;


+-------------+----------------------+
|   unit_id   |      unit_name       |
+-------------+----------------------+
| 280lJWgzVX  | 中国移动通信集团四川有限公司德阳分公司  |
| 280RNWAVBS  | 中国移动通信集团四川有限公司德阳分公司  |
| 280zHdQGxO  | 中国移动通信集团四川有限公司德阳分公司  |
| 280zsbQasM  | 中国移动通信集团四川有限公司德阳分公司  |

SELECT unit_id,unit_name,COUNT(DISTINCT phone_no)
FROM
(
SELECT DISTINCT phone_no,unit_id,unit_name
FROM datamart.data_dw_group_meb 
WHERE dy = '2020' AND dm = '02' 
AND unit_name LIKE '%中国移动%德阳%'  
--AND unit_id = '280zHdQGxO'
) a
GROUP BY unit_id,unit_name
limit 5;
+-------------+----------------------+------+
|   unit_id   |      unit_name       | _c2  |
+-------------+----------------------+------+
| 280RNWAVBS  | 中国移动通信集团四川有限公司德阳分公司  | 4    |
| 280zsbQasM  | 中国移动通信集团四川有限公司德阳分公司  | 47   |
| 280lJWgzVX  | 中国移动通信集团四川有限公司德阳分公司  | 984  |
| 280zHdQGxO  | 中国移动通信集团四川有限公司德阳分公司  | 55   |
+-------------+----------------------+------+

 --1838380YJMr 
 SELECT *
 FROM datamart.data_dm_uv_info_m
 WHERE dy = '2020' AND dm = '04' AND phone_no = '1838380YJMr';


SELECT *
FROM datamart.data_user_group
WHERE dy = '2020' and dm = '06' AND phone_no = '1370091GHSL'
limit 5;


--德阳移动 group_id 650zoXgVBf 
drop table workspace.zb_internal_short_num_20200605;
create table workspace.zb_internal_short_num_20200605 
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
SELECT DISTINCT phone_no,short_num
FROM datamart.data_user_group
WHERE dy = '2020' AND dm = '06' AND group_id = '650zoXgVBf' AND exp_time > '2020-06-05'
;

SELECT group_id,phone_no,short_num,state,eff_time,exp_time,group_id_no,group_code,group_type,group_type_name
FROM workspace.zb_internal_short_num_20200605 
limit 20;