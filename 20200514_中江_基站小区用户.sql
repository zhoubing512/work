--4月全量常驻基站小区用户
--zb_near_month_202004_cz_all

--4月中江 常驻基站小区用户
drop table workspace.zb_near_month_20200514_cz_zhongjiang;
create table workspace.zb_near_month_20200514_cz_zhongjiang
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  select distinct a.bs_id,a.bs_name,b.phone_no
  from 
  (
  select distinct cell_id,bs_id,bs_name
  from business.base_cell_info
  where day = '20200428' and county = '中江'
  ) a
  left join workspace.zb_near_month_202004_cz_all b
  on a.cell_id = b.ci
  ;

--导入数据 敏感用户
drop table workspace.zb_sensitive_phone_info_20200514;
create table workspace.zb_sensitive_phone_info_20200514(phone_no string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
putdata -f sensitive_phone_info.txt -t workspace.zb_sensitive_phone_info_20200514;

--th_en_zb_sensitive_phone_info_20200514 加密
--剔除敏感数据后的结果
drop table workspace.zb_near_month_20200514_cz_zhongjiang_result;
create table workspace.zb_near_month_20200514_cz_zhongjiang_result
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  select a.bs_id,a.bs_name,a.phone_no
  from workspace.zb_near_month_20200514_cz_zhongjiang a
  left join workspace.th_en_zb_sensitive_phone_info_20200514 b
  on a.phone_no = b.phone_no
  where b.phone_no is null
  ;
