
号码,目前所在区县,目前所在乡镇,常驻区县,常驻乡镇,是否开通个人视频彩铃,是否开通集团视频彩铃,是否上网用户,号码状态,是否智能手机,是否有4G上网流量,是否开通VOLTE功能,归属V网

select * from  base_prc_info where prod_prc_name like '%企业%视频彩铃%'   or prod_prc_name like '% 集团%视频彩铃%'    limit 10;

select * from  base_prc_info where prod_prc_name like '%VOLTE%'limit 10;
##1月常住用户，本月所待时间最长的geo_code对应的乡镇
select count(distinct phone_no) from dwd_user_location_5minute where dy = '2020';

drop table workspace.zb_now_location_hour;
create table workspace.zb_now_location_hour
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select m.phone_no,m.geo_code,n.county_name,n.area_name,n.town_name,n.village_name
from 
(
select phone_no,geo_code
from 
(
select phone_no,geo_code,row_number()over(partition by phone_no order by sum_time desc) rk
from 
(
  select phone_no,geo_code,count(geo_code) sum_time 
  from business.dwd_user_location_hour
  where dy = '2020' and dm = '01' and (dd = '29' or dd = '28' and dd = '27')
  group by phone_no,geo_code
) a
) a
where rk = 1
) m
left join business.base_deyang_country_geocode n
on m.geo_code = n.geo_code
;



## 检查是否都匹配到位置
select count(*) from workspace.zb_now_location_hour where county_name is null or county_name = '';
select * from workspace.zb_now_location_hour where county_name is null or county_name = '' limit 10;

#农村用户,常驻乡镇
drop table workspace.zb_20200130_result;
create table workspace.zb_20200130_result
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select  distinct a.phone_no
      ,b.county_name as now_county_name
      ,b.town_name as now_town
      ,a.county_name as cz_county_name
      ,a.town as cz_town 
      ,case when c.phone_no is not null then 1 else 0 end as SFgeren
      ,case when d.phone_no is not null then 1 else 0 end as SFjituan
      ,case when e.phone_no is not null and e.is_usenet = '1' then '1' else '0' end as is_usenet
      ,e.run_code
      ,case when (f.os_flag like '%iOS%' or f.os_flag like '%ndroid%') then '1' else '0' end as SFtech_phone
      ,case when f.phone_no is not null and f.g4_user_flag = '1' then '1' else '0' end as SF4G_flow
      ,case when f.phone_no is not null and f.volte_if = '1' then '1' else '0' end as SF_volte
      ,case when f.vpmn_code like '838%' then f.vpmn_code else '' end as Vpmn
      ,case when f.vpmn_code like '280%' then f.vpmn_code else '' end as Vpmn_jituan
      ,case when g.phone_no is not null then g.call_times_12 else 0 end as call_times_12
from
(
  select phone_no,county_name,town,country_name,fenju
  from workspace.th_deyang_xiangzhen_2020 
  where fenju not like '城%'
) a
left join workspace.zb_now_location_hour b
on a.phone_no = b.phone_no
left join
(
  select distinct phone_no 
  from datamart.data_user_prc a
  inner join datamart.base_prc_info b 
  on a.prod_prcid = b.prod_prcid
  where b.prod_prc_name like '%个人%视频彩铃%'
) c
on a.phone_no = c.phone_no
left join
(
  select distinct phone_no 
  from datamart.data_user_prc a
  inner join datamart.base_prc_info b 
  on a.prod_prcid = b.prod_prcid
  where b.prod_prc_name like '%集团%视频彩铃%' or b.prod_prc_name like '%企业%视频彩铃%'
) d
on a.phone_no = d.phone_no
left join 
(
select distinct phone_no,is_usenet,run_code
from datamart.data_user_info
where dy = '2020'
) e
on a.phone_no = e.phone_no
left join
(
  select distinct phone_no,volte_if,vpmn_code,os_flag,g4_user_flag
  from datamart.data_dw_user_base_info
  where dy = '2019' and dm = '11'
) f
on a.phone_no = f.phone_no
left join 
(
select phone_no,sum(call_times) as call_times_12
from 
(
select phone_no,opposite_no,system_type,calltype_id,call_times,dial_type
from datamart.data_dwb_cal_user_voc_yx_ds
where dy = '2019' and dm = '12' and calltype_id = '02'
) a
group by phone_no 
) g
on a.phone_no = g.phone_no
;


select * from  data_user_group where dy = '2020'  and limit 10;
##检查多少目前所在地为空
select count(*) from workspace.zb_20200130_result where now_county_name is null;

select count(*) from workspace.zb_20200130_result where call_times_12 is null;

  select * from workspace.th_deyang_xiangzhen_2020 
  where fenju not like '城%' limit 10;
  
  
  select phone_no,volte_if,vpmn_code,os_flag,g4_user_flag
  from datamart.data_dw_user_base_info
  where  vpmn_code like '838%'
  limit 10
  ;
  
#长住用户表
dwd_obode_kb_202001

desc th_deyang_xiangzhen_2020 ;

select phone_no,run_code,SFtech_phone,SF4G_flow,SF_volte,Vpmn,Vpmn_jituan,call_times_12
from workspace.zb_20200130_result
limit 20
;
##12月用户被叫次数
select phone_no,sum(call_times) as call_times_12
from 
(
select phone_no,opposite_no,system_type,calltype_id,call_times,dial_type
from datamart.data_dwb_cal_user_voc_yx_ds
where dy = '2019' and dm = '12' and calltype_id = '02'
) a
group by phone_no 
limit 10
;