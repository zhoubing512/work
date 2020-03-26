--动漫类app用户，流量，次数
drop table workspace.zb_dm_appflow_1;
create table workspace.zb_dm_appflow_1
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select *  
from 
(
    select *
    from datamart.data_dw_xdr_gprs
    where dy = '2019' and dm = '12' and dd = '31'
) a
left semi join
(
    select app_id 
    from datamart.data_dw_xdr_gprs_app 
    where app_name like '%动漫%'
) b
on a.app_id = b.app_id
;
drop table workspace.zb_dm_appflow_2;
create table workspace.zb_dm_appflow_2
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select *  
from 
(
    select *
    from datamart.data_dw_xdr_gprs
    where dy = '2019' and dm = '12' and dd = '30'
) a
left semi join
(
    select app_id 
    from datamart.data_dw_xdr_gprs_app 
    where app_name like '%动漫%'
) b
on a.app_id = b.app_id
;
drop table workspace.zb_dm_appflow_3;
create table workspace.zb_dm_appflow_3
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select *  
from 
(
    select *
    from datamart.data_dw_xdr_gprs
    where dy = '2019' and dm = '12' and dd = '29'
) a
left semi join
(
    select app_id 
    from datamart.data_dw_xdr_gprs_app 
    where app_name like '%动漫%'
) b
on a.app_id = b.app_id
;
drop table workspace.zb_dm_appflow_4;
create table workspace.zb_dm_appflow_4
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select *  
from 
(
    select *
    from datamart.data_dw_xdr_gprs
    where dy = '2019' and dm = '12' and dd = '28'
) a
left semi join
(
    select app_id 
    from datamart.data_dw_xdr_gprs_app 
    where app_name like '%动漫%'
) b
on a.app_id = b.app_id
;
drop table workspace.zb_dm_appflow_5;
create table workspace.zb_dm_appflow_5
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select *  
from 
(
    select *
    from datamart.data_dw_xdr_gprs
    where dy = '2019' and dm = '12' and dd = '27'
) a
left semi join
(
    select app_id 
    from datamart.data_dw_xdr_gprs_app 
    where app_name like '%动漫%'
) b
on a.app_id = b.app_id
;
drop table workspace.zb_dm_appflow_6;
create table workspace.zb_dm_appflow_6
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select *  
from 
(
    select *
    from datamart.data_dw_xdr_gprs
    where dy = '2019' and dm = '12' and dd = '26'
) a
left semi join
(
    select app_id 
    from datamart.data_dw_xdr_gprs_app 
    where app_name like '%动漫%'
) b
on a.app_id = b.app_id
;
drop table workspace.zb_dm_appflow_7;
create table workspace.zb_dm_appflow_7
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select *  
from 
(
    select *
    from datamart.data_dw_xdr_gprs
    where dy = '2019' and dm = '12' and dd = '25'
) a
left semi join
(
    select app_id 
    from datamart.data_dw_xdr_gprs_app 
    where app_name like '%动漫%'
) b
on a.app_id = b.app_id
;
drop table workspace.zb_dm_appflow_8;
create table workspace.zb_dm_appflow_8
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select *  
from 
(
    select *
    from datamart.data_dw_xdr_gprs
    where dy = '2019' and dm = '12' and dd = '24'
) a
left semi join
(
    select app_id 
    from datamart.data_dw_xdr_gprs_app 
    where app_name like '%动漫%'
) b
on a.app_id = b.app_id
;
drop table workspace.zb_dm_appflow_9;
create table workspace.zb_dm_appflow_9
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select *  
from 
(
    select *
    from datamart.data_dw_xdr_gprs
    where dy = '2019' and dm = '12' and dd = '23'
) a
left semi join
(
    select app_id 
    from datamart.data_dw_xdr_gprs_app 
    where app_name like '%动漫%'
) b
on a.app_id = b.app_id
;
drop table workspace.zb_dm_appflow_10;
create table workspace.zb_dm_appflow_10
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select *  
from 
(
    select *
    from datamart.data_dw_xdr_gprs
    where dy = '2019' and dm = '12' and dd = '22'
) a
left semi join
(
    select app_id 
    from datamart.data_dw_xdr_gprs_app 
    where app_name like '%动漫%'
) b
on a.app_id = b.app_id
;
drop table workspace.zb_dm_appflow_11;
create table workspace.zb_dm_appflow_11
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select *  
from 
(
    select *
    from datamart.data_dw_xdr_gprs
    where dy = '2019' and dm = '12' and dd = '21'
) a
left semi join
(
    select app_id 
    from datamart.data_dw_xdr_gprs_app 
    where app_name like '%动漫%'
) b
on a.app_id = b.app_id
;
drop table workspace.zb_dm_appflow_12;
create table workspace.zb_dm_appflow_12
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select *  
from 
(
    select *
    from datamart.data_dw_xdr_gprs
    where dy = '2019' and dm = '12' and dd = '20'
) a
left semi join
(
    select app_id 
    from datamart.data_dw_xdr_gprs_app 
    where app_name like '%动漫%'
) b
on a.app_id = b.app_id
;

drop table workspace.zb_dm_appflow_13;
create table workspace.zb_dm_appflow_13
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select *  
from 
(
    select *
    from datamart.data_dw_xdr_gprs
    where dy = '2019' and dm = '12' and dd = '19'
) a
left semi join
(
    select app_id 
    from datamart.data_dw_xdr_gprs_app 
    where app_name like '%动漫%'
) b
on a.app_id = b.app_id
;
drop table workspace.zb_dm_appflow_14;
create table workspace.zb_dm_appflow_14
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select *  
from 
(
    select *
    from datamart.data_dw_xdr_gprs
    where dy = '2019' and dm = '12' and dd = '18'
) a
left semi join
(
    select app_id 
    from datamart.data_dw_xdr_gprs_app 
    where app_name like '%动漫%'
) b
on a.app_id = b.app_id
;
drop table workspace.zb_dm_appflow_15;
create table workspace.zb_dm_appflow_15
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select *  
from 
(
    select *
    from datamart.data_dw_xdr_gprs
    where dy = '2019' and dm = '12' and dd = '17'
) a
left semi join
(
    select app_id 
    from datamart.data_dw_xdr_gprs_app 
    where app_name like '%动漫%'
) b
on a.app_id = b.app_id
;
drop table workspace.zb_dm_appflow_16;
create table workspace.zb_dm_appflow_16
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select *  
from 
(
    select *
    from datamart.data_dw_xdr_gprs
    where dy = '2019' and dm = '12' and dd = '16'
) a
left semi join
(
    select app_id 
    from datamart.data_dw_xdr_gprs_app 
    where app_name like '%动漫%'
) b
on a.app_id = b.app_id
;
-动漫类app用户，流量，次数
drop table workspace.zb_dm_appflow_17;
create table workspace.zb_dm_appflow_17
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select *  
from 
(
    select *
    from datamart.data_dw_xdr_gprs
    where dy = '2019' and dm = '12' and dd = '15'
) a
left semi join
(
    select app_id 
    from datamart.data_dw_xdr_gprs_app 
    where app_name like '%动漫%'
) b
on a.app_id = b.app_id
;
drop table workspace.zb_dm_appflow_18;
create table workspace.zb_dm_appflow_18
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select *  
from 
(
    select *
    from datamart.data_dw_xdr_gprs
    where dy = '2019' and dm = '12' and dd = '14'
) a
left semi join
(
    select app_id 
    from datamart.data_dw_xdr_gprs_app 
    where app_name like '%动漫%'
) b
on a.app_id = b.app_id
;
drop table workspace.zb_dm_appflow_19;
create table workspace.zb_dm_appflow_19
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select *  
from 
(
    select *
    from datamart.data_dw_xdr_gprs
    where dy = '2019' and dm = '12' and dd = '13'
) a
left semi join
(
    select app_id 
    from datamart.data_dw_xdr_gprs_app 
    where app_name like '%动漫%'
) b
on a.app_id = b.app_id
;
drop table workspace.zb_dm_appflow_20;
create table workspace.zb_dm_appflow_20
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select *  
from 
(
    select *
    from datamart.data_dw_xdr_gprs
    where dy = '2019' and dm = '12' and dd = '12'
) a
left semi join
(
    select app_id 
    from datamart.data_dw_xdr_gprs_app 
    where app_name like '%动漫%'
) b
on a.app_id = b.app_id
;
drop table workspace.zb_dm_appflow_21;
create table workspace.zb_dm_appflow_21
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select *  
from 
(
    select *
    from datamart.data_dw_xdr_gprs
    where dy = '2019' and dm = '12' and dd = '11'
) a
left semi join
(
    select app_id 
    from datamart.data_dw_xdr_gprs_app 
    where app_name like '%动漫%'
) b
on a.app_id = b.app_id
;
drop table workspace.zb_dm_appflow_22;
create table workspace.zb_dm_appflow_22
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select *  
from 
(
    select *
    from datamart.data_dw_xdr_gprs
    where dy = '2019' and dm = '12' and dd = '10'
) a
left semi join
(
    select app_id 
    from datamart.data_dw_xdr_gprs_app 
    where app_name like '%动漫%'
) b
on a.app_id = b.app_id
;
drop table workspace.zb_dm_appflow_23;
create table workspace.zb_dm_appflow_23
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select *  
from 
(
    select *
    from datamart.data_dw_xdr_gprs
    where dy = '2019' and dm = '12' and dd = '9'
) a
left semi join
(
    select app_id 
    from datamart.data_dw_xdr_gprs_app 
    where app_name like '%动漫%'
) b
on a.app_id = b.app_id
;
drop table workspace.zb_dm_appflow_24;
create table workspace.zb_dm_appflow_24
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select *  
from 
(
    select *
    from datamart.data_dw_xdr_gprs
    where dy = '2019' and dm = '12' and dd = '8'
) a
left semi join
(
    select app_id 
    from datamart.data_dw_xdr_gprs_app 
    where app_name like '%动漫%'
) b
on a.app_id = b.app_id
;
drop table workspace.zb_dm_appflow_25;
create table workspace.zb_dm_appflow_25
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select *  
from 
(
    select *
    from datamart.data_dw_xdr_gprs
    where dy = '2019' and dm = '12' and dd = '7'
) a
left semi join
(
    select app_id 
    from datamart.data_dw_xdr_gprs_app 
    where app_name like '%动漫%'
) b
on a.app_id = b.app_id
;
drop table workspace.zb_dm_appflow_26;
create table workspace.zb_dm_appflow_26
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select *  
from 
(
    select *
    from datamart.data_dw_xdr_gprs
    where dy = '2019' and dm = '12' and dd = '6'
) a
left semi join
(
    select app_id 
    from datamart.data_dw_xdr_gprs_app 
    where app_name like '%动漫%'
) b
on a.app_id = b.app_id
;
drop table workspace.zb_dm_appflow_27;
create table workspace.zb_dm_appflow_27
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select *  
from 
(
    select *
    from datamart.data_dw_xdr_gprs
    where dy = '2019' and dm = '12' and dd = '5'
) a
left semi join
(
    select app_id 
    from datamart.data_dw_xdr_gprs_app 
    where app_name like '%动漫%'
) b
on a.app_id = b.app_id
;
drop table workspace.zb_dm_appflow_28;
create table workspace.zb_dm_appflow_28
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select *  
from 
(
    select *
    from datamart.data_dw_xdr_gprs
    where dy = '2019' and dm = '12' and dd = '4'
) a
left semi join
(
    select app_id 
    from datamart.data_dw_xdr_gprs_app 
    where app_name like '%动漫%'
) b
on a.app_id = b.app_id
;
drop table workspace.zb_dm_appflow_29;
create table workspace.zb_dm_appflow_29
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select *  
from 
(
    select *
    from datamart.data_dw_xdr_gprs
    where dy = '2019' and dm = '12' and dd = '3'
) a
left semi join
(
    select app_id 
    from datamart.data_dw_xdr_gprs_app 
    where app_name like '%动漫%'
) b
on a.app_id = b.app_id
;
drop table workspace.zb_dm_appflow_30;
create table workspace.zb_dm_appflow_30
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select *  
from 
(
    select *
    from datamart.data_dw_xdr_gprs
    where dy = '2019' and dm = '12' and dd = '2'
) a
left semi join
(
    select app_id 
    from datamart.data_dw_xdr_gprs_app 
    where app_name like '%动漫%'
) b
on a.app_id = b.app_id
;
drop table workspace.zb_dm_appflow_31;
create table workspace.zb_dm_appflow_31
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select *  
from 
(
    select *
    from datamart.data_dw_xdr_gprs
    where dy = '2019' and dm = '12' and dd = '1'
) a
left semi join
(
    select app_id 
    from datamart.data_dw_xdr_gprs_app 
    where app_name like '%动漫%'
) b
on a.app_id = b.app_id
;

drop table workspace.zb_dm_appflow_all;
create table workspace.zb_dm_appflow
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
    select * 
    from workspace.zb_dm_appflow_1
    union all
    select * 
    from workspace.zb_dm_appflow_2
    union all
    select * 
    from workspace.zb_dm_appflow_3
    union all
    select * 
    from workspace.zb_dm_appflow_4
    union all
    select * 
    from workspace.zb_dm_appflow_5
    union all
    select * 
    from workspace.zb_dm_appflow_6
        union all
    select * 
    from workspace.zb_dm_appflow_7
    union all
    select * 
    from workspace.zb_dm_appflow_8
    union all
    select * 
    from workspace.zb_dm_appflow_9
    union all
    select * 
    from workspace.zb_dm_appflow_10
    union all
    select * 
    from workspace.zb_dm_appflow_11
        union all
    select * 
    from workspace.zb_dm_appflow_12
    union all
    select * 
    from workspace.zb_dm_appflow_13
    union all
    select * 
    from workspace.zb_dm_appflow_14
    union all
    select * 
    from workspace.zb_dm_appflow_15
    union all
    select * 
    from workspace.zb_dm_appflow_16
        union all
    select * 
    from workspace.zb_dm_appflow_17
    union all
    select * 
    from workspace.zb_dm_appflow_18
    union all
    select * 
    from workspace.zb_dm_appflow_19
    union all
    select * 
    from workspace.zb_dm_appflow_20
    union all
    select * 
    from workspace.zb_dm_appflow_21
        union all
    select * 
    from workspace.zb_dm_appflow_22
    union all
    select * 
    from workspace.zb_dm_appflow_23
    union all
    select * 
    from workspace.zb_dm_appflow_24
    union all
    select * 
    from workspace.zb_dm_appflow_25
    union all
    select * 
    from workspace.zb_dm_appflow_26
        union all
    select * 
    from workspace.zb_dm_appflow_27
    union all
    select * 
    from workspace.zb_dm_appflow_28
    union all
    select * 
    from workspace.zb_dm_appflow_29
    union all
    select * 
    from workspace.zb_dm_appflow_30
    union all
    select * 
    from workspace.zb_dm_appflow_31
;

--动漫App使用流量
drop table workspace.zb_hdm_flow_t3;
create table workspace.zb_hdm_flow_t3
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
    select phone_no,dd
      ,round((sum(flow)/1024),0) as flow_day
      ,sum(times) as times_day
      ,round((sum(time_duration)/60),0) as time_duration_day
    from (
        select phone_no,dd,flow,times,time_duration
        from workspace.zb_dm_appflow_all
    ) c
    group by phone_no,dd
;
--动漫 均值 标准差 振幅 振幅/均值  趋势
drop table workspace.zb_hdm_flow_t4;
create table workspace.zb_hdm_flow_t4
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  select a.phone_no,a.flow_means,a.times_means,a.time_duration_means,a.ft
    ,sqrt(sum(power(a.flow_day-a.flow_means,2)/(a.ft-1))) as flow_std,max(a.flow_day)-min(a.flow_day) as flow_day_dif,(max(a.flow_day)-min(a.flow_day))/flow_means as flow_day_dif_std,(a.ft*sum(a.rn*a.flow_day)-sum(a.rn)*sum(a.flow_day))/(a.ft*sum(a.rn*a.rn)-sum(a.rn)*sum(a.rn)) as flow_trend
    ,sqrt(sum(power(a.times_day-a.times_means,2)/(a.ft-1))) as times_std,max(a.times_day)-min(a.times_day) as times_day_dif,(max(a.times_day)-min(a.times_day))/times_means as times_day_dif_std,(a.ft*sum(a.rn*a.times_day)-sum(a.rn)*sum(a.times_day))/(a.ft*sum(a.rn*a.rn)-sum(a.rn)*sum(a.rn)) as times_trend
    ,sqrt(sum(power(a.time_duration_day-a.time_duration_day,2)/(a.ft-1))) as time_duration_std,max(a.time_duration_day)-min(a.time_duration_day) as time_duration_day_dif,(max(a.time_duration_day)-min(a.time_duration_day))/time_duration_means as time_duration_day_dif_std,(a.ft*sum(a.rn*a.time_duration_day)-sum(a.rn)*sum(a.time_duration_day))/(a.ft*sum(a.rn*a.rn)-sum(a.rn)*sum(a.rn)) as time_duration_trend
  from
  (
    select a.phone_no,a.flow_day,a.times_day,a.time_duration_day,a.rn
        ,avg(a.flow_day) over(partition by a.phone_no order by a.rn rows between unbounded preceding and unbounded following) as flow_means
        ,avg(a.times_day) over(partition by a.phone_no order by a.rn rows between unbounded preceding and unbounded following) as times_means
        ,avg(a.time_duration_day) over(partition by a.phone_no order by a.rn rows between unbounded preceding and unbounded following) as time_duration_means
        ,sum(1) over(partition by a.phone_no order by a.rn rows between unbounded preceding and unbounded following) as ft
    from 
    (
        select a.phone_no,a.flow_day,a.times_day,a.time_duration_day
          ,row_number() over(partition by a.phone_no order by a.dd asc) as rn 
        from 
        (
            select a.phone_no,dd,flow_day,a.times_day,a.time_duration_day
            from workspace.zb_hdm_flow_t3 a
        ) a
    ) a
  ) a
  where a.ft >= 3
  group by a.phone_no,a.flow_means,a.times_means,a.time_duration_means,a.ft
;


select count(distinct phone_no) 
from zb_dm_train a
where a.dm_label = 0 and a.phone_no in (
    select phone_no 
    from workspace.zb_dm_appflow_all
)
;

219156 dm_label = 0
63622 dm_label = 1



select app_id 
from datamart.data_dw_xdr_gprs_app 
where app_type = '动漫'
;

zb_dm_appflow_31_t1  --12月
zb_dm_appflow_31_t1_11  --11月
drop table workspace.zb_dm_appflow_31_t1_11;
create table workspace.zb_dm_appflow_31_t1_11
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  select *
  from datamart.data_dw_xdr_gprs
  where dy = '2019' and dm = '11'
   and (app_id = '10-40670'
   or app_id = '10-7008'
   or app_id = '10-1'
   or app_id = '10-304'
   or app_id = '10-316'
   or app_id = '10-328'
   or app_id = '10-305'
   or app_id = '10-317'
   or app_id = '10-329'
   or app_id = '10-60194'
   or app_id = '10-60875'
   or app_id = '10-0010'
   or app_id = '10-306'
   or app_id = '10-318'
   or app_id = '10-32994'
   or app_id = '10-46341'
   or app_id = '10-49272'
   or app_id = '10-49380'
   or app_id = '10-0011'
   or app_id = '10-0023'
   or app_id = '10-307'
   or app_id = '10-319'
   or app_id = '10-40516'
   or app_id = '10-46807'
   or app_id = '10-49285'
   or app_id = '10-0012'
   or app_id = '10-0024'
   or app_id = '10-308'
   or app_id = '10-41276'
   or app_id = '10-48074'
   or app_id = '10-49022'
   or app_id = '10-340'
   or app_id = '10-46840'
   or app_id = '10-49423'
   or app_id = '10-7007'
   or app_id = '10-7019'
   or app_id = '10-303'
   or app_id = '10-327'
   or app_id = '10-48558'
   or app_id = '10-48990'
   or app_id = '10-47196'
   or app_id = '10-48480'
   or app_id = '10-62427'
   or app_id = '10-7000'
   or app_id = '10-7012'
   or app_id = '10-7024'
   or app_id = '10-7036'
   or app_id = '10-7013'
   or app_id = '10-7025'
   or app_id = '10-7037'
   or app_id = '10-61422'
   or app_id = '10-7003'
   or app_id = '10-7015'
   or app_id = '10-7027'
   or app_id = '10-7039'
   or app_id = '10-7004'
   or app_id = '10-7016'
   or app_id = '10-7028'
   or app_id = '10-46742'
   or app_id = '10-7005'
   or app_id = '10-7017'
   or app_id = '10-7029'
   or app_id = '10-40200'
   or app_id = '10-7006'
   or app_id = '10-7018'
   or app_id = '10-330'
   or app_id = '10-342'
   or app_id = '10-46842'
   or app_id = '10-7009'
   or app_id = '10-331'
   or app_id = '10-343'
   or app_id = '10-40552'
   or app_id = '10-46519'
   or app_id = '10-47230'
   or app_id = '10-60457'
   or app_id = '10-62068'
   or app_id = '10-31829'
   or app_id = '10-320'
   or app_id = '10-332'
   or app_id = '10-333'
   or app_id = '10-345'
   or app_id = '10-47004'
   )
;

zb_dm_appflow_31_t2 --12月
zb_dm_appflow_31_t2_11  --11月
drop table workspace.zb_dm_appflow_31_t2_11;
create table workspace.zb_dm_appflow_31_t2_11
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  select *
  from datamart.data_dw_xdr_gprs
  where dy = '2019' and dm = '11'
   and (app_id = '10-61419'
   or app_id = '10-300'
   or app_id = '10-312'
   or app_id = '10-324'
   or app_id = '10-336'
   or app_id = '10-40425'
   or app_id = '10-60882'
   or app_id = '10-302'
   or app_id = '10-314'
   or app_id = '10-326'
   or app_id = '10-338'
   or app_id = '10-0001'
   or app_id = '10-0013'
   or app_id = '10-309'
   or app_id = '10-31317'
   or app_id = '10-40293'
   or app_id = '10-48744'
   or app_id = '10-61254'
   or app_id = '10-62454'
   or app_id = '10-0002'
   or app_id = '10-0014'
   or app_id = '10-40282'
   or app_id = '10-47533'
   or app_id = '10-7040'
   or app_id = '10-0003'
   or app_id = '10-0015'
   or app_id = '10-7041'
   or app_id = '10-8'
   or app_id = '10-0004'
   or app_id = '10-0016'
   or app_id = '10-62793'
   or app_id = '10-7030'
   or app_id = '10-7042'
   or app_id = '10-9'
   or app_id = '10-0017'
   or app_id = '10-46336'
   or app_id = '10-46444'
   or app_id = '10-47872'
   or app_id = '10-0006'
   or app_id = '10-0018'
   or app_id = '10-31850'
   or app_id = '10-7020'
   or app_id = '10-7032'
   or app_id = '10-61421'
   or app_id = '10-7002'
   or app_id = '10-7014'
   or app_id = '10-7026'
   or app_id = '10-7038'
   or app_id = '10-0007'
   or app_id = '10-0019'
   or app_id = '10-46314'
   or app_id = '10-46326'
   or app_id = '10-46794'
   or app_id = '10-47778'
   or app_id = '10-48069'
   or app_id = '10-7021'
   or app_id = '10-7033'
   or app_id = '10-0008'
   or app_id = '10-10'
   or app_id = '10-40372'
   or app_id = '10-47194'
   or app_id = '10-7010'
   or app_id = '10-7022'
   or app_id = '10-7034'
   or app_id = '10-0009'
   or app_id = '10-11'
   or app_id = '10-46520'
   or app_id = '10-7011'
   or app_id = '10-7023'
   or app_id = '10-7035'
   or app_id = '10-310'
   or app_id = '10-334'
   or app_id = '10-346'
   or app_id = '10-311'
   or app_id = '10-323'
   or app_id = '10-335'
   or app_id = '10-46370'
   or app_id = '10-47927'
   or app_id = '10-301'
   or app_id = '10-313'
   or app_id = '10-325'
   or app_id = '10-337'
   or app_id = '10-40438'
   )
;
--12月
drop table workspace.zb_dm_appflow_all;
create table workspace.zb_dm_appflow_all
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select * 
from workspace.zb_dm_appflow_31_t1
union all
select * 
from workspace.zb_dm_appflow_31_t2
;
--11月
drop table workspace.zb_dm_appflow_all_11;
create table workspace.zb_dm_appflow_all_11
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select * 
from workspace.zb_dm_appflow_31_t1_11
union all
select * 
from workspace.zb_dm_appflow_31_t2_11
;











drop table workspace.zb_dm_appflow_all_train;
create table workspace.zb_dm_appflow_all_train
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select a.*
    ,b.flow_means as dm_flow_means
    ,b.times_means as dm_times_means
    ,b.time_duration_means as dm_time_duration_means
    ,b.flow_std as dm_flow_std
    ,b.flow_day_dif as dm_flow_day_dif
    ,b.flow_day_dif_std as dm_flow_day_dif_std
    ,b.flow_trend as dm_flow_trend
    ,b.times_std as dm_times_std
    ,b.times_day_dif as dm_times_day_dif
    ,b.times_day_dif_std as dm_times_day_dif_std
    ,b.times_trend as dm_times_trend
    ,b.time_duration_std as dm_time_duration_std
    ,b.time_duration_day_dif as dm_time_duration_day_dif
    ,b.time_duration_day_dif_std as dm_time_duration_day_dif_std
    ,b.time_duration_trend as dm_time_duration_trend
from workspace.zb_dm_train a
inner join workspace.zb_hdm_flow_t4 b
on a.phone_no = b.phone_no
;
    dm_flow_means,dm_times_means,dm_time_duration_means,dm_flow_std,dm_flow_day_dif,dm_flow_day_dif_std,dm_flow_trend,dm_times_std,dm_times_day_dif,dm_times_day_dif_std,dm_times_trend,dm_time_duration_std,dm_time_duration_day_dif,dm_time_duration_day_dif_std,dm_time_duration_trend                              
select dm_flow_means,dm_times_means,dm_time_duration_means,dm_label
from zb_dm_appflow_all_train limit 50;
--导出表到文件
insert overwrite local directory '/mnt/disk1/user/lj/results/zb.zb_dm_appflow_all_predict_two' row format delimited fields 
terminated by ',' 
select * from workspace.zb_dm_appflow_all_predict_two where 1=1 
;
--退出hive压缩文件
merge_reduce.sh /mnt/disk1/user/lj/results/zb.zb_dm_appflow_all_predict_two;



drop table workspace.zb_dm_appflow_all_predict_one;
create table workspace.zb_dm_appflow_all_predict_one
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select a.*
    ,b.flow_means as dm_flow_means
    ,b.times_means as dm_times_means
    ,b.time_duration_means as dm_time_duration_means
    ,b.flow_std as dm_flow_std
    ,b.flow_day_dif as dm_flow_day_dif
    ,b.flow_day_dif_std as dm_flow_day_dif_std
    ,b.flow_trend as dm_flow_trend
    ,b.times_std as dm_times_std
    ,b.times_day_dif as dm_times_day_dif
    ,b.times_day_dif_std as dm_times_day_dif_std
    ,b.times_trend as dm_times_trend
    ,b.time_duration_std as dm_time_duration_std
    ,b.time_duration_day_dif as dm_time_duration_day_dif
    ,b.time_duration_day_dif_std as dm_time_duration_day_dif_std
    ,b.time_duration_trend as dm_time_duration_trend
from workspace.zb_dm_predict_one a
inner join workspace.zb_hdm_flow_t4 b
on a.phone_no = b.phone_no
;
 workspace.zb_dm_predict_one


drop table workspace.zb_dm_appflow_all_predict_two;
create table workspace.zb_dm_appflow_all_predict_two
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select a.*
    ,b.flow_means as dm_flow_means
    ,b.times_means as dm_times_means
    ,b.time_duration_means as dm_time_duration_means
    ,b.flow_std as dm_flow_std
    ,b.flow_day_dif as dm_flow_day_dif
    ,b.flow_day_dif_std as dm_flow_day_dif_std
    ,b.flow_trend as dm_flow_trend
    ,b.times_std as dm_times_std
    ,b.times_day_dif as dm_times_day_dif
    ,b.times_day_dif_std as dm_times_day_dif_std
    ,b.times_trend as dm_times_trend
    ,b.time_duration_std as dm_time_duration_std
    ,b.time_duration_day_dif as dm_time_duration_day_dif
    ,b.time_duration_day_dif_std as dm_time_duration_day_dif_std
    ,b.time_duration_trend as dm_time_duration_trend
from workspace.zb_dm_predict_two a
inner join workspace.zb_hdm_flow_t4 b
on a.phone_no = b.phone_no
;
 zb_dm_predict_two
select distinct app_id,app_type,app_name
from datamart.data_dw_xdr_gprs_app 
where app_id = '10-0017' or app_id = '10-307' or app_id = '10-318'
or app_id = '10-0024'
;

--11,12月数据，对每个用户进行周划分，1-7 ， 8-14 ，15-21 ，22-月末
drop table workspace.zb_dm_appflow_all_11_fenduan;
create table workspace.zb_dm_appflow_all_11_fenduan
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  select a.phone_no,a.flow_11_1,a.flow_11_2,a.flow_11_3,a.flow_11_4
    ,a.times_11_1,a.times_11_2,a.times_11_3,a.times_11_4
    ,a.time_duration_11_1,a.time_duration_11_2,a.time_duration_11_3,a.time_duration_11_4
  from
  (
    select a.phone_no
      ,a.flow as flow_11_1
      ,lead(flow,1) over(partition by a.phone_no order by fenduan_data) as flow_11_2
      ,lead(flow,2) over(partition by a.phone_no order by fenduan_data) as flow_11_3
      ,lead(flow,3) over(partition by a.phone_no order by fenduan_data) as flow_11_4
      ,a.times as times_11_1
      ,lead(times,1) over(partition by a.phone_no order by fenduan_data) as times_11_2
      ,lead(times,2) over(partition by a.phone_no order by fenduan_data) as times_11_3
      ,lead(times,3) over(partition by a.phone_no order by fenduan_data) as times_11_4
      ,a.time_duration as time_duration_11_1
      ,lead(time_duration,2) over(partition by a.phone_no order by fenduan_data) as time_duration_11_2
      ,lead(time_duration,3) over(partition by a.phone_no order by fenduan_data) as time_duration_11_3
      ,lead(time_duration,4) over(partition by a.phone_no order by fenduan_data) as time_duration_11_4
      ,row_number() over (partition by phone_no order by fenduan_data ) as fenduan_data
    from 
    (
      select a.phone_no,a.fenduan_data,sum(a.flow) as flow,sum(a.times) as times,sum(a.time_duration) as time_duration
      from 
      (
        select a.phone_no,a.flow,a.times,a.time_duration
          ,case when dd >= '01' and dd <= '07' then 1
          when dd >= '08' and dd <= '14' then 2
          when dd >= '15' and dd <= '21' then 3
          else 4 end as fenduan_data
        from workspace.zb_dm_appflow_all_11 a
      ) a
      group by a.phone_no,a.fenduan_data
    ) a
  ) a
  where fenduan_data = 1
;

--12月
drop table workspace.zb_dm_appflow_all_12_fenduan;
create table workspace.zb_dm_appflow_all_12_fenduan
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  select a.phone_no,a.flow_12_1,a.flow_12_2,a.flow_12_3,a.flow_12_4
    ,a.times_12_1,a.times_12_2,a.times_12_3,a.times_12_4
    ,a.time_duration_12_1,a.time_duration_12_2,a.time_duration_12_3,a.time_duration_12_4
  from
  (
    select a.phone_no
      ,a.flow as flow_12_1
      ,lead(flow,1) over(partition by a.phone_no order by fenduan_data) as flow_12_2
      ,lead(flow,2) over(partition by a.phone_no order by fenduan_data) as flow_12_3
      ,lead(flow,3) over(partition by a.phone_no order by fenduan_data) as flow_12_4
      ,a.times as times_12_1
      ,lead(times,1) over(partition by a.phone_no order by fenduan_data) as times_12_2
      ,lead(times,2) over(partition by a.phone_no order by fenduan_data) as times_12_3
      ,lead(times,3) over(partition by a.phone_no order by fenduan_data) as times_12_4
      ,a.time_duration as time_duration_12_1
      ,lead(time_duration,2) over(partition by a.phone_no order by fenduan_data) as time_duration_12_2
      ,lead(time_duration,3) over(partition by a.phone_no order by fenduan_data) as time_duration_12_3
      ,lead(time_duration,4) over(partition by a.phone_no order by fenduan_data) as time_duration_12_4
      ,row_number() over (partition by phone_no order by fenduan_data ) as fenduan_data
    from 
    (
      select a.phone_no,a.fenduan_data,sum(a.flow) as flow,sum(a.times) as times,sum(a.time_duration) as time_duration
      from 
      (
        select a.phone_no,a.flow,a.times,a.time_duration
          ,case when dd >= '01' and dd <= '07' then 1
          when dd >= '08' and dd <= '14' then 2
          when dd >= '15' and dd <= '21' then 3
          else 4 end as fenduan_data
        from workspace.zb_dm_appflow_all a
      ) a
      group by a.phone_no,a.fenduan_data
    ) a
  ) a
  where fenduan_data = 1
;

--合并两表
drop table workspace.zb_dm_appflow_all_1112_fenduan;
create table workspace.zb_dm_appflow_all_1112_fenduan
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select a.phone_no
    ,case when a.flow_11_1 is not null then a.flow_11_1 else 0 end as flow_11_1
    ,case when a.flow_11_2 is not null then a.flow_11_2 else 0 end as flow_11_2
    ,case when a.flow_11_3 is not null then a.flow_11_3 else 0 end as flow_11_3
    ,case when a.flow_11_4 is not null then a.flow_11_4 else 0 end as flow_11_4
    ,case when a.times_11_1 is not null then a.times_11_1 else 0 end as times_11_1
    ,case when a.times_11_2 is not null then a.times_11_2 else 0 end as times_11_2
    ,case when a.times_11_3 is not null then a.times_11_3 else 0 end as times_11_3
    ,case when a.times_11_4 is not null then a.times_11_4 else 0 end as times_11_4
    ,case when a.time_duration_11_1 is not null then a.time_duration_11_1 else 0 end as time_duration_11_1
    ,case when a.time_duration_11_2 is not null then a.time_duration_11_2 else 0 end as time_duration_11_2
    ,case when a.time_duration_11_3 is not null then a.time_duration_11_3 else 0 end as time_duration_11_3
    ,case when a.time_duration_11_4 is not null then a.time_duration_11_4 else 0 end as time_duration_11_4
    ,case when b.flow_12_1 is not null then b.flow_12_1 else 0 end as flow_12_1
    ,case when b.flow_12_2 is not null then b.flow_12_2 else 0 end as flow_12_2
    ,case when b.flow_12_3 is not null then b.flow_12_3 else 0 end as flow_12_3
    ,case when b.flow_12_4 is not null then b.flow_12_4 else 0 end as flow_12_4
    ,case when b.times_12_1 is not null then b.times_12_1 else 0 end as times_12_1
    ,case when b.times_12_2 is not null then b.times_12_2 else 0 end as times_12_2
    ,case when b.times_12_3 is not null then b.times_12_3 else 0 end as times_12_3
    ,case when b.times_12_4 is not null then b.times_12_4 else 0 end as times_12_4
    ,case when b.time_duration_12_1 is not null then b.time_duration_12_1 else 0 end as time_duration_12_1
    ,case when b.time_duration_12_2 is not null then b.time_duration_12_2 else 0 end as time_duration_12_2
    ,case when b.time_duration_12_3 is not null then b.time_duration_12_3 else 0 end as time_duration_12_3
    ,case when b.time_duration_12_4 is not null then b.time_duration_12_4 else 0 end as time_duration_12_4
from zb_dm_appflow_all_11_fenduan a
inner join zb_dm_appflow_all_12_fenduan b
on a.phone_no = b.phone_no
;
--导出表到文件
insert overwrite local directory '/mnt/disk1/user/lj/results/zb_dm_appflow_all_1112_fenduan' row format delimited fields 
terminated by ',' 
select * from workspace.zb_dm_appflow_all_1112_fenduan where 1=1 
;
--退出hive压缩文件
merge_reduce.sh /mnt/disk1/user/lj/results/zb_dm_appflow_all_1112_fenduan;


select * 
from datamart.data_dw_xdr_gprs_app
where app_type = '视频'
limit 50
;

select phone_no,jp_app,app_name_top1,app_flow_top1,app_name_top2,app_flow_top2,app_name_top3
  ,app_flow_top3,app_name_top4,app_flow_top4,app_name_top5,app_flow_top5
from datamart.data_dm_uv_info_m
where dy = '2019' and dm = '12' and
 (app_name_top1 = '阅读' or app_name_top2 = '阅读' or app_name_top3 = '阅读' or app_name_top4 = '阅读' or app_name_top5 = '阅读')
limit 20;

select * 
from datamart.data_dw_xdr_gprs_app
where app_id = '15-33582';
10-304

1878381pNWP
1828056YkcL
select a.phone_no,a.dd,a.flow,a.times,a.time_duration,b.app_type,b.app_name
from 
(
  select a.phone_no,sum(a.flow) as flow,sum(a.times) as times,sum(a.time_duration) as time_duration,a.dd,a.app_id
  from 
  (
    select *
    from datamart.data_dw_xdr_gprs 
    where phone_no = '1828056YkcL' and dy = '2019' and dm = '12'
  ) a
  group by a.phone_no,a.dd,a.app_id
) a
inner join 
(
  select *
  from datamart.data_dw_xdr_gprs_app
  where app_type = '游戏'
) b
on a.app_id = b.app_id
order by a.dd
limit 30
;

--看周末用户的位置
select *
from business.dwd_user_location_5minute a
where phone_no = '1878381pNWP' and dy = '2019' and dm = '12' and dd = '22'
order by date_time
;

1828056YkcL
1228 1229 726C07F3E0
百度经纬度：104.40961632356 31.105260564306
select *
from business.base_deyang_geocode 
where geo_code = '726C087720'
;
1226
726C0D566A 家
104.41512208563 31.125492969846
726C0D5C5D 公司
104.41752406713 31.129257895526
1221 1222
726C0D566A

1878381pNWP
1222
726C0D560A
104.41237163019 31.124140599382
726C0D54E8
104.41512275921 31.122405768449
726C0D5EC5
104.41614406839 31.136819894023
726C0D5F21
104.4182070963  31.135432099897
726C0D5EC4
104.41579494484 31.136822901147
726C180A33
104.42509727  31.135686171765

