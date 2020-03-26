--中江市场用户安装app情况1226
--主资费、腾讯、优酷、爱奇艺、近三月用户消费、流量使用量   筛选未办理AZ330287活动且安装有腾讯、优酷、爱奇艺app的用户

腾讯视频 05-0006 5-6、优酷 05-0003、爱奇艺PPS 12-0007 /爱奇艺视频 05-0017
select app_id ,app_name
from datamart.data_dw_xdr_gprs_app
where app_name like '%爱奇艺%' or app_name like '%iQIYI%';
ACAZ36282   视频会员任选包
ACAZ40312   视频任选包（周卡）-专属享
--ACAZ40250   视频会员任选包-促销
ACAZ40311   视频任选包（月卡）-专属享
ACAZ40448   合伙人版-视频会员任选包（含10个G定向流量）

select app_id ,app_name
from datamart.data_dw_xdr_gprs_app
where app_id = '05-0006' or app_id = '5-6' or app_id = '05-0003' or app_id = '12-0007' or app_id = '05-0017'
;

ACAZ33547、ACAZ33548、ACAZ33549、ACAZ40371、ACAZ40372、ACAZ40373
select * 
from datamart.base_prc_info
where prod_prc_name like '%视频%任选%' limit 5;

select * 
from datamart.base_prc_info
where prod_prcid = 'ACAZ33547' limit 5;


select * 
from datamart.data_user_act
where means_id = 'AZ330287';

--中间表1
drop table workspace.app_1216_zhongjian1;
create table workspace.app_1216_zhongjian1
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  select a.phone_no,a.app_id,count(a.app_id) as app_times
  from 
  (
        select a.phone_no,a.app_id
        from 
        (
            select a.phone_no,a.app_id
            from datamart.data_dw_xdr_gprs a
            where dy = '2019' and dm = '12'
            and (app_id = '05-0006' or app_id = '5-6' or app_id = '05-0003' or app_id = '12-0007' 
                or app_id = '05-0017') 
            and a.phone_no in (
                                select distinct phone_no
                                from datamart.data_dm_uv_info_m 
                                where county_id = '115' and dy = '2019' and dm  in ('11','10')
                            )
        ) a 
  ) a group by a.phone_no,a.app_id
;
--中间表2
drop table workspace.app_1216_zhongjian2;
create table workspace.app_1216_zhongjian2
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  select a.phone_no
        ,sum(case when app_my_type=1 then app_times ELSE 0 END) as tencent
        ,sum(case when app_my_type=2 then app_times ELSE 0 END) as youku
        ,sum(case when app_my_type=3 then app_times ELSE 0 END) as aiqiyi
  from
    (
            select a.phone_no,a.app_times
                ,case 
                when a.app_name like '%腾讯视频%' then 1 
                when a.app_name like '%优酷%' then 2
                when a.app_name like '%爱奇艺%' then 3
                end as app_my_type
            from
            (
                select a.phone_no,b.app_name,a.app_times 
                from workspace.app_1216_zhongjian1 a 
                left join 
                ( 
                    select app_id,app_name
                    from datamart.data_dw_xdr_gprs_app
                    where app_id = '05-0006' or app_id = '5-6' or app_id = '05-0003' or app_id = '12-0007' 
                            or app_id = '05-0017' 
                ) b on a.app_id = b.app_id
            ) a
    ) a
    group by a.phone_no
;



select count(distinct phone_no) ,count(phone_no)
from app_1216_zhongjian2
where tencent > 0 or youku > 0 or aiqiyi > 0; 
--228003
select count(distinct phone_no) ,count(phone_no)
from app_1216_zhongjian2
;
--结果
drop table workspace.app_1216_zhongjian_result;
create table workspace.app_1216_zhongjian_result
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select distinct a.phone_no,b.prod_prc_name_main,b.prod_prc_price_main,a.tencent,a.youku,a.aiqiyi
        ,c.avg_3mon_arpu,c.avg_3mon_dou
from 
(
    select a.phone_no,a.tencent,a.youku,a.aiqiyi  
    from workspace.app_1216_zhongjian2 a
    left join
    (
        select phone_no
        from datamart.data_user_prc
        where prod_prcid = 'ACAZ40250'
    ) b
    on a.phone_no = b.phone_no
    where b.phone_no is null
) a
left join datamart.dw_user_prc_main_latest b
on a.phone_no = b.phone_no
left join 
(
    select * 
    from datamart.data_dm_uv_info_m
    where dy = '2019' and dm = '11'
) c on a.phone_no = c.phone_no
;


--检查
select count(distinct phone_no) ,count(phone_no)
from app_1216_zhongjian_result
where tencent > 0 or youku > 0 or aiqiyi > 0; 
--227704
select count(distinct phone_no) ,count(phone_no)
from app_1216_zhongjian_result
;



select a.* 
from workspace.app_1216_zhongjian_result a
inner join
(
select *
from app_1216_zhongjian_result
where prod_prc_name_main is null
) b on a.phone_no = b.phone_no
;



select count(distinct phone_no),count(phone_no)
from workspace.app_1216_zhongjian_result
;

select phone_no,count(phone_no)
from workspace.app_1216_zhongjian_result
group by phone_no
having count(phone_no) > 1
;

select * 
from workspace.app_1216_zhongjian_result
where phone_no = '1828386YHXP';

select * from workspace.app_1216_zhongjian_result;

select count(phone_no)
from datamart.data_user_prc
where prod_prcid = 'ACAZ40250'






drop table workspace.lj_scb_lj_app_20190807_tmp;
create table workspace.lj_scb_lj_app_20190807_tmp
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select phone_no
,sum(case when app_my_type='dytt' then flow_app else 0 end) as dytt
,sum(case when app_my_type='yk' then flow_app else 0 end ) as yk
,sum(case when app_my_type='mgsp' then flow_app else 0 end) as mgsp
,sum(case when app_my_type='wyyyy' then flow_app else 0 end) as wyyyy
,sum(case when app_my_type='mgtv' then flow_app else 0 end ) as mgtv
,sum(case when app_my_type='bdaqy' then flow_app else 0 end ) as bdaqy
,sum(case when app_my_type='txsp' then flow_app else 0 end ) as txsp
,sum(case when app_my_type='al' then flow_app else 0 end ) as al
,sum(case when app_my_type='xmlyt' then flow_app else 0 end) as xmlyt
from (
select phone_no,app_my_type,round((sum(flow)/1024),0) as flow_app
       from (
select phone_no,flow,b.app_name
,case when app_name in ('今日头条','抖音','抖音短视频') then 'dytt'
     when app_name in ('优酷') then 'yk'
     when app_name in ('咪咕视频') then 'mgsp'
     when app_name in ('网易云音乐') then 'wyyyy'
     when app_name in ('芒果TV') then 'mgtv'
     when app_name in ('百度视频','爱奇艺') then 'bdaqy'
     when app_name in ('腾讯视频') then 'txsp'
     when app_name in ('天猫','淘宝','支付宝') then 'al'
     when app_name in ('喜马拉雅听') then 'xmlyt'
end as app_my_type
 from (
select phone_no
       ,flow
       ,times
       ,time_duration
       ,app_id
    from  datamart.data_dw_xdr_gprs
where dy='2019' and dm='06') as a
left join (
select app_id,app_name
    from datamart.data_dw_xdr_gprs_app 
    where app_name in  ('今日头条','抖音','抖音短视频','咪咕视频','优酷','网易云音乐'
                       ,'芒果TV','百度视频','爱奇艺','腾讯视频','天猫','淘宝','支付宝','喜马拉雅听')
) as b 
    on a.app_id=b.app_id
    where b.app_id is not null) as c
    group by phone_no,app_my_type
    ) as d
    group by phone_no
    ;

















