--罗江，电视包外呼，三大视频会员营销

提取字段，移动手机号使用过（优酷，爱奇艺，腾讯）app记录的；
是否有宽带，是否关键人，宽带类型（保底版或共享版）；
有宽带的关键人，是否订购过电视包（酷喵影视）；
有保底版宽带个人消费是否可升舱；
手机号使用终端类型；
select distinct a.app_id,a.app_name
from datamart.data_dw_xdr_gprs_app a
where a.app_name like '%腾讯视频%'
or a.app_name like '%优酷%' 
or a.app_name like '%爱奇艺%' 
;
05-0006 腾讯视频
05-0003 优酷
05-0017 爱奇艺视频
5-3 优酷网
5-6 腾讯视频

--中间表1
drop table workspace.luojiang_20200103_zhongjian1;
create table workspace.luojiang_20200103_zhongjian1
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  select a.phone_no
    ,case when sum(tx) > 0 then 1 else 0 end as tx
    ,case when sum(youku) > 0 then 1 else 0 end as youku
    ,case when sum(aiqiyi) > 0 then 1 else 0 end as aiqiyi
  from 
  (
      select distinct a.phone_no
        ,case when a.app_id = '05-0006' or a.app_id = '5-6' then 1 else 0 end as tx
        ,case when a.app_id = '05-0003' or a.app_id = '5-3' then 1 else 0 end as youku
        ,case when a.app_id = '05-0017' then 1 else 0 end as aiqiyi
      from 
      (
        select distinct a.phone_no,a.app_id
        from datamart.data_dw_xdr_gprs a
        where dy = '2019' and dm = '12'
        and (app_id = '05-0006' or app_id = '5-6' or app_id = '05-0003' or app_id = '5-3' 
            or app_id = '05-0017')
      ) a
  ) a
  group by a.phone_no
;

select count(distinct phone_no)
from datamart.data_dm_uv_info_m
where dy = '2019' and dm in ('11','10','09') and county_id = '58';
broad_prod_id       
broad_prod_name     
broad_prod_fee      

drop table workspace.luojiang_20200103_result;
create table workspace.luojiang_20200103_result
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select distinct a.phone_no,a.tx,a.youku,a.aiqiyi,a.shifouyoukuandai,a.shifou_key_phone,a.kumiao,a.term_factory,a.term_model,a.term_type,a.arpu,a.avg_3mon_arpu,broad_prod_id,broad_prod_name,broad_prod_fee
from
(
    select a.phone_no,a.tx,a.youku,a.aiqiyi
        ,case when c.phone_no is not null then 1 else 0 end as shifouyoukuandai
        ,case when c.phone_no is not null and c.phone_no = c.key_phone_no then 1 else 0 end as shifou_key_phone
        ,case when c.phone_no is not null and c.phone_no = c.key_phone_no then kumiao else 0 end as kumiao
        ,c.key_phone_no
        ,b.term_factory,b.term_model,b.term_type,b.arpu,b.avg_3mon_arpu,b.broad_prod_id,b.broad_prod_name
        ,b.broad_prod_fee
    from 
    (
        select * 
        from workspace.luojiang_20200103_zhongjian1 a
        where a.phone_no in (
            select distinct phone_no
            from datamart.data_dm_uv_info_m
            where dy = '2019' and dm = '11' and county_id = '58'
            )
    ) a
    left join
    (
        select distinct phone_no,term_factory,term_model,term_type,arpu,avg_3mon_arpu,broad_prod_id,broad_prod_name
        ,broad_prod_fee
        from datamart.data_dm_uv_info_m
        where dy = '2019' and dm = '11' and county_id = '58'
    ) b
    on a.phone_no = b.phone_no
    left join 
    (
        select a.*,case when b.phone_no is not null then 1 else 0 end as kumiao
        from 
        (
            select *
            from datamart.dw_user_kd
            where dy = '2019' and dm = '11'
        ) a
        left join
        (
            select distinct phone_no 
            from datamart.data_user_prc
            where prod_prcid in ('ACAZ34479','ACAZ34478','ACAZ34480')
        ) b
        on a.phone_no = b.phone_no
    ) c 
    on a.phone_no = c.phone_no
) a
;


select *
from datamart.dw_user_kd
limit 50
;
select term_factory,term_model,term_type
from datamart.data_dm_uv_info_m
where dy = '2019' and dm = '11' and county_id = '58'
limit 5;

酷喵影视
select distinct prod_prcid,prod_prc_name,prod_prc_desc
from datamart.base_prc_info
where prod_prc_name like '%酷喵%' limit 5;

ACAZ34479   酷喵影视季包  四川移动电视酷喵影视季包业务，100元/季
ACAZ34478   酷喵影视    酷喵影视专区业务，产品价格29元/月，产品包含酷喵影视专区收视权益
ACAS51chcmd 魔百和-酷喵少儿包月22元-福建    魔百和-酷喵少儿包月22元-福建
ACAS51chcme 魔百和-酷喵影视包月26元-福建    魔百和-酷喵影视包月26元-福建
ACAZ34480   酷喵影视年包  四川移动电视酷喵影视年包业务，399元/年

select distinct phone_no 
from datamart.data_user_prc
where prod_prcid in ('ACAZ34479','ACAZ34478','ACAZ34480')

select sum(kumiao)
from 
(
    select case when b.phone_no is not null then 1 else 0 end as kumiao
    from 
    (
        select *
        from datamart.dw_user_kd
        where dy = '2019' and dm = '11'
    ) a
    left join
    (
        select distinct phone_no 
        from datamart.data_user_prc a
        where (a.prod_prcid = 'ACAZ34479' or a.prod_prcid = 'ACAZ34478' or a.prod_prcid = 'ACAZ34480')
        and a.phone_no in (
            select distinct phone_no
            from datamart.data_dm_uv_info_m
            where dy = '2019' and dm = '11' and county_id = '58'
            )
    ) b
    on a.phone_no = b.phone_no
) a
;
3031 key_phone_no
23057 phone_no
