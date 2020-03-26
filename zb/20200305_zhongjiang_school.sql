drop table workspace.zb_zhongjiang_phoneNo_20200305;
create table workspace.zb_zhongjiang_phoneNo_20200305(school_name string,phone_no string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
load data local inpath '/mnt/disk1/user/zhoubing/temp_files/zj_phone_no_20200305.txt' into table  workspace.zb_zhongjiang_phoneNo_20200305;

近三月月均消费 主资费 流量套餐    近三月月均分钟数    流量使用数   归属分局    归属乡镇    夜间常驻基站  是否有宽带   是否子成员   联系紧密的异网号码
--二月常驻的分局，乡镇，夜间常驻基站
drop table workspace.zb_202002_location_hour;
create table workspace.zb_202002_location_hour
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select m.phone_no,m.geo_code,n.county_name,n.area_name,n.town_name,n.village_name,c.bs_id,c.bs_name
from 
(
select phone_no,geo_code
from 
(
select phone_no,geo_code,row_number() over(partition by phone_no order by sum_time desc) rk
from 
(
    select phone_no,geo_code,count(geo_code) sum_time
    from 
    (
        select phone_no,geo_code
        from
        (
            select phone_no,geo_code,dd,row_number() over(partition by phone_no,geo_code,dd order by sum_time desc) rk
            from 
            (
              select a.phone_no,a.geo_code,dd,sum(a.times) sum_time 
              from business.dwd_user_location_hour a
              where a.dy = '2020' and a.dm = '02' and (substr(a.date_time,12,1) <= 8 or substr(a.date_time,12,1) >= 21)
              group by a.phone_no,a.geo_code,a.dd
            ) a
        ) a
        where rk = 1
    ) a
    group by phone_no,geo_code
) a
) a
where rk = 1
) m
left join business.base_deyang_country_geocode n
on m.geo_code = n.geo_code
left join business.base_geocode_info c
on m.geo_code = c.geo_code
;

--结果表，中江校园数据
drop table workspace.zb_zhongjiang_20200305_result;
create table workspace.zb_zhongjiang_20200305_result
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select distinct a.school_name,a.phone_no,b.avg_3mon_arpu,b.prod_prc_name as main_prc_name
        ,case when e.prod_prc_name like '%流量达量降速服务%' then b.prod_prc_name else e.prod_prc_name end as liuliangbao
        ,b.avg_3mon_voc,b.avg_3mon_dou
        ,d.area_name,d.town_name
        ,b.broad_type
        ,case when b.broad_type = 1 and b.is_main_pay = 0 then 1 else 0 end as shifouzichengyuan
        ,c.opposite_no
from workspace.th_encrypt_zb_zhongjiang_phoneno_20200305 a
left join 
(
    select a.*,b.prod_prc_name
    from
    (
        select * 
        from datamart.data_dm_uv_info_m
        where dy = '2020' and dm = '01'
    ) a
    left join datamart.base_prc_info b
    on a.main_prod_prcid = b.prod_prcid
) b
on a.phone_no = b.phone_no
left join
(
    select a.phone_no,a.opposite_no
    from 
    (
        select a.phone_no,a.opposite_no,a.call_time,row_number() over(partition by a.phone_no order by a.call_time desc ) rn
        from
        (
            select a.phone_no,a.opposite_no,sum(a.call_duration) as call_time
            from
            (
                select a.*
                from
                (
                    select *
                    from datamart.data_dwb_cal_user_voc_yx_ds
                    where dy = '2020' and (dm = '02' or dm = '03')
                ) a
                inner join business.base_mobile_locale b
                on substr(a.opposite_no,1,7) = b.mobilenumber
                where b.mobiletype not like '%中国移动%'
            ) a
            group by a.phone_no,a.opposite_no
        ) a
    ) a
    where rn = 1
) c
on a.phone_no = c.phone_no
left join workspace.zb_202002_location_hour d
on a.phone_no = d.phone_no
left join
(
    select a.phone_no,b.prod_prc_name,a.op_time,a.login_no,a.eff_time,a.exp_time
    from 
    (
        select phone_no,prod_prcid,login_no,op_time,eff_time,exp_time
        from datamart.data_user_prc
        where data_eff_flag = 1 
    ) a
    left join datamart.base_prc_info b
    on a.prod_prcid = b.prod_prcid
    where b.prod_prc_name like '%流量%' and b.prod_prc_name not like '%流量%安心%' and b.prod_prc_type = 1
) e
on a.phone_no = e.phone_no
;


select distinct prod_prcid,prod_prc_name,prod_prc_desc,prod_prc_type
from datamart.base_prc_info
where prod_prc_name like '%流量%安心%'
limit 50;

select count(*)
from 
(
select phone_no,count(phone_no)
from workspace.zb_zhongjiang_20200305_result
group by phone_no
having count(phone_no) > 1
) a
;

1528289zNvQ 2
1830840zscZ 3
1828422THdA 2
1577556awdt 2
1354700GJjA 2
1519636zkjt 3
1355065pHvQ 3
1828050psjA 2
1872800YnMr 2
1588386poSg 2

select * 
from workspace.zb_zhongjiang_20200305_result
where phone_no = '1528289zNvQ'
;

select distinct *
from workspace.zb_202002_location_hour
where phone_no = '1589248RUMA'
;
select count(*) 
from workspace.zb_zhongjiang_20200305_result
where area_name is null
limit 5;