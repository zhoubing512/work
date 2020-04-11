
--导入数据
drop table workspace.zb_wangluo_phone_info;
create table workspace.zb_wangluo_phone_info(quxian string,phone_no string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
putdata -f 20200410_wangluobu_manyidu.txt -t workspace.zb_wangluo_phone_info;

--二月常驻小区1
workspace.dwd_obode_kb_202002
--二月常驻小区2
drop table workspace.zb_wangluo_user_20200410_changzhu_2;
create table workspace.zb_wangluo_user_20200410_changzhu_2
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select a.phone_no,a.ci1,b.cell_name
from 
(
    select distinct phone_no,ci1
    from 
    (
        select phone_no,ci1,row_number() over(partition by phone_no order by day_sum desc) as rn
        from 
        (
            select phone_no,count(dd) as day_sum,ci1
            from 
            (
                select phone_no,dd,ci1
                from business.dwd_user_location_day
                where dy = '2020' and dm = '02'
            ) a
            group by phone_no,ci1
        ) a
    ) a
    where rn = 1 
) a
left join 
( 
    select * 
    from business.base_cell_info 
) b on a.ci1 = b.cell_id
;

--区县	客户号码	常驻小区1	常驻小区2	ARPU值	资费套餐	2月是否超套限速
main_prod_prcid,main_prc_fee ,arpu,over_gprs_fee
select count(*) from workspace.zb_wangluo_phone_info limit 5;


--5分钟表跑常驻
drop table workspace.zb_wangluo_user_20200410_changzhu_minute_tmp1;
create table workspace.zb_wangluo_user_20200410_changzhu_minute_tmp1
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  select a.phone_no,b.cnt,b.cell_id 
  from workspace.th_en_zb_wangluo_phone_info a
  left join 
  (
    select distinct phone_no,cnt,cell_id
    from business.dwd_user_location_5minute
    where dy = '2020' and dm = '02'
    ) b on a.phone_no = b.phone_no
;

drop table workspace.zb_wangluo_user_20200410_changzhu_minute;
create table workspace.zb_wangluo_user_20200410_changzhu_minute
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select a.phone_no,a.cell_id,a.rn,b.cell_name
from 
(
    select distinct phone_no,cell_id,rn
    from 
    (
        select phone_no,cell_id,row_number() over(partition by phone_no order by day_sum desc) as rn
        from 
        (
            select phone_no,sum(cnt) as day_sum,cell_id
            from 
            (
                select distinct phone_no,cnt,cell_id
                from business.dwd_user_location_5minute
                where dy = '2020' 
            ) a
            group by phone_no,cell_id
        ) a
    ) a
    where (rn = 1 or rn = 2) 
) a
left join 
( 
    select * 
    from business.base_cell_info 
) b on a.cell_id = b.cell_id
;

--匹配结果
drop table workspace.zb_wangluo_user_20200410_result;
create table workspace.zb_wangluo_user_20200410_result
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select a.quxian,a.phone_no,b.cell_id,b.cell_name,b1.cell_id as cell_id2,b1.cell_name as cell_name2
        ,c.prod_prc_name,c.main_prc_fee ,c.arpu,c.over_gprs_fee
from workspace.th_en_zb_wangluo_phone_info a
left join  
(
    select *
    from workspace.zb_wangluo_user_20200410_changzhu_minute
    where rn = 1
) b
on a.phone_no = b.phone_no
left join 
(
    select *
    from workspace.zb_wangluo_user_20200410_changzhu_minute
    where rn = 2
) b1
on a.phone_no = b1.phone_no
left join
(
    select a.*,b.prod_prc_name
    from 
    (
    select *
    from datamart.data_dm_uv_info_m
    where dy = '2020' and dm = '02'
    ) a
    left join datamart.base_prc_info b
    on a.main_prod_prcid = b.prod_prcid
) c on a.phone_no = c.phone_no
;

--去重后临时表
drop table workspace.zb_wangluo_user_20200410_result_tmp;
create table workspace.zb_wangluo_user_20200410_result_tmp
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select distinct quxian,phone_no,cell_id,cell_id2,prod_prc_name,main_prc_fee ,arpu,over_gprs_fee
from workspace.zb_wangluo_user_20200410_result
;

select count(*) from zb_wangluo_user_20200410_result_tmp where prod_prc_name is null;
--检验
select *
from 
(
select distinct phone_no,quxian,ci1
from workspace.zb_wangluo_user_20200410_result
) a
where ci1 is null limit 5
;

left join (
    select *
    from 
)
select *
from datamart.data_dm_uv_info_m
where dy = '2020' and dm = '02'