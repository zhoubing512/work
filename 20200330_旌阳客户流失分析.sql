
--需要匹配的用户号码
drop table workspace.zb_jingyang_phone_no_20200330;
create table workspace.zb_jingyang_phone_no_20200330(phone_no string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
putdata -f 20200330_jingyang_phoneno.txt -t workspace.zb_jingyang_phone_no_20200330;

--字段：用户近3个月动态基站	用户近3个月基站归属区域	用户近3个月基站归属区县	用户最后一次办理业务的营业厅名称

--近三月常驻动态基站、归属分局、区县
--天表(day表)跑数，近三月动态常驻归属地
drop table workspace.zb_jingyang_user_20200330_changzhu;
create table workspace.zb_jingyang_user_20200330_changzhu
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select a.phone_no,a.geo_code,b.county_name,b.area_name,b.town_name,b.village_name
from 
(
    select distinct phone_no,geo_code
    from 
    (
        select phone_no,geo_code,row_number() over(partition by phone_no order by day_sum desc) as rn
        from 
        (
            select phone_no,count(day_time) as day_sum,geo_code
            from 
            (
                select phone_no,dd,geo_code,time_type,concat(dy,'-',dm,'-',dd) as day_time
                from business.dwd_user_location_day
                where dy = '2020' 
            ) a
            group by phone_no,geo_code,day_time
            having count(day_time) > 5
        ) a
    ) a
    where rn = 1
) a
left join 
( 
    select * 
    from business.base_deyang_country_geocode 
) b on a.geo_code = b.geo_code
;

--匹配结果
drop table workspace.zb_jingyang_user_20200330_result;
create table workspace.zb_jingyang_user_20200330_result
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select distinct a.phone_no,b.geo_code,b.county_name,b.area_name,c.chl_name,d.chl_name as chl_name_all_last
from workspace.th_en_zb_jingyang_phone_no_20200330 a
left join workspace.zb_jingyang_user_20200330_changzhu b
on a.phone_no = b.phone_no
left join
(
    select *
    from 
    (
        select phone_no,chl_name,row_number() over(partition by phone_no order by op_date desc) as rn
        from 
        (
        select *
        from datamart.data_user_channel
        where dy='2020' and op_code in ('8003','8000','8038','8002')
        ) a
    ) a
    where a.rn = 1
) c
on a.phone_no = c.phone_no
left join
(
    select *
    from 
    (
        select phone_no,chl_name,row_number() over(partition by phone_no order by op_date desc) as rn
        from 
        (
        select *
        from datamart.data_user_channel
        where dy='2020'
        ) a
    ) a
    where a.rn = 1
) d
on a.phone_no = d.phone_no
;

--检查各个号码
--96个号码无归属 近三月归属区县、分局； 常驻地方为外地，不在德阳
--726645B57D
--730AB0DF1A
--724AE737D8
--7266A20257
--7267D992B5
select *
from business.base_msc_info
where geo_code = '726645B57D';



