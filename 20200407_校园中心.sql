--需要匹配的用户号码
drop table workspace.zb_xiaoyuan_phone_no_20200407;
create table workspace.zb_xiaoyuan_phone_no_20200407(phone_no string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
putdata -f 20200407_xiaoyuan.txt -t workspace.zb_xiaoyuan_phone_no_20200407;
--加密表  th_en_zb_xiaoyuan_phone_no_20200407




--201911月常驻
drop table workspace.zb_xiaoyuan_user_20200407_changzhu;
create table workspace.zb_xiaoyuan_user_20200407_changzhu
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
                where dy = '2019'  and dm = '11'
            ) a
            group by phone_no,geo_code,day_time
            having count(day_time) > 2
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

--匹配归属分公司
drop table workspace.zb_xiaoyuan_user_20200407_result;
create table workspace.zb_xiaoyuan_user_20200407_result
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select distinct a.phone_no,b.county_name
from workspace.th_en_zb_xiaoyuan_phone_no_20200407 a
left join
(
    select *
    from datamart.data_user_channel 
    where dy='2020' and dm='03'
) b
on a.phone_no = b.phone_no
;