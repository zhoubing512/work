--匹配用户号码
drop table workspace.zb_jingyang_phone_no_20200330;
create table workspace.zb_jingyang_phone_no_20200330(phone_no string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
putdata -f 20200330_jingyang_phoneno.txt -t workspace.zb_jingyang_phone_no_20200330;

--用户近3个月动态基站	用户近3个月基站归属区域	用户近3个月基站归属区县	用户最后一次办理业务的营业厅名称

--加密后的表
th_en_zb_jingyang_phone_no_20200330
--近三月常驻动态基站、归属分局、区县
--5分钟表
drop table workspace.zb_jingyang_user_20200330_changzhu_minute;
create table workspace.zb_jingyang_user_20200330_changzhu_minute
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select a.phone_no,b.county_name,b.area_name,b.town_name,b.village_name
from 
(
select distinct phone_no,geo_code
from 
(
    select phone_no,count(day_time) as day_sum,geo_code
    from 
    (
        select distinct phone_no,day_time,day_time
        from 
        (
            select phone_no,count(date_time) as date_time_sum,geo_code,day_time
            from 
            (
                select phone_no,date_time,cell_id,geo_code,concat(dy,'-',dm,'-',dd) as day_time
                from business.dwd_user_location_5minute
                where dy = '2020' 
            ) a
            group by phone_no,geo_code,day_time
            having count(date_time) >= 48
        ) a
    )  a
    group by phone_no,cell_id
) a 
where a.day_sum > 5
) a
left join 
( 
    select * 
    from business.base_deyang_country_geocode 
) b on a.geo_code = b.geo_code
;
--天表
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

+--------------+
|   phone_no   |
+--------------+
| 1345849GJXQ  |
| 1519636qIMt  |
| 1365816lHWy  |
| 1518106GkcL  |
| 1528146lscy  |
| 1598383YnSQ  |
| 1528383qNbt  |
| 1588364Fsjt  |
| 1519639Rnvy  |
| 1399023THdA  |
+--------------+
| 1368962asWZ                                  | 6775A71425                                   | NULL                                            | NULL                                          | NULL                                          | NULL                                             |
| 1570838Rojt                                  | 6775A71425                                   | NULL                                            | NULL                                          | NULL                                          | NULL                                             |
| 1872805GHcy                                  | 6775A71425                                   | NULL                                            | NULL                                          | NULL                                          | NULL                                             |
| 1500148TwXP                                  | 67B4024629                                   | NULL                                            | NULL                                          | NULL                                          | NULL                                             |
| 1522848zocZ                                  | 67B4024629

select phone_no, geo_code ,duration
from business.dwd_user_location_day
where dy = '2020' and phone_no = '1345849GJXQ'
;
select county_name,area_name,town_name,village_name,geo_code
from business.base_deyang_country_geocode
where geo_code = '7266A20257'
;



drop table workspace.zb_jingyang_user_20200330_result;
create table workspace.zb_jingyang_user_20200330_result
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select distinct a.phone_no,b.geo_code,b.county_name,b.area_name,c.chl_name
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
;
7266A20257
select *
from business.base_msc_info
where geo_code = '7266A20257';


select distinct op_code,op_name from data_user_channel 
where op_name like '%缴费%' and dy = '2020' and dm = '02'
limit 20;

+----------+----------+
| op_code  | op_name  |
+----------+----------+
| 8003     | 缴费冲正     |
| 8000     | 普通缴费     |
| 8038     | 批量缴费     |
| 8002     | 缴费(帐号)   |
+----------+----------+
