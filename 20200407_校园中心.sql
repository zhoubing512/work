--需要匹配的用户号码
drop table workspace.zb_xiaoyuan_phone_no_20200407;
create table workspace.zb_xiaoyuan_phone_no_20200407(phone_no string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
putdata -f 20200407_xiaoyuan.txt -t workspace.zb_xiaoyuan_phone_no_20200407;
--加密表  th_en_zb_xiaoyuan_phone_no_20200407




--近5个月常驻
zb_xiaoyuan_user_20200407_changzhu
--19年到20年全年
zb_xiaoyuan_user_20200407_changzhu_19
drop table workspace.zb_xiaoyuan_user_20200407_changzhu_19;
create table workspace.zb_xiaoyuan_user_20200407_changzhu_19
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
                where dy = '2019'or dy = '2020'
            ) a
            group by phone_no,geo_code,day_time
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
--5个月 zb_xiaoyuan_user_20200407_result

drop table workspace.zb_xiaoyuan_user_20200407_result_19;
create table workspace.zb_xiaoyuan_user_20200407_result_19
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select distinct a.phone_no,b.county_name,b.area_name
from workspace.th_en_zb_xiaoyuan_phone_no_20200407 a
left join workspace.zb_xiaoyuan_user_20200407_changzhu_19 b
on a.phone_no = b.phone_no
;

select count(*)
from 
(
select distinct a.phone_no,b.county_name
from workspace.th_en_zb_xiaoyuan_phone_no_20200407 a
left join workspace.zb_xiaoyuan_user_20200407_changzhu b
on a.phone_no=b.phone_no
where b.phone_no is not null
) a
limit 5;
+--------------+----------------+
|  a.phone_no  | b.county_name  |
+--------------+----------------+
| 1340838FkbA  | NULL           |
| 1340838RUSg  | NULL           |
| 1340838lHbA  | 广汉             |
| 1340838lIbA  | 什邡             |
| 1340838psSA  | 罗江             |
| 1340838qHSQ  | NULL           |
| 1340838zkSt  | NULL           |
| 1340838znSA  | 中江             |
| 1345896YNjZ  | NULL           |
| 1345896pHMr  | 中江             |
| 1345896pnXr  | 中江             |
| 1350802FUMr  | 绵竹             |
| 1351826THjZ  | NULL           |
| 1351826Tsjt  | NULL           |
| 1354700RJEu  | 中江             |
| 1354701qUMg  | 绵竹             |
| 1354702anbr  | NULL           |
| 1354704RHvu  | 罗江             |
| 1354708qsSZ  | NULL           |
| 1354709TnMZ  | 旌阳             |
+--------------+----------------+


select *
from business.dwd_user_location_5minute
where (dy = '2020' or (dy = '2019' and (dm ='11' or dm = '12'))) and phone_no = '1340838zkSt'
limit 200
;