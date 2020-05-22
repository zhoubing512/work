--18年10、11、12月常驻

--dwd_obode_kb_201812
--dwd_obode_kb_201811
--dwd_obode_kb_201810

--跑数口径：
--小时表，晚上 [20点，早上6点] 在德阳的用户，且待的时长大于等于2小时(按天统计待的时长)
--剔除掉18年12月常驻(可一起剔除11，10月的常驻)
--剔除物联网 brand_id wl表示物联网  datamart.data_user_info


--跑19年非德阳常驻 来德阳过夜的（或者晚上8，9点还在德阳）用户
--剔除物联网 brand_id wl表示物联网
--datamart.data_user_info
--zb_dy_2019_visitors_dy_temp1_1

drop table workspace.zb_dy_2019_visitors_dy_temp1_v0;
create table workspace.zb_dy_2019_visitors_dy_temp1_v0 
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as 
    select phone_no,date_time,ci,duration,times,dy,dm,dd,substr(a.date_time,1,10) as data_date,substr(a.date_time,12,2) as data_hour,substr(a.date_time,15,2) as data_minute
    from business.dwd_user_location_hour a
    where dy = '2019' and substr(a.date_time,12,2) in ('20','21','22','23','00','01','02','03','04','05','06')
    ;

drop table workspace.zb_dy_2019_visitors_dy_temp1_v1;
create table workspace.zb_dy_2019_visitors_dy_temp1_v1 
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as 
    select phone_no,data_date,count(distinct data_hour) as stay_hours
    from workspace.zb_dy_2019_visitors_dy_temp1_v0 a
    group by phone_no,data_date
    having count(distinct data_hour) >= 2
    ;

--19年整一起跑，数据量大，很慢   按月进行跑数
drop table workspace.zb_dy_2019_visitors_dy_temp1_1;
create table workspace.zb_dy_2019_visitors_dy_temp1_1 
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as 
    select phone_no,data_date,count(distinct data_hour) as stay_hours
    from 
    (
    select phone_no,date_time,ci,duration,times,dy,dm,dd,substr(a.date_time,1,10) as data_date,substr(a.date_time,12,2) as data_hour,substr(a.date_time,15,2) as data_minute
    from business.dwd_user_location_hour a
    where dy = '2019' and dm = '01' and substr(a.date_time,12,2) in ('20','21','22','23','00','01','02','03','04','05','06')
    ) a
    group by phone_no,data_date
    having count(distinct data_hour) >= 2
    ;


--剔除常驻后的结果表
drop table workspace.zb_dy_2019_visitors_dy;
create table workspace.zb_dy_2019_visitors_dy 
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as 
select distinct a.phone_no,a.data_date,a.stay_hours
from 
(
    select a.phone_no,a.data_date,a.stay_hours 
    from workspace.zb_dy_2019_visitors_dy_temp1_v1 a
    left join 
    (
        select * from datamart.data_user_info where dy = '2019' and dm = '12' and brand_id = 'wl'
    ) b
    on a.phone_no = b.phone_no
    where b.phone_no is null
) a
left join
(
    select phone_no from dwd_obode_kb_201812 where days > 10
    union all
    select phone_no from dwd_obode_kb_201811 where days > 10
    union all
    select phone_no from dwd_obode_kb_201810 where days > 10
) b
on a.phone_no = b.phone_no
where b.phone_no is null
;

--再剔除 10开头 结果
drop table workspace.zb_dy_2019_visitors_dy_temp;
create table workspace.zb_dy_2019_visitors_dy_temp 
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as 
select phone_no,substr(data_date,1,4) as years,substr(data_date,6,2) as months,substr(data_date,9,2) as day_time,stay_hours
from workspace.zb_dy_2019_visitors_dy
where phone_no not like '10%' and phone_no like '1%'
;


+---------+---------------+
| months  | months_times  |
+---------+---------------+
| 07      | 2419842       |
| 05      | 2395423       |
| 04      | 2398800       |
| 11      | 2426531       |
| 08      | 2628660       |
| 01      | 2561908       |
| 09      | 2633290       |
| 12      | 2397796       |
| 02      | 3211705       |
| 03      | 2306224       |
| 10      | 2888583       |
| 06      | 2313793       |
+---------+---------------+

+---------+---------------+
| months  | months_times  |
+---------+---------------+
| 01      | 1502740       |
| 02      | 1843090       |
| 03      | 1367781       |
| 04      | 1342462       |
| 05      | 1388737       |
| 06      | 1303812       |
| 07      | 1372445       |
| 08      | 1447141       |
| 09      | 1511480       |
| 10      | 1660957       |
| 11      | 1444565       |
| 12      | 1445043       |
+---------+---------------+

--按月统计人数
select months,count(distinct phone_no) as months_times
from 
(
    select *
    from workspace.zb_dy_2019_visitors_dy_temp
    where stay_hours > 5
) a
group by months
order by months
;
--按年龄统计人数
select age_fenduan,count(distinct phone_no) as ages_num
from 
(
    select phone_no,
            case when age <= 10 then '0-10'
            when age>10 and age <=20 then '10-20'
            when age>20 and age <=30 then '20-30'
            when age>30 and age <=40 then '30-40'
            when age>40 and age <=50 then '40-50'
            when age>50 and age <=60 then '50-60'
            when age>70 and age <=80 then '70-80'
            else '其他' end as age_fenduan
    from 
    (
        select distinct a.phone_no,b.sex,b.age
        from 
        (
        select phone_no,substr(data_date,1,4) as years,substr(data_date,6,2) as months,substr(data_date,9,2) as day_time, 
        from workspace.zb_dy_2019_visitors_dy
        where phone_no not like '10%' and phone_no like '1%'
        ) a
        left join 
        (
            select phone_no,sex,age 
            from datamart.data_user_baseinfo 
            where dy='2019'
        ) b
        on a.phone_no = b.phone_no
    ) a
) a
group by age_fenduan
;
--按性别统计人数
select sex_new,count(distinct phone_no) as sex_new_num
from 
(
    select phone_no,case when (sex = '' or sex is null) then '' else sex end as sex_new,
    from 
    (
        select distinct a.phone_no,b.sex,b.age
        from 
        (
        select phone_no,substr(data_date,1,4) as years,substr(data_date,6,2) as months,substr(data_date,9,2) as day_time, 
        from workspace.zb_dy_2019_visitors_dy
        where phone_no not like '10%' and phone_no like '1%'
        ) a
        left join 
        (
            select phone_no,sex,age 
            from datamart.data_user_baseinfo 
            where dy='2019'
        ) b
        on a.phone_no = b.phone_no
    ) a
) a
group by sex_new
;


--test
select substr('2019-12-28 00:00:00',12,2) in ('20','21','22','23','00','01','02','03','04','05','06')
select substr('2019-12-28 00:00:00',1,4);
2019
select substr('2019-12-28 00:00:00',6,2);
12
select substr('2019-12-28 00:00:00',9,2);
28

select * from business.dwd_user_location_hour
where phone_no = '1528285qocg' and dy = '2019' and dm = '01' and dd = '05'
;



--林静 2019 外地来访德阳游客 跑数结果表
--workspace.lj_bigdata_visitors_results_2019_20200520

select data_month,count(distinct phone_no)
from 
(
select data_month,data_date,phone_no,count(distinct data_hour) as stay_hours
from 
(
    select data_month,data_date,data_hour,phone_no
    from workspace.lj_bigdata_visitors_results_2019_20200520
    where data_hour in ('22','23','00','01','02','03','04','05')
) a
group by data_month,data_date,phone_no
having count(distinct data_hour) > 5
) a
group by data_month
;
--count(distinct data_hour) > 3
+-------------+---------+
| data_month  |   _c1   |
+-------------+---------+
| 2019-01     | 449764  |
| 2019-09     | 367075  |
| 2019-06     | 322062  |
| 2019-04     | 344840  |
| 2019-05     | 381039  |
| 2019-12     | 315704  |
| 2019-08     | 387055  |
| 2019-10     | 378709  |
| 2019-07     | 343572  |
| 2019-11     | 312870  |
| 2019-02     | 669411  |
| 2019-03     | 387788  |
+-------------+---------+


+-------------+---------+
| data_month  |   _c1   |
+-------------+---------+
| 2019-01     | 428346  |
| 2019-09     | 346246  |
| 2019-06     | 303642  |
| 2019-04     | 327250  |
| 2019-05     | 361357  |
| 2019-12     | 297504  |
| 2019-08     | 366360  |
| 2019-10     | 348222  |
| 2019-07     | 323157  |
| 2019-11     | 295393  |
| 2019-02     | 642635  |
| 2019-03     | 369951  |
+-------------+---------+
--count(distinct data_hour) > 5
+-------------+---------+
| data_month  |   _c1   |
+-------------+---------+
| 2019-01     | 408995  |
| 2019-09     | 325580  |
| 2019-06     | 285097  |
| 2019-04     | 311095  |
| 2019-05     | 342643  |
| 2019-12     | 279798  |
| 2019-08     | 347739  |
| 2019-10     | 321340  |
| 2019-07     | 305434  |
| 2019-11     | 278422  |
| 2019-02     | 617368  |
| 2019-03     | 353291  |
+-------------+---------+
--按年龄统计人数
select age_fenduan,count(distinct phone_no) as ages_num
from 
(
    select phone_no,
            case when age <= 10 then '0-10'
            when age>10 and age <=20 then '10-20'
            when age>20 and age <=30 then '20-30'
            when age>30 and age <=40 then '30-40'
            when age>40 and age <=50 then '40-50'
            when age>50 and age <=60 then '50-60'
            when age>70 and age <=80 then '70-80'
            else '其他' end as age_fenduan
    from 
    (
        select distinct a.phone_no,b.sex,b.age
        from 
        (
            select data_month,data_date,phone_no,count(distinct data_hour) as stay_hours
            from 
            (
                select data_month,data_date,data_hour,phone_no
                from workspace.lj_bigdata_visitors_results_2019_20200520
                where data_hour in ('22','23','00','01','02','03','04','05')
            ) a
            group by data_month,data_date,phone_no
            having count(distinct data_hour) > 4
        ) a
        left join 
        (
            select phone_no,sex,age 
            from datamart.data_user_baseinfo 
            where dy='2019'
        ) b
        on a.phone_no = b.phone_no
    ) a
) a
group by age_fenduan
;
--count(distinct data_hour) > 4
+--------------+-----------+
| age_fenduan  | ages_num  |
+--------------+-----------+
| 50-60        | 96474     |
| 30-40        | 120973    |
| 10-20        | 52022     |
| 20-30        | 119698    |
| 70-80        | 18417     |
| 0-10         | 8812      |
| 其他           | 1408347   |
| 40-50        | 168823    |
+--------------+-----------+


--按性别统计人数
select sex_new,count(distinct phone_no) as sex_new_num
from 
(
    select phone_no,case when (sex = '' or sex is null) then '' else sex end as sex_new
    from 
    (
        select distinct a.phone_no,b.sex,b.age
        from 
        (
            select data_month,data_date,phone_no,count(distinct data_hour) as stay_hours
            from 
            (
                select data_month,data_date,data_hour,phone_no
                from workspace.lj_bigdata_visitors_results_2019_20200520
                where data_hour in ('22','23','00','01','02','03','04','05')
            ) a
            group by data_month,data_date,phone_no
            having count(distinct data_hour) > 4
        ) a
        left join 
        (
            select phone_no,sex,age 
            from datamart.data_user_baseinfo 
            where dy='2019'
        ) b
        on a.phone_no = b.phone_no
    ) a
) a
group by sex_new
;


+----------+--------------+
| sex_new  | sex_new_num  |
+----------+--------------+
| 男        | 329378       |
| 女        | 251093       |
|          | 1362086      |
+----------+--------------+


--1、德阳-成都、绵阳、重庆、资阳、眉山出行和来访情况。
--2、年龄段情况（青年、中年、老年）
--3、所有出行人员的性别情况。


--workspace.zb_dy_2019_to_5place
--workspace.zb_dy_2018_to_5place
--workspace.zb_dy_2017_to_5place

drop table workspace.zb_dy_2017_to_5place;
create table workspace.zb_dy_2017_to_5place 
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as 
  select phone_no,months,mobilecity,count(phone_no) as months_times
  from 
  (
    select distinct a.phone_no,a.data_date,substr(a.data_date,1,4) as years,substr(a.data_date,6,2) as months,substr(a.data_date,9,2) as day_time,
            b.mobilecity
    from 
    (
        select phone_no,data_date,sum(duration) as duration_time
        from 
        (
            select phone_no,date_time,ci,duration,times,dy,dm,dd,substr(a.date_time,1,10) as data_date,substr(a.date_time,12,2) as data_hour,substr(a.date_time,15,2) as data_minute
            from business.dwd_user_location_hour a
            where dy = '2017'   
        ) a
        group by phone_no,data_date
        having sum(duration) >= 60*60*4
    ) a
    inner join 
    (
        select *
        from business.base_mobile_locale 
        where (mobilecity = '成都' or mobilecity = '绵阳' or mobilecity = '重庆' or mobilecity = '资阳' or mobilecity = '眉山') and mobiletype = '中国移动'
    ) b on substr(a.phone_no,1,7) = b.mobilenumber
  ) a
  group by months,mobilecity,phone_no
  having count(phone_no) <= 15
    ;

--按月统计
select mobilecity,months,count(distinct phone_no) as num_19
from workspace.zb_dy_2019_to_5place a
group by mobilecity,months
order by mobilecity,months
;
--age/sex
select mobilecity,sex_new,count(distinct phone_no) as sex_new_num
from
(
    select distinct a.mobilecity,a.phone_no,case when (b.sex = '' or b.sex is null) then '其他' else b.sex end as sex_new
    from workspace.zb_dy_2019_to_5place a
    left join 
    (
        select * 
        from 
        (
            select phone_no,sex,age,row_number() over(partition by phone_no order by dy,dm desc ) rn
            from datamart.data_user_baseinfo 
        ) a where rn = 1
    ) b on a.phone_no = b.phone_no
) a
group by mobilecity,sex_new
order by mobilecity,sex_new
;

--age
select mobilecity,age_fenduan,count(distinct phone_no) as age_fenduan_num
from
(
    select distinct a.mobilecity,a.phone_no,case when (b.sex = '' or b.sex is null) then '其他' else b.sex end as sex_new,
            case when b.age <= 18 then '少年'
          when b.age>18 and b.age <=40 then '青年'
          when b.age>40 and b.age <=60 then '中年'
          when b.age>60 then '老年'
          else '其他' end as age_fenduan
    from workspace.zb_dy_2019_to_5place a
    left join 
    (
        select * 
        from 
        (
            select phone_no,sex,age,row_number() over(partition by phone_no order by dy,dm desc ) rn
            from datamart.data_user_baseinfo 
        ) a where rn = 1
    ) b on a.phone_no = b.phone_no
) a
group by mobilecity,age_fenduan
order by mobilecity,age_fenduan
;

--新需求  到罗江
drop table workspace.zb_dy_2019_to_5place_to_luojiang;
create table workspace.zb_dy_2019_to_5place_to_luojiang 
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as 
  select phone_no,months,mobilecity,count(phone_no) as months_times
  from 
  (
    select distinct a.phone_no,a.data_date,substr(a.data_date,1,4) as years,substr(a.data_date,6,2) as months,substr(a.data_date,9,2) as day_time,
            b.mobilecity
    from 
    (
        select phone_no,data_date,sum(duration) as duration_time
        from 
        (
            select a.phone_no,a.date_time,a.ci,a.duration,a.times,a.dy,a.dm,a.dd,a.data_date,a.data_hour,a.data_minute
            from 
            (
                select phone_no,date_time,ci,duration,times,dy,dm,dd,substr(a.date_time,1,10) as data_date,substr(a.date_time,12,2) as data_hour,substr(a.date_time,15,2) as data_minute
                from business.dwd_user_location_hour a
                where dy = '2019' 
            ) a 
            inner join 
            (
                select cell_id,cell_name,bs_name,county,town
                from business.base_cell_info a
                where day = '20191225' and county = '罗江'
            ) b
            on a.ci = b.cell_id
        ) a
        group by phone_no,data_date
        having sum(duration) >= 60*60*4
    ) a
    inner join 
    (
        select *
        from business.base_mobile_locale 
        where (mobilecity = '成都' or mobilecity = '绵阳' or mobilecity = '德阳') and mobiletype = '中国移动'
    ) b on substr(a.phone_no,1,7) = b.mobilenumber
  ) a
  group by months,mobilecity,phone_no
  having count(phone_no) <= 15
    ;

--test
select cell_id,cell_name,bs_name,county,town
from business.base_cell_info a
where day = '20191225' 
limit 5;

--将德阳分为除罗江外的其他区县

drop table workspace.zb_dy_2019_to_5place_to_luojiang_result;
create table workspace.zb_dy_2019_to_5place_to_luojiang_result 
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as 
    select substr(a.county_name,1,2) as mobilecity,b.months,a.phone_no,b.months_times
    from 
    (
        select phone_no,county_name,area_name,town_name,create_login,create_date,login_no,op_date
        from datamart.data_user_channel
        where dy = '2020' and dm = '04' and county_name <> '罗江分公司'
    ) a
    inner join workspace.zb_dy_2019_to_5place_to_luojiang b
    on a.phone_no = b.phone_no
    union all
    select mobilecity,months,phone_no,months_times
    from workspace.zb_dy_2019_to_5place_to_luojiang a
    where mobilecity <> '德阳'
    ;

--可按之前方式进行统计


--统计  成都 绵阳  旌阳 中江 广汉 绵竹 什邡  到罗江的人数
--按月统计
select mobilecity,months,count(distinct phone_no) as num_19
from workspace.zb_dy_2019_to_5place_to_luojiang_result a
group by mobilecity,months
order by mobilecity,months
;
--按月统计人次
select mobilecity,months,sum(months_times) as months_num_19
from workspace.zb_dy_2019_to_5place_to_luojiang_result a
group by mobilecity,months
order by mobilecity,months
;

--sex
select mobilecity,sex_new,count(distinct phone_no) as sex_new_num
from
(
    select distinct a.mobilecity,a.phone_no,case when (b.sex = '' or b.sex is null) then '其他' else b.sex end as sex_new
    from workspace.zb_dy_2019_to_5place_to_luojiang_result a
    left join 
    (
        select * 
        from 
        (
            select phone_no,sex,age,row_number() over(partition by phone_no order by dy,dm desc ) rn
            from datamart.data_user_baseinfo 
        ) a where rn = 1
    ) b on a.phone_no = b.phone_no
) a
group by mobilecity,sex_new
order by mobilecity,sex_new
;
--sex 人次统计
select mobilecity,sex_new,sum(months_times) as sex_new_num
from
(
    select distinct a.mobilecity,a.phone_no,a.months_times,case when (b.sex = '' or b.sex is null) then '其他' else b.sex end as sex_new
    from workspace.zb_dy_2019_to_5place_to_luojiang_result a
    left join 
    (
        select * 
        from 
        (
            select phone_no,sex,age,row_number() over(partition by phone_no order by dy,dm desc ) rn
            from datamart.data_user_baseinfo 
        ) a where rn = 1
    ) b on a.phone_no = b.phone_no
) a
group by mobilecity,sex_new
order by mobilecity,sex_new
;
--age
select mobilecity,age_fenduan,count(distinct phone_no) as age_fenduan_num
from
(
    select distinct a.mobilecity,a.phone_no,case when (b.sex = '' or b.sex is null) then '其他' else b.sex end as sex_new,
            case when b.age <= 18 then '少年'
          when b.age>18 and b.age <=40 then '青年'
          when b.age>40 and b.age <=60 then '中年'
          when b.age>60 then '老年'
          else '其他' end as age_fenduan
    from workspace.zb_dy_2019_to_5place_to_luojiang_result a
    left join 
    (
        select * 
        from 
        (
            select phone_no,sex,age,row_number() over(partition by phone_no order by dy,dm desc ) rn
            from datamart.data_user_baseinfo 
        ) a where rn = 1
    ) b on a.phone_no = b.phone_no
) a
group by mobilecity,age_fenduan
order by mobilecity,age_fenduan
;

--age 按人次统计
select mobilecity,age_fenduan,sum(months_times) as age_fenduan_num
from
(
    select distinct a.mobilecity,a.phone_no,a.months_times,case when (b.sex = '' or b.sex is null) then '其他' else b.sex end as sex_new,
            case when b.age <= 18 then '少年'
          when b.age>18 and b.age <=40 then '青年'
          when b.age>40 and b.age <=60 then '中年'
          when b.age>60 then '老年'
          else '其他' end as age_fenduan
    from workspace.zb_dy_2019_to_5place_to_luojiang_result a
    left join 
    (
        select * 
        from 
        (
            select phone_no,sex,age,row_number() over(partition by phone_no order by dy,dm desc ) rn
            from datamart.data_user_baseinfo 
        ) a where rn = 1
    ) b on a.phone_no = b.phone_no
) a
group by mobilecity,age_fenduan
order by mobilecity,age_fenduan
;