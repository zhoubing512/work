
--更多到访代码在 '2020520_集客.sql' 

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