--2019年四川装备智造国际博览  2019年10月22-25日
--2017、2019年四川国际航展  2017年9月29日至10月3日  2019年9月29日—10月3日

--导入数据
drop table workspace.zb_deyang_exhibition_bs_info_20200506;
create table workspace.zb_deyang_exhibition_bs_info_20200506(bs_name string,type_out_in string,bs_type string,lac_id string,cell_id string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
putdata -f deyang_international_exhibition.txt -t workspace.zb_deyang_exhibition_bs_info_20200506;

select bs_name,type_out_in,bs_type,lac_id,cell_id
from workspace.zb_deyang_exhibition_bs_info_20200506
;

--2017年9月常驻用户, 2019年9月常驻用户
--2017  zb_dy_201709_cz
--2019  zb_dy_201909_cz
drop table workspace.zb_dy_201909_cz;
create table workspace.zb_dy_201909_cz 
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as 
  select *from (
    select dd.phone_no,dd.ci,dd.ci_num,row_number() over (partition by phone_no order by ci_num desc ) num  from (
      select bb.phone_no,bb.ci,count(ci) ci_num from (
        select * from (
          select phone_no,ci,time_type,duration,dd,row_number() over (partition by phone_no,dd order by duration desc ) num  
          from business.dwd_user_location_day  where dy='2019' and dm='09'
          ) cc
        where cc.num = 1
        ) bb
      group by phone_no,ci
      ) dd
    ) as ff
  where ff.num=1 and ff.phone_no not like '10%' and ff.phone_no like '1%' 
  ;


--2017 航展 筛选在这些基站待了2小时以上的用户
--zb_exhibition_visitors_info_201709
--2019 航展 筛选在这些基站待了2小时以上的用户
--zb_exhibition_visitors_info_201909
--2019 四川装备智造国际博览 筛选在这些基站待了2小时以上的用户
--zb_exhibition_visitors_info_201910
drop table workspace.zb_exhibition_visitors_info_201909;
create table workspace.zb_exhibition_visitors_info_201909 as
select distinct a.cell_id,a.phone_no,substr(a.date_time,1,10) as data_date,substr(a.date_time,12,2) as data_hour,substr(a.date_time,15,2) as data_minute
            from
                (
                  select * from business.dwd_user_location_5minute 
                  where dy='2019' and ((dm='09' and (dd = '29' or dd = '30')) or (dm='10' and (dd = '01' or dd = '02' or dd = '03')))
                  --where dy='2019' and dm='10' and (dd = '22' or dd = '23' or dd = '24' or dd = '25')
                )a 
                left join (
                            select b.cell_id from  workspace.zb_deyang_exhibition_bs_info_20200506 a
                            left join 
                            (
                              select * from business.base_cell_info where day = '20191010'
                            ) b
                            on a.cell_id = b.bs_id
                        ) d on a.cell_id = d.cell_id
                where d.cell_id is not null;

--17年9月  zb_exhibition_visitors_info_results_1709_20200504
--19年9月  zb_exhibition_visitors_info_results_1909_20200504
--19年10月 zb_exhibition_visitors_info_results_1910_20200504


drop table workspace.zb_exhibition_visitors_info_results_1910_20200504;
create table workspace.zb_exhibition_visitors_info_results_1910_20200504 as
select distinct a.phone_no,a.cell_id,b.stay_hours,c.sex,c.age,g.mobileprovince,g.mobilecity,g.areacode
from workspace.zb_exhibition_visitors_info_201910 a
left join(
  select phone_no,data_date,count(distinct data_hour) stay_hours
  from workspace.zb_exhibition_visitors_info_201910
  group by phone_no,data_date
) b on a.phone_no = b.phone_no
left join (
  select phone_no,sex,age 
  from datamart.data_user_baseinfo 
  where dy='2019' and dm='10'
) c on a.phone_no = c.phone_no
left join business.base_mobile_locale g on substr(a.phone_no,1,7) = g.mobilenumber
left join(
  select * 
  from workspace.zb_dy_201909_cz 
  where ci_num >= 20
) d on a.phone_no = d.phone_no
where d.phone_no is null
;


select phone_no,cell_id,stay_hours,sex,age,mobileprovince,mobilecity,areacode
from workspace.zb_exhibition_visitors_info_results_1909_20200504
where stay_hours > 2
limit 5;


select mobileprovince,mobilecity,age_fenduan,sex_new,count(distinct phone_no) as num
from (
  select mobileprovince,mobilecity,
          case when (sex = '' or sex is null) then '' else sex end as sex_new,
          age,phone_no,
          case when age <= 10 then '0-10'
          when age>10 and age <=20 then '10-20'
          when age>20 and age <=30 then '20-30'
          when age>30 and age <=40 then '30-40'
          when age>40 and age <=50 then '40-50'
          when age>50 and age <=60 then '50-60'
          when age>70 and age <=80 then '70-80'
          else '80以上' end as age_fenduan
  from workspace.zb_exhibition_visitors_info_results_1709_20200504
  where stay_hours > 2
) a
group by mobileprovince,mobilecity,age_fenduan,sex_new
order by mobileprovince,mobilecity,age_fenduan,sex_new
;

select mobileprovince
--mobilecity
--        age_fenduan
--        sex_new
        ,count(distinct phone_no) as num
from (
  select mobileprovince,mobilecity,
          case when (sex = '' or sex is null) then '其他' else sex end as sex_new,
          age,phone_no,
          case when age <= 18 then '少年'
          when age>18 and age <=40 then '青年'
          when age>40 and age <=60 then '中年'
          when age>60 then '老年'
          else '其他' end as age_fenduan
  from workspace.zb_exhibition_visitors_info_results_1910_20200504
  where stay_hours > 1
) a
group by mobileprovince
order by num desc
;


--不剔常驻
--1910
drop table workspace.zb_exhibition_visitors_info_results_1910_20200504_all;
create table workspace.zb_exhibition_visitors_info_results_1910_20200504_all as
select distinct a.phone_no,a.cell_id,b.stay_hours,c.sex,c.age,g.mobileprovince,g.mobilecity,g.areacode
from workspace.zb_exhibition_visitors_info_201910 a
left join(
  select phone_no,data_date,count(distinct data_hour) stay_hours
  from workspace.zb_exhibition_visitors_info_201910
  group by phone_no,data_date
) b on a.phone_no = b.phone_no
left join (
  select phone_no,sex,age 
  from datamart.data_user_baseinfo 
  where dy='2019' and dm='10'
) c on a.phone_no = c.phone_no
left join business.base_mobile_locale g on substr(a.phone_no,1,7) = g.mobilenumber
;
--1909
drop table workspace.zb_exhibition_visitors_info_results_1909_20200504_all;
create table workspace.zb_exhibition_visitors_info_results_1909_20200504_all as
select distinct a.phone_no,a.cell_id,b.stay_hours,c.sex,c.age,g.mobileprovince,g.mobilecity,g.areacode
from workspace.zb_exhibition_visitors_info_201909 a
left join(
  select phone_no,data_date,count(distinct data_hour) stay_hours
  from workspace.zb_exhibition_visitors_info_201909
  group by phone_no,data_date
) b on a.phone_no = b.phone_no
left join (
  select phone_no,sex,age 
  from datamart.data_user_baseinfo 
  where dy='2019' and dm='10'
) c on a.phone_no = c.phone_no
left join business.base_mobile_locale g on substr(a.phone_no,1,7) = g.mobilenumber
;


--检查基站
select distinct a.cell_id,b.cell_id from  workspace.zb_deyang_exhibition_bs_info_20200506 a
left join 
(
  select * from business.base_cell_info where day = '20191010'
) b
on a.cell_id = b.bs_id
;



--17年 国际航展 基站

--导入数据
drop table workspace.zb_deyang_exhibition_bs_info_20200509_2017;
create table workspace.zb_deyang_exhibition_bs_info_20200509_2017(cell_id string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
putdata -f 17_bs_info.txt -t workspace.zb_deyang_exhibition_bs_info_20200509_2017;

--到过航展的用户
drop table workspace.zb_exhibition_visitors_info_201709_new_bs;
create table workspace.zb_exhibition_visitors_info_201709_new_bs as
select distinct a.cell_id,a.phone_no,substr(a.date_time,1,10) as data_date,substr(a.date_time,12,2) as data_hour,substr(a.date_time,15,2) as data_minute
            from
                (
                  select * from business.dwd_user_location_5minute 
                  where dy='2017' and ((dm='09' and (dd = '29' or dd = '30')) or (dm='10' and (dd = '01' or dd = '02' or dd = '03')))
                  --where dy='2019' and dm='10' and (dd = '22' or dd = '23' or dd = '24' or dd = '25')
                )a 
                left join (
                            select cell_id from  workspace.zb_deyang_exhibition_bs_info_20200509_2017 a
                        ) d on a.cell_id = d.cell_id
                where d.cell_id is not null;

--不剔除常驻
--zb_exhibition_visitors_info_results_1709_20200509_bs_new
--剔除常驻
drop table workspace.zb_exhibition_visitors_info_results_1709_20200509_bs_new_nocz;
create table workspace.zb_exhibition_visitors_info_results_1709_20200509_bs_new_nocz as
select distinct a.phone_no,a.data_date,a.cell_id,b.stay_hours,c.sex,c.age,g.mobileprovince,g.mobilecity,g.areacode
from workspace.zb_exhibition_visitors_info_201709_new_bs a
left join(
  select phone_no,data_date,count(distinct data_hour) stay_hours
  from workspace.zb_exhibition_visitors_info_201709_new_bs
  group by phone_no,data_date
) b on a.phone_no = b.phone_no
left join (
  select phone_no,sex,age 
  from datamart.data_user_baseinfo 
  where dy='2018' and dm='03'
) c on a.phone_no = c.phone_no
left join business.base_mobile_locale g on substr(a.phone_no,1,7) = g.mobilenumber
left join(
  select * 
  from workspace.zb_dy_201709_cz 
  where ci_num >= 20
) d on a.phone_no = d.phone_no
where d.phone_no is null
;





--检查
select count(distinct phone_no) from zb_exhibition_visitors_info_results_1709_20200509_bs_new_nocz where stay_hours>1;

select phone_no,count(phone_no) 
from zb_exhibition_visitors_info_results_1709_20200509_bs_new
where stay_hours > 1
group by phone_no
having count(phone_no) > 1 limit 5;

| 1340234Fsjr  | 4    |
| 1340236pIcg  | 2    |
| 1340280pJMg  | 4    |
| 1340282aIXy  | 10   |
| 1340283GwEQ  | 3    |

select * from zb_exhibition_visitors_info_results_1709_20200509_bs_new
where phone_no = '1340282aIXy';

--统计
select mobileprovince
--mobilecity
        --age_fenduan
        --sex_new
        --data_date
        ,count(distinct data_date,phone_no) as num
from (
  select mobileprovince,mobilecity,data_date,
          case when (sex = '' or sex is null) then '其他' else sex end as sex_new,
          age,phone_no,
          case when age <= 18 then '少年'
          when age>18 and age <=40 then '青年'
          when age>40 and age <=60 then '中年'
          when age>60 then '老年'
          else '其他' end as age_fenduan
  from workspace.zb_exhibition_visitors_info_results_1709_20200509_bs_new_nocz
  where stay_hours > 1
) a
group by mobileprovince
--order by data_date
order by num desc
;