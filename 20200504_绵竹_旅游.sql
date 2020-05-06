--关于绵竹市旅游景区大数据相关需求
--导入数据
drop table workspace.zb_mianzhu_bs_info_20200504;
create table workspace.zb_mianzhu_bs_info_20200504(quyu_name string,lac_id string,cell_id string,cell_name string,xiaoquzhishi string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
putdata -f 20200504_mianzhu_lvyou.txt -t workspace.zb_mianzhu_bs_info_20200504;

select quyu_name,lac_id,cell_id,cell_name,xiaoquzhishi
from zb_mianzhu_bs_info_20200504
limit 10;

select phone_no,cell_id,geo_code,cell_type,date_time,cnt,dy,dm,dd
from dwd_user_location_5minute
where dy = '2020' and dm = '05' and dd = '01'
and phone_no = '1368966zkbQ'
limit 5;

--所有4.30-5.4游客  在30号9点后-4号 中途出现2小时以上，并且在28号及以前未在景区出现过(目的排除本地居民，工作人员等)
--4月30号前在景区出现用户
--zb_mianzhu_lvyou_20200504_tmp
--test  zb_mianzhu_lvyou_20200504_tmp_test
drop table workspace.zb_mianzhu_lvyou_20200504_tmp_test;
create table workspace.zb_mianzhu_lvyou_20200504_tmp_test
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select phone_no,quyu_name,count(date_time)*5.0 as time_long
from 
(
    select a.cell_name,a.quyu_name,b.phone_no,b.cell_id,b.geo_code,b.cell_type,b.date_time,b.dy,b.dm,b.dd
    from workspace.zb_mianzhu_bs_info_20200504 a
    left join(
        select * 
        from business.dwd_user_location_5minute
        where dy = '2020' and dm = '04' and dd <> '29' and dd <> '30' and dd <> '28'
        and dd <> '27' and dd <> '26'
    ) b on a.cell_id = b.cell_id
) a
group by phone_no,quyu_name
having count(date_time) > 96
 ;


 --4.30-5.3出现在景区2小时以上的用户，并排除掉4.28以前出现在景区8小时以上的用户，即为游客用户
 --zb_mianzhu_lvyou_20200504_tmp_youke
 --test  zb_mianzhu_lvyou_20200504_tmp_youke_test
drop table workspace.zb_mianzhu_lvyou_20200504_tmp_youke;
create table workspace.zb_mianzhu_lvyou_20200504_tmp_youke
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  select a.*
  from (
    select phone_no,quyu_name,count(date_time)*5.0 as time_long
    from 
    (
        select a.cell_name,a.quyu_name,b.phone_no,b.cell_id,b.geo_code,b.cell_type,b.date_time,b.dy,b.dm,b.dd
        from workspace.zb_mianzhu_bs_info_20200504 a
        left join(
            select * 
            from business.dwd_user_location_5minute
            where dy = '2020' and ((dm = '04' and dd = '30' ) or (dm = '05'))
        ) b on a.cell_id = b.cell_id
    ) a
    group by phone_no,quyu_name
    having count(date_time) > 12
  ) a 
  left join (
      select distinct phone_no from workspace.zb_mianzhu_lvyou_20200504_tmp
  ) b
  on a.phone_no = b.phone_no where b.phone_no is null
  ;


--base_mobile_locale
select quyu_name,count(distinct phone_no)
from 
(
    select a.quyu_name,a.phone_no,
    from workspace.zb_mianzhu_lvyou_20200504_tmp_youke a
    left join business.base_mobile_locale b
    on substr(a.phone_no,1,7) = b.mobilenumber

) a
group by quyu_name
;


--林静口径(与19年五一提数  绵竹提数一致)
drop table workspace.zb_visitors_info_20200504;
create table workspace.zb_visitors_info_20200504 as
select distinct a.imsi,a.cell_id,substr(a.date_time,1,10) as data_date,substr(a.date_time,12,2) as data_hour,substr(a.date_time,15,2) as data_minute
            ,b.phone_no,quyu_name
            from
                (
                select * from business.dwd_user_location_5minute 
                        where 1 = 1 
                        and dy='2020' and concat(dm,dd)>='0430' and concat(dm,dd)<='0504'
                )a  
                left join (select * from business.dwd_imsi_phone where eff_flag='1') b on a.imsi = b.imsi
                left join (
                        select * from  workspace.zb_mianzhu_bs_info_20200504
                        ) d on a.cell_id = d.cell_id
                where d.cell_id is not null;


drop table workspace.zb_visitors_info_results_20200504;
create table workspace.zb_visitors_info_results_20200504 as
      select distinct a.data_date,a.data_hour,a.imsi,a.phone_no,a.cell_id
             ,a.quyu_name
             ,b.stay_days,d.stay_hours,d1.stay_night_days,e.stay_minutes
             ,f.sex,f.age
             ,case when f.age<=18 then '少年' 
                                when f.age >18 and f.age<=40 then '青年'
                                when f.age >40 and f.age<=60 then '中年'
                                when f.age >60 then '老年'
                                else '其他' end age_type
             ,g.mobileprovince,g.mobilecity,g.areacode
             ,case when h.phone_no is null then '非常驻' else '常驻' end obode_day
              from 
              (   
                select imsi,data_date,data_hour,phone_no,quyu_name,cell_id
                               from workspace.zb_visitors_info_20200504
              )a
              left join (
                          select phone_no,quyu_name,count(distinct data_date) stay_days
                                 from (select phone_no,quyu_name,data_date,count(distinct data_hour) stay_hours
                                        from workspace.zb_visitors_info_20200504
                                         group by phone_no,quyu_name,data_date
                                         having count(distinct data_hour)>1) as b1
                                 group by phone_no,quyu_name
                        ) b on a.phone_no = b.phone_no
                            and a.quyu_name=b.quyu_name
              left join (
                          select phone_no,quyu_name,data_date,count(distinct data_hour) stay_hours
                                 from workspace.zb_visitors_info_20200504
                                 group by phone_no,quyu_name,data_date
                        ) d on a.phone_no = d.phone_no 
                            and a.data_date = d.data_date 
                            and a.quyu_name=d.quyu_name
              left join (                                 
                          select phone_no,quyu_name,count(distinct data_date) stay_night_days
                                 from (select phone_no,quyu_name,data_date,count(distinct data_hour) stay_hours
                                        from workspace.zb_visitors_info_20200504
                                         where data_hour in ('22','23','00','01','02','03','04','05','06','07')
                                         group by phone_no,quyu_name,data_date
                                         having count(distinct data_hour)>1) as d1
                                 group by phone_no,quyu_name
                        ) d1 on a.phone_no = d1.phone_no 
                            and a.quyu_name=d1.quyu_name                      
              left join (
                          select phone_no,quyu_name,data_date,data_hour,count(distinct data_minute) stay_minutes
                                 from workspace.zb_visitors_info_20200504
                                 group by phone_no,quyu_name,data_date,data_hour
                        ) e on a.phone_no = e.phone_no 
                            and a.data_date = e.data_date 
                            and a.quyu_name=e.quyu_name
                            and a.data_hour = e.data_hour
              
              left join (select phone_no,sex,age from datamart.data_user_baseinfo where dy='2020' and dm='02') f on a.phone_no = f.phone_no
              left join business.base_mobile_locale g on substr(a.phone_no,1,7) = g.mobilenumber
              
              left join(
                        select a.phone_no,a.cell_id,a.days,quyu_name from 
                               (select * from workspace.lj_dwd_obode_kb_202003) a
                               left join workspace.zb_mianzhu_bs_info_20200504 c on a.cell_id = c.cell_id
                               where a.days>20
                       ) h
                   on a.phone_no = h.phone_no 
                   and a.quyu_name=h.quyu_name;


--外省来访
select quyu_name,count(distinct phone_no) visitos 
       from workspace.zb_visitors_info_results_20200504 a 
       where mobileprovince  not like '%四川%' 
         and  obode_day = '非常驻' 
         and  stay_days>0 and stay_days<5 
group by quyu_name
order by quyu_name
         ;

         
--省内
select quyu_name,count(distinct phone_no) visitos 
       from workspace.zb_visitors_info_results_20200504 a 
       where mobileprovince  like '%四川%' 
         and mobilecity not like '%德阳%'
         and  obode_day = '非常驻' 
         and stay_days>0 and stay_days<5
group by quyu_name
order by quyu_name
         ; 
         
--德阳           
select quyu_name,count(distinct phone_no) visitos 
       from workspace.zb_visitors_info_results_20200504 a 
       where 
              mobilecity  like '%德阳%'
         and  obode_day = '非常驻' 
         and (stay_hours > 2 or (stay_days>1 and stay_days<4))
group by quyu_name
order by quyu_name
         ;


--过夜    
select quyu_name,count(distinct phone_no) visitos 
       from workspace.zb_visitors_info_results_20200504 a 
       where obode_day = '非常驻' and stay_night_days>=1
group by quyu_name
order by quyu_name
         ;

--驻留多日

select quyu_name,count(distinct phone_no) visitos 
       from workspace.zb_visitors_info_results_20200504 a 
       where obode_day = '非常驻' 
         and (stay_night_days >1 and stay_night_days<5)
group by quyu_name
order by quyu_name
         ;




-- 19年绵竹大数据分析

--========================绵竹大数据分析====================================
create table workspace.lj_bigdata_mz_20190513(jq string,cell_id string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
load data local inpath '/mnt/disk1/user/lj/temp_files/mz_cell_20190513.txt' into table workspace.lj_bigdata_mz_20190513;

 set hive.execution.engine=spark;
 create table workspace.visitors_info_mz_20190513 as
 select distinct a.imsi,a.cell_id,substr(a.date_time,1,10) as data_date,substr(a.date_time,12,2) as data_hour,substr(a.date_time,15,2) as data_minute
                ,b.phone_no,jq
                from
                 (
                   select * from business.dwd_user_location_5minute 
                            where 1 = 1 
                            and dy = 2019 and concat(dm,dd)>='0501' and concat(dm,dd)<='0504'
                 )a  
                 left join (select * from business.dwd_imsi_phone where eff_flag='1') b on a.imsi = b.imsi
                 left join (
                            select * from  workspace.lj_bigdata_mz_20190513
                           ) d on a.cell_id = d.cell_id
                 where d.cell_id is not null;
                 
                 
                  
create table workspace.visitors_info_results_mz_20190513 as
      select distinct a.data_date,a.data_hour,a.imsi,a.phone_no,a.cell_id
             ,a.jq
             ,b.stay_days,d.stay_hours,e.stay_minutes
             ,f.sex,f.age
             ,case when f.age<=18 then '少年' 
                                when f.age >18 and f.age<=40 then '青年'
                                when f.age >40 and f.age<=60 then '中年'
                                when f.age >60 then '老年'
                                else '其他' end age_type
             ,g.mobileprovince,g.mobilecity,g.areacode
             ,case when h.phone_no is null then '非常驻' else '常驻' end obode_day
              from 
              (   
                select imsi,data_date,data_hour,phone_no,jq,cell_id
                               from workspace.visitors_info_mz_20190513
              )a
              left join (
                          select phone_no,jq,count(distinct data_date) stay_days
                                 from workspace.visitors_info_mz_20190513
                                 group by phone_no,jq
                        ) b on a.phone_no = b.phone_no  
                            and a.jq=b.jq
              left join (
                          select phone_no,jq,data_date,count(distinct data_hour) stay_hours
                                 from workspace.visitors_info_mz_20190513
                                 group by phone_no,jq,data_date
                        ) d on a.phone_no = d.phone_no 
                            and a.data_date = d.data_date 
                            and a.jq=d.jq
              left join (
                          select phone_no,jq,data_date,data_hour,count(distinct data_minute) stay_minutes
                                 from workspace.visitors_info_mz_20190513
                                 group by phone_no,jq,data_date,data_hour
                        ) e on a.phone_no = e.phone_no 
                            and a.data_date = e.data_date 
                            and a.jq=e.jq
                            and a.data_hour = e.data_hour
              
              left join (select phone_no,sex,age from datamart.data_user_baseinfo where dy=2019 and dm=02) f on a.phone_no = f.phone_no
              left join business.base_mobile_locale g on substr(a.phone_no,1,7) = g.mobilenumber
              
              left join(
                        select a.phone_no,a.cell_id,a.days,jq from 
                               (select * from workspace.dwd_obode_kb_201904)a
                               left join workspace.lj_bigdata_mz_20190513 c on a.cell_id = c.cell_id
                               where a.days>20
                       ) h 
                   on a.phone_no = h.phone_no 
                   and a.jq=h.jq;
                   
    set hive.execution.engine=mr;   
    
    


select count(distinct phone_no) visitos 
       from workspace.visitors_info_results_mz_20190513 a 
       where  obode_day='非常驻'
         and (stay_hours > 2)-- or (stay_days>2 and stay_days<4 ))
         ;
         
--外省来访
select jq,count(distinct phone_no) visitos 
       from workspace.visitors_info_results_mz_20190513 a 
       where mobileprovince  not like '%四川%' 
         and  obode_day = '非常驻' 

         and (stay_hours > 1 or (stay_days>1 and stay_days<4))
group by jq
order by jq
         ;
         
--省内
select jq,count(distinct phone_no) visitos 
       from workspace.visitors_info_results_mz_20190513 a 
       where mobileprovince  like '%四川%' 
         and mobilecity not like '%德阳%'
         and  obode_day = '非常驻' 

         and (stay_hours > 1 or (stay_days>1 and stay_days<4))
group by jq
order by jq
         ;   
         
--德阳           
select jq,count(distinct phone_no) visitos 
       from workspace.visitors_info_results_mz_20190513 a 
       where 
              mobilecity  like '%德阳%'
         and  obode_day = '非常驻' 

         and (stay_hours > 1 or (stay_days>1 and stay_days<4))
group by jq
order by jq
         ;
         
         
--过夜    
select jq,count(distinct phone_no) visitos 
       from workspace.visitors_info_results_mz_20190513 a 
       where obode_day = '非常驻' 

         and (stay_days = 2)
group by jq
order by jq
         ;

--驻留多日

select jq,count(distinct phone_no) visitos 
       from workspace.visitors_info_results_mz_20190513 a 
       where obode_day = '非常驻' 

         and (stay_days >2 and stay_days<5)
group by jq
order by jq
         ;





--------------4月景区大数据-----------------

 drop table workspace.lj_visitors_info_20200427;
 create table workspace.lj_visitors_info_20200427 as
 select distinct a.imsi,a.cell_id,substr(a.date_time,1,10) as data_date,substr(a.date_time,12,2) as data_hour,substr(a.date_time,15,2) as data_minute
                ,b.phone_no,jq
                from
                 (
                   select * from business.dwd_user_location_5minute 
                            where 1 = 1 
                            and dy='2020' and concat(dm,dd)>='0326' and concat(dm,dd)<='0425'
                 )a  
                 left join (select * from business.dwd_imsi_phone where eff_flag='1') b on a.imsi = b.imsi
                 left join (
                            select * from  workspace.lj_bigdata_20190326
                           ) d on a.cell_id = d.cell_id
                 where d.cell_id is not null;


drop table workspace.lj_visitors_info_results_20200427;
create table workspace.lj_visitors_info_results_20200427 as
      select distinct a.data_date,a.data_hour,a.imsi,a.phone_no,a.cell_id
             ,a.jq
             ,b.stay_days,d.stay_hours,d1.stay_night_days,e.stay_minutes
             ,f.sex,f.age
             ,case when f.age<=18 then '少年' 
                                when f.age >18 and f.age<=40 then '青年'
                                when f.age >40 and f.age<=60 then '中年'
                                when f.age >60 then '老年'
                                else '其他' end age_type
             ,g.mobileprovince,g.mobilecity,g.areacode
             ,case when h.phone_no is null then '非常驻' else '常驻' end obode_day
              from 
              (   
                select imsi,data_date,data_hour,phone_no,jq,cell_id
                               from workspace.lj_visitors_info_20200427
              )a
              left join (
                          select phone_no,jq,count(distinct data_date) stay_days
                                 from (select phone_no,jq,data_date,count(distinct data_hour) stay_hours
                                        from workspace.lj_visitors_info_20200427
                                         group by phone_no,jq,data_date
                                         having count(distinct data_hour)>1) as b1
                                 group by phone_no,jq
                        ) b on a.phone_no = b.phone_no
                            and a.jq=b.jq
              left join (
                          select phone_no,jq,data_date,count(distinct data_hour) stay_hours
                                 from workspace.lj_visitors_info_20200427
                                 group by phone_no,jq,data_date
                        ) d on a.phone_no = d.phone_no 
                            and a.data_date = d.data_date 
                            and a.jq=d.jq
              left join (                                 
                          select phone_no,jq,count(distinct data_date) stay_night_days
                                 from (select phone_no,jq,data_date,count(distinct data_hour) stay_hours
                                        from workspace.lj_visitors_info_20200427
                                         where data_hour in ('22','23','00','01','02','03','04','05','06','07')
                                         group by phone_no,jq,data_date
                                         having count(distinct data_hour)>1) as d1
                                 group by phone_no,jq
                        ) d1 on a.phone_no = d1.phone_no 
                            and a.jq=d1.jq                      
              left join (
                          select phone_no,jq,data_date,data_hour,count(distinct data_minute) stay_minutes
                                 from workspace.lj_visitors_info_20200427
                                 group by phone_no,jq,data_date,data_hour
                        ) e on a.phone_no = e.phone_no 
                            and a.data_date = e.data_date 
                            and a.jq=e.jq
                            and a.data_hour = e.data_hour
              
              left join (select phone_no,sex,age from datamart.data_user_baseinfo where dy='2020' and dm='02') f on a.phone_no = f.phone_no
              left join business.base_mobile_locale g on substr(a.phone_no,1,7) = g.mobilenumber
              
              left join(
                        select a.phone_no,a.cell_id,a.days,jq from 
                               (select * from workspace.lj_dwd_obode_kb_202003) a
                               left join workspace.lj_bigdata_20190326 c on a.cell_id = c.cell_id
                               where a.days>20
                       ) h
                   on a.phone_no = h.phone_no 
                   and a.jq=h.jq;

--select * from workspace.lj_visitors_info_results_20200427  where phone_no='1588447RwWQ';

--外省来访
select jq,count(distinct phone_no) visitos 
       from workspace.lj_visitors_info_results_20200427 a 
       where mobileprovince  not like '%四川%' 
         and  obode_day = '非常驻' 
         and  stay_days>0 and stay_days<5 
group by jq
order by jq
         ;

         
--省内
select jq,count(distinct phone_no) visitos 
       from workspace.lj_visitors_info_results_20200427 a 
       where mobileprovince  like '%四川%' 
         and mobilecity not like '%德阳%'
         and  obode_day = '非常驻' 
         and stay_days>0 and stay_days<5
group by jq
order by jq
         ; 
         
--德阳           
select jq,count(distinct phone_no) visitos 
       from workspace.lj_visitors_info_results_20200427 a 
       where 
              mobilecity  like '%德阳%'
         and  obode_day = '非常驻' 
         and stay_days>0 and stay_days<5
group by jq
order by jq
         ;

         
--过夜    
select jq,count(distinct phone_no) visitos 
       from workspace.lj_visitors_info_results_20200427 a 
       where obode_day = '非常驻' and stay_night_days>=1
group by jq
order by jq
         ;

--驻留多日

select jq,count(distinct phone_no) visitos 
       from workspace.lj_visitors_info_results_20200427 a 
       where obode_day = '非常驻' 
         and (stay_night_days >1 and stay_night_days<5)
group by jq
order by jq
         ;
 