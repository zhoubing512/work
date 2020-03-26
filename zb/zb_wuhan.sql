
--第一步：1月份有武汉漫游记录德阳用户
--每次跑数无需修改此段代码直接运行即可
drop table workspace.zb_wuhan;
create table workspace.zb_wuhan
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
    select a.phone_no
    from
    (
      select *
      from 
      (
        select phone_no,count(phone_no) as sum_day
        from 
        (
          select distinct phone_no,dm,dd
          from business.dwd_user_location_day
          where dy = '2020'  and cell_type = 'MC' and bearing_code = '27'
        ) a
        group by a.phone_no
      ) a
      where a.sum_day > 1
    ) a
;

--第二步：累加武汉归属地号码 在2019年11,12月不在德阳，近两日在德阳的用户
--每次跑数请修改dd=后面的日期，仅计算最近两天的日期即可（例如：1月22日跑数，则修改为20日和21日）
insert into  workspace.zb_wuhan
select a.phone_no
from 
(
  select a.phone_no
  from 
  (
    select phone_no 
    from business.dwd_user_location_5minute
    where dy = '2020' and dm = '01' 
    and (dd = '21' or dd = '22')
  ) a
  inner join business.base_mobile_locale b
  on substr(a.phone_no,1,7) = b.mobilenumber
  where b.mobilecity like '%武汉%'
) a
left join 
(
  select  distinct phone_no
  from business.dwd_user_location_day
  where dy = '2019' and dm in ('11','12')
) b
on a.phone_no = b.phone_no
where b.phone_no is null ;

--第三步：德阳各区县近两日常驻用户明细数
--每次跑数请修改dd=后面的日期，仅计算最近两天的日期即可（例如：1月22日跑数，则修改为20日和21日）
drop table workspace.zb_wuhan_dyquxian_detail ;
create table workspace.zb_wuhan_dyquxian_detail
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as

  insert into  workspace.zb_wuhan_dyquxian_detail
  select distinct a.phone_no,b.county_name,b.town_name,b.village_name,'2020-01-22' as data_date
  from 
  (
    select geo_code,phone_no
    from 
    (
      select phone_no,geo_code,row_number() over(distribute by a.phone_no sort by geo_sum_times desc) rank
      from 
      (
        select phone_no,geo_code,count(date_time) as geo_sum_times
        from 
        (
          select phone_no,geo_code,date_time
          from business.dwd_user_location_5minute
          where dy = '2020' and dm = '01' 
          and (dd = '21' or dd = '22') 
          and (substring(date_time,12,2) in ('21','22','23','00','01','02','03','04','05','06'))
        ) a
        group by phone_no,geo_code
      ) a
    ) a
    where a.rank = 1 and a.phone_no in
    (
      select phone_no
      from workspace.zb_wuhan
    )
  ) a
  left join business.base_deyang_country_geocode b
  on a.geo_code = b.geo_code;

select count(phone_no) from  business.dwd_user_location_5minute where dy='2020' and dm='01' and dd='21';
  select distinct data_date from zb_wuhan_dyquxian_detail;

--第四步：生成到区县村组的统计数据
--每次跑数无需修改代码，直接运行即可
drop table workspace.zb_wuhan_dyquxian_count ;
create table workspace.zb_wuhan_dyquxian_count
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as

select *
from 
(
  select data_date,county_name,town_name,village_name,count(distinct phone_no) sum_phone
  from 
  (
    select *
    from workspace.zb_wuhan_dyquxian_detail
  ) a
  group by data_date,a.county_name,a.town_name,a.village_name
) a
order by data_date,county_name,town_name,sum_phone desc
;

 --第五步：统计各个区县的人员数据
 select a.county_name,phone_no_count1,phone_no_count2
 from
 (
  select  county_name,count(distinct phone_no) as phone_no_count1
  from workspace.zb_wuhan_dyquxian_detail
  where data_date='2020-01-21'
  group by county_name
) a
 left join
  (
  select  county_name,count(distinct phone_no) as phone_no_count2
  from workspace.zb_wuhan_dyquxian_detail
  where data_date='2020-01-22'
  group by county_name
) b
  on a.county_name=b.county_name
order by phone_no_count2 desc 
;

中江  404 466
旌阳  401 415
什邡  208 226
绵竹  183 209
广汉  187 194
罗江  104 112


--第六步：导出村组统计数据到文件
insert overwrite local directory '/mnt/disk1/user/lj/results/zb_wuhan_dyquxian_count' row format delimited fields 
terminated by ',' 
select * from workspace.zb_wuhan_dyquxian_count where 1=1 
;
--退出hive压缩文件
merge_reduce.sh /mnt/disk1/user/lj/results/zb_wuhan_dyquxian_count;



--检查是否有重复数据
select count(*)
from 
(
select phone_no,count(phone_no)
from workspace.zb_wuhan_dyquxian_detail
group by phone_no
having count(phone_no) > 1
) a;
