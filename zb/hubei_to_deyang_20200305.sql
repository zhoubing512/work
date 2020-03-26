--第一步：1月份有武汉漫游记录德阳用户
--每次跑数无需修改此段代码直接运行即可
drop table workspace.zb_hubei;
create table workspace.zb_hubei
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  select a.phone_no,count(a.day_time_2) as day_dy
  from 
  (
  select a.phone_no,a.day_time_2
  from 
  (
    select a.phone_no,a.day_time,b.day_time_2,case when a.day_time > b.day_time_2 then 1 else 0 end as day_dif
    from 
    (
      select a.phone_no,a.day_time
      from
      (
        select a.phone_no,sum_day,day_time,row_number() over(partition by phone_no order by day_time) rn
        from 
        (
          select phone_no,concat(dy,'-',dm,'-',dd) as day_time,count(phone_no) over(partition by phone_no) as sum_day
          from 
          (
            select distinct phone_no,dy,dm,dd
            from business.dwd_user_location_day
            where (dy = '2020' or (dy = '2019' and dm = '12'))  and cell_type = 'MC' and 
            (bearing_code = '27'
              or bearing_code = '714'
              or bearing_code = '719'
              or bearing_code = '717'
              or bearing_code = '710'
              or bearing_code = '711'
              or bearing_code = '714'
              or bearing_code = '712'
              or bearing_code = '716'
              or bearing_code = '713'
              or bearing_code = '715'
              or bearing_code = '722'
              or bearing_code = '718'
              or bearing_code = '728'
              or bearing_code = '719'
              )
          ) a
        ) a
        where a.sum_day > 1
      ) a
      where rn = 1
    ) a
    left join 
    (
      select distinct phone_no,concat(dy,'-',dm,'-',dd) as day_time_2
      from business.dwd_user_location_hour
      where dy = '2020'
    ) b
    on a.phone_no = b.phone_no
  ) a
  where day_dif = 1
  ) a
  group by a.phone_no
  having count(a.day_time_2) > 10
;
11541 5天
9934  10天
武汉：27
黄石：714
十堰：719
宜昌：717
襄樊：710
鄂州：711
荆门：724
孝感：712
荆州：716
黄冈：713
咸宁：715
随州：722
恩施：718
仙桃、潜江、天门：728
神农架：719
--第二步：累加武汉归属地号码 在2019年11,12月不在德阳，近两日在德阳的用户
insert into  workspace.zb_hubei
select a.phone_no,'' as day_dy
from 
(
  select a.phone_no
  from 
  (
    select phone_no,count(phone_no)
    from 
    (
      select distinct phone_no,dm,dd
      from business.dwd_user_location_5minute
      where dy = '2020' 
    ) a
    group by phone_no
    having count(phone_no) > 10
  ) a
  inner join business.base_mobile_locale b
  on substr(a.phone_no,1,7) = b.mobilenumber
  where b.mobileprovince like '%湖北%'
) a
left join 
(
  select  distinct phone_no
  from business.dwd_user_location_day
  where dy = '2019' and dm = '12'
) b
on a.phone_no = b.phone_no
where b.phone_no is null ;
13093 5天

11152 10天

11724 10天
--统计总数
select count(distinct phone_no)
from workspace.zb_hubei
where phone_no like '1%' and phone_no not like '10%'
;


select * from base_mobile_locale where mobileprovince like '%湖北%' limit 5;