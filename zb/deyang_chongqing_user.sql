--第一步：每个月份有重庆漫游记录德阳用户
drop table workspace.zb_deyang_to_chongqing;
create table workspace.zb_deyang_to_chongqing
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  select phone_no,dm,count(case when dd_dif <> 1 then dd_dif else null end) + 1 as dm_times
  from 
  (
    select phone_no,dm,dd,dd_2,dd_2-dd as dd_dif
    from 
    (
      select phone_no,dm,dd
            ,lead(dd,1,null) over(partition by phone_no,dm order by dd) dd_2
      from
      (
        select distinct phone_no,dm,dd
        from business.dwd_user_location_day a
        where a.dy = '2019'  and a.cell_type = 'MC' and a.bearing_code = '23'
        and a.phone_no in 
        (
          select phone_no
          from 
          (
            select phone_no,dm,count(phone_no) as sum_day
            from 
            (
              select distinct phone_no,dm,dd
              from business.dwd_user_location_day
              where dy = '2019'  and cell_type = 'MC' and bearing_code = '23'
            ) a
            group by a.phone_no,a.dm
          ) a
          where a.sum_day > 1
        )
      ) a
    ) a
  ) a
  group by a.phone_no,a.dm
;

select distinct phone_no,dm,dd
from business.dwd_user_location_day
where dy = '2020'  and cell_type = 'MC' and bearing_code = '23'
and phone_no = '1340814lwWL'
limit 5;

--第二步：每个月份在德阳的重庆用户
drop table workspace.zb_chongqing_to_deyang;
create table workspace.zb_chongqing_to_deyang
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  select phone_no,dm,count(case when dd_dif <> 1 then dd_dif else null end) + 1 as dm_times
  from 
  (
    select phone_no,dm,dd,dd_2,dd_2-dd as dd_dif
    from 
    (
      select phone_no,dm,dd
            ,lead(dd,1,null) over(partition by phone_no,dm order by dd) dd_2
      from
      (
        select distinct a.phone_no,a.dm,a.dd
        from 
        (
            select a.phone_no,a.dm,a.dd
            from 
            (
              select distinct phone_no,dm,dd 
              from business.dwd_user_location_5minute
              where dy = '2019'
            ) a
            inner join business.base_mobile_locale b
            on substr(a.phone_no,1,7) = b.mobilenumber
            where b.mobilecity like '%重庆%'
        ) a
        where a.phone_no in 
        (
          select phone_no
          from 
          (
            select phone_no,dm,count(phone_no) as sum_day
            from 
            (
                select a.phone_no,a.dm,a.dd
                from 
                (
                  select distinct phone_no,dm,dd 
                  from business.dwd_user_location_5minute
                  where dy = '2019'
                ) a
                inner join business.base_mobile_locale b
                on substr(a.phone_no,1,7) = b.mobilenumber
                where b.mobilecity like '%重庆%'
            ) a
            group by a.phone_no,a.dm
          ) a
          where a.sum_day > 1
        )
      ) a
    ) a
  ) a
  group by a.phone_no,a.dm
;

select dm,sum(dm_times) 
from workspace.zb_deyang_to_chongqing
group by dm
order by dm
;

select dm,sum(dm_times) 
from workspace.zb_chongqing_to_deyang
group by dm
order by dm
;
