
-- 经过学校geo_code的所有用户
drop table th_gaoxiao_pass_phone_2016;
create table workspace.th_gaoxiao_pass_phone_2016
    row format delimited fields terminated by '\001' stored as orc tblproperties ('orc.compress'='ZLIB') 
as
select distinct p.phone_no from  business.dwd_user_location_5minute p  
where dy=2016 and dm in ('09','10','11','12')
--and (substring(date_time,12,2) in ('21','22','23','00','01','02','03','04','05','06','10','11')) 
and  p.geo_code in (select g.geo_code from workspace.th_gaoxiao_geo  g);


-- 计算2016年高校用户的加权经纬度
--drop table workspace.th_gaoxiao_lonlat_mid_night_2016;
--create table workspace.th_gaoxiao_lonlat_mid_night_2016 (dm string,dd string,phone_no string,geo_code string,stays int,rank int);

insert overwrite table workspace.th_gaoxiao_lonlat_mid_night_2016
select c.dm,c.dd,c.phone_no,c.geo_code,c.stays,c.rank from
(
  select f.* from
  (
    select e.*,row_number() over(distribute by e.dm,e.dd,e.phone_no sort by stays desc) rank from
    (
        select a.dm,a.dd,a.phone_no,a.geo_code,count(distinct a.date_time) as stays
        from
        (
            select n.phone_no,n.geo_code,n.date_time,n.dm,n.dd
            from
                 (
                      select phone_no,geo_code,date_time,dm,dd
                      from business.dwd_user_location_5minute m
                      where dy=2016 and dm in ('09','10','11','12')
                      --and (substring(date_time,12,2) in ('21','22','23','00','01','02','03','04','05','06','10','11','',''))
                       and m.phone_no in
                       (
                            select phone_no from th_gaoxiao_pass_phone_2016
                       )
                 ) n 

        ) a
        group by a.dm,a.dd,a.phone_no,a.geo_code
    ) e
  ) f
  where f.rank<9
) c;

--使用中间表计算距离
--drop table gaoxiao_geo_night_2016;
--create table workspace.gaoxiao_geo_night_2016 (dm string,dd string,phone_no string,geo_code string,p_long float);
insert overwrite table workspace.gaoxiao_geo_night_2016
select m.dm,m.dd,m.phone_no,geo_code,p_long from
(
select n.* from
(
    select d.*,row_number() over(distribute by d.dm,d.dd,d.phone_no sort by stays desc) rank from
    (
      select c.* from
      (
        select a.dm,a.dd,a.phone_no,a.geo_code,a.stays,default.geo_distance(a.geo_code,b.geo_code) as p_long 
        from th_gaoxiao_lonlat_mid_night_2016 a
        left join 
        (
            select dm,dd,phone_no,geo_code from th_gaoxiao_lonlat_mid_night_2016
            where rank=1
        )b
        on a.dm=b.dm and a.dd=b.dd and a.phone_no=b.phone_no 
      ) c 
      where c.p_long<32
    ) d
  ) n
  where n.rank<9
)m;

--晚上高校常驻人员中间表1（可精确到geo）
--drop table workspace.th_gaoxiao_alluser_detail_mid_night0_2016;
--create table workspace.th_gaoxiao_alluser_detail_mid_night0_2016
--    row format delimited fields terminated by '\001' stored as orc tblproperties ('orc.compress'='ZLIB') 
--as
insert overwrite table workspace.th_gaoxiao_alluser_detail_mid_night0_2016
select dm,dd,phone_no,college_id,college_name,day_college_num
from
(
    select n.*,row_number()over(distribute by n.phone_no,dm,dd,college_id sort by day_college_num desc) as rank 
    from
    (
        select dm,dd,phone_no,college_id,college_name,count(geo_code) as day_college_num
        from
        (
            select a.dm,a.dd,a.phone_no,a.geo_code,b.college_id,b.college_name,p_long
            from gaoxiao_geo_night_2016 a
            left join th_gaoxiao_geo b
            on a.geo_code=b.geo_code
            where b.geo_code is not null
        ) m
        group by dm,dd,phone_no,college_id,college_name
    ) n
) p
where p.rank = 1;

--晚上高校常驻人员中间表2（因为需要统计同属于一个学校的geo，所以无法在精确到geo的明细）
--增加学校互斥，即在一个学校出现次数最多的那个学校算作用户所在学校，避免重复计算。
--create table workspace.th_gaoxiao_alluser_detail_mid_night_2016 (phone_no string,college_id string,college_name string,sum_day int,sum_month int,jq_days int);
insert overwrite table workspace.th_gaoxiao_alluser_detail_mid_night_2016
select d.phone_no,d.college_id,d.college_name,d.sum_day,d.sum_month,d.jq_days
from
(
    select  c.phone_no,c.college_id,c.college_name,c.sum_day,c.sum_month,c.jq_days,row_number()over(distribute by c.phone_no sort by sum_day desc,sum_month desc) as rank 
    from
    (
        select m.phone_no,m.college_id,m.college_name,sum(m.days) as sum_day,count(m.dm) as sum_month,concat(count(m.dm),sum(m.days)) as jq_days
        from
        (
            select dm,phone_no,college_id,college_name,count(college_id) as days
            from workspace.th_gaoxiao_alluser_detail_mid_night0_2016 
            group by dm,phone_no,college_id,college_name
         )m
        group by m.phone_no,m.college_id,m.college_name
    )c
) d
left join workspace.th_gaoxiao_set_config_value e
on d.college_id = e.college_id
where d.rank=1
and d.sum_day>e.oldyear_sum_day
and d.sum_month>=oldyear_sum_month ;

