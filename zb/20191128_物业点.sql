
drop table workspace.zb_lon_lat_20191128;
create table workspace.zb_lon_lat_20191128(quxian string,cell_name string,s_lon string,s_lat string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
load data local inpath '/mnt/disk1/user/lj/temp_files/xiaoqu.txt' into table  workspace.zb_lon_lat_20191128;
------营销活动
drop table workspace.zb_act_20191128;
create table workspace.zb_act_20191128(daima string,act_name string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
load data local inpath '/mnt/disk1/user/lj/temp_files/act.txt' into table  workspace.zb_act_20191128;
--经纬度转化为geo_code

create table workspace.zb_keep_geo_20191128
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select quxian ,cell_name ,s_lon ,s_lat,default.geo_Encode(s_lon,s_lat) as scb_geo
from workspace.zb_lon_lat_20191128
;

------距离100米的geo_code
drop table workspace.zb_keep_geo_distance_20191128;
create table workspace.zb_keep_geo_distance_20191128
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select quxian ,cell_name ,s_lon ,a.s_lat ,a.scb_geo ,b.geo_code ,default.geo_distance(a.scb_geo,b.geo_code) as juli
from workspace.zb_keep_geo_20191128 a ,(select distinct geo_code from business.base_deyang_country_geocode) b
where  default.geo_distance(a.scb_geo,b.geo_code) <= 200
;

--150米 36个小区无数据
--200米 21个小区无数据
select * from workspace.zb_keep_geo_distance_20191128 order by scb_geo  limit 10;




------夜间常驻用户

----筛选常驻人口号码
drop table workspace.zb_wyd_20191128_cz;
create table workspace.zb_wyd_20191128_cz 
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as 
  select * 
  from 
  (
        select dd.phone_no,dd.geo_code,dd.geo_num,row_number() over (partition by phone_no order by geo_num desc ) num  
        from (
            select bb.phone_no,bb.geo_code,count(bb.geo_code) geo_num 
            from 
            (
                select * 
                from 
                (
                  select phone_no,geo_code,time_type,duration,dd,row_number() over (partition by phone_no,dd order by duration desc ) num  
                  from 
                  ( select phone_no,geo_code,time_type,sum(duration) as duration,dd
                    from
                    business.dwd_user_location_day  
                  where dy='2019' and dm='11' and time_type in('sf')
                  group by phone_no,geo_code,time_type,dd
                  ) a
                ) cc
                where cc.num = 1
            ) bb
            group by phone_no,geo_code
        ) dd
    ) as ff
  where ff.num=1 
  and ff.phone_no not like '10%' 
  and ff.phone_no like '1%' 
  ;

drop table workspace.zb_wyd_20191128_cz;
create table workspace.zb_wyd_20191128_cz 
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as 
  select * 
  from 
  (
        select * 
        from 
        (
          select phone_no,geo_code,geo_num,row_number() over (partition by phone_no order by geo_num desc ) num  
          from 
          ( 
            select phone_no,geo_code,count(geo_code) as geo_num
            from
            business.dwd_user_location_day  
              where dy='2019' and dm='11' and time_type in('sf')
              group by phone_no,geo_code
          ) a
        ) cc
        where cc.num = 1
    ) ff
  where ff.phone_no not like '10%' 
  and ff.phone_no like '1%' 
  ;






--直接计算用户的geo_code和小区geo_code距离，取小于100米的
drop table workspace.zb_wyd_20191128_cz_new;
create table workspace.zb_wyd_20191128_cz_new 
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as 
select phone_no,geo_num,quxian ,cell_name ,s_lon ,a.s_lat ,a.scb_geo ,b.geo_code ,default.geo_distance(a.scb_geo,b.geo_code) as juli
from workspace.zb_keep_geo_20191128 a ,(select phone_no,geo_code,geo_num from workspace.zb_wyd_20191128_cz) b
;
where  default.geo_distance(a.scb_geo,b.geo_code) <= 100
;



select phone_no,geo_code,time_type,duration,dd
from business.dwd_user_location_day  
where dy='2019' and dm='11' and time_type in('sf') and phone_no='1340838FJEL'
order by dd;

  select count(phone_no) from workspace.zb_wyd_20191128_cz where juli<300; 

  select * from workspace.zb_wyd_20191128_cz where phone_no is null;

    select count(phone_no),count(distinct phone_no) from  workspace.zb_wyd_20191128_cz;

select *
from (
            select quxian ,cell_name ,s_lon ,a.s_lat ,a.scb_geo ,b.geo_code ,juli ,b.phone_no
            from workspace.zb_keep_geo_distance_20191128 a
            left join 
            (
                select phone_no,geo_code
                from workspace.zb_wyd_20191128_cz
                where geo_num>=4
            ) b on a.geo_code = b.geo_code

) a where phone_no='1340838FJEL';


select count(phone_no),count(distinct phone_no) from zb_wyd_geo_20191128;

-----data_kd_user_t 最终结果
drop table workspace.zb_wyd_geo_20191202;
create table workspace.zb_wyd_geo_20191202
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
    select distinct d.county_name,d.fenju,d.cell_name,d.phone_no,d.product_name
    ,d.kd_family_name,d.kd_family_prod,d.kd_main_phone,d.kd_cys,e.kd_no1,g.kd_area_name,d.g4_user_flag
    ,case when f.phone_no is not null then '1' else '0' end as shifouheyue
    ,d.durations_m,d.flow,d.total_fee,d.online_days,d.machine_type,d.imei_chg_ym,h.cy_phone1,h.cy_phone2,h.cy_phone3
    ,m.mobileprovince,m.mobilecity,d.juli
    from 
    (
        select quxian ,cell_name ,s_lon ,b.s_lat ,b.scb_geo ,b.geo_code ,juli ,b.phone_no
        ,d.county_name,d.fenju,c.product_name,c.kd_family_name
        ,c.kd_family_prod,c.kd_main_phone,c.kd_cys,c.durations_m,c.flow,c.total_fee,c.online_days,c.machine_type
        ,c.g4_user_flag,c.imei_chg_ym,d.mobileprovince,d.mobilecity 
        from (
            select * 
            from workspace.zb_wyd_20191128_cz_new
--            select * from 
--            (
--            select  quxian ,cell_name ,s_lon ,s_lat ,scb_geo ,geo_code ,juli ,phone_no 
--            ,row_number() over (partition by phone_no order by juli ) num
--            from (
--                        select quxian ,cell_name ,s_lon ,a.s_lat ,a.scb_geo ,b.geo_code ,juli ,b.phone_no
--                        from workspace.zb_keep_geo_distance_20191128 a
--                        left join 
--                        (
--                            select phone_no,geo_code
--                            from workspace.zb_wyd_20191128_cz
--                            --where geo_num>=4
--                        ) b on a.geo_code = b.geo_code
--                ) a
--            ) a where a.num=1
        ) b 
        left join 
        (
            select phone_no,product_name,kd_family_name
            ,kd_family_prod,kd_main_phone,kd_cys,durations_m,flow,total_fee,online_days,machine_type,g4_user_flag,imei_chg_ym 
            from datamart.data_dw_user_base_info 
            where dy='2019' 
            and dm='10'
        ) c on b.phone_no=c.phone_no
        left join 
        (
            select distinct phone_no,county_name,fenju,mobileprovince,mobilecity
            from workspace.th_deyang_alluser_cunzu_fenju_detail_night_2019 
        ) d 
        on b.phone_no=d.phone_no
    ) d
    left join 
    (
        select phone_no,kd_no1 
        from datamart.dw_user_kd 
        where dy='2019' 
        and dm='10'
    ) e 
    on d.phone_no=e.phone_no
    left join 
    (
        select distinct phone_no 
        from 
        (
            select phone_no 
            from workspace.zb_act_20191128 a 
            left join datamart.dw_user_act b 
            on a.daima = b.means_id_a
        ) a
    ) f 
    on d.phone_no = f.phone_no
    left join 
    (
      select phone_no,kd_area_name,num 
      from
        (
            select phone_no,kd_area_name,dy,dm,row_number() over (partition by phone_no order by dm desc ) num
            from datamart.data_kd_user_t 
            where dy='2019' 
            and dm='07'
        ) a 
        where a.num=1
    )  g 
    on d.kd_main_phone=g.phone_no
    left join 
    (
        --家庭圈异网
            select phone_no,cy_phone1,cy_phone2,cy_phone3,rank 
            from 
            (
                select d.phone_no,d.cy_phone as cy_phone1
                ,lead(cy_phone,1) over(partition by d.phone_no order by rank) as cy_phone2
                ,lead(cy_phone,2) over(partition by d.phone_no order by rank) as cy_phone3
                ,row_number() over (partition by phone_no order by rank ) as rank
                from
                (
                    select phone_no,cy_phone,rank
                    from workspace.th_family_phone_result a
                    left join business.base_mobile_locale b 
                    on substr(a.cy_phone,1,7) = b.mobilenumber 
                    where a.rank<=3
                    and b.mobiletype <> '中国移动' 
                ) d
            ) a 
            where a.rank = 1
    ) h 
    on d.phone_no=h.phone_no 
    left join
    (
        --匹配运营商
        select * 
        from business.base_mobile_locale
    ) m 
    on substr(d.phone_no,1,7) = m.mobilenumber
  ;



select * from business.dwd_user_location_day limit 20;


select * from workspace.zb_wyd_geo_20191202 where phone_no='1878100qHMg';
select * from workspace.th_family_phone_result where phone_no = '1878100qHMg';
select * from business.base_mobile_locale where mobilenumber='1368961';
select * from business.base_mobile_locale where mobilenumber='1390810';
--
291511
142918
28140

--基站id和geo_code/cell_id
select count(distinct geo_code),count(distinct cell_id),count(*) from business.base_geocode_info  order by geo_code limit 20;

select cell_id,count(cell_name) num from business.base_geocode_info group by cell_id having num>1;
select cell_id,cell_name from business.base_geocode_info where cell_id = '5366371';


select count(distinct geo_code) from business.dwd_user_location_day where dy='2019';

--统计没有找到用户的小区
select * from workspace.zb_lon_lat_20191128 a where a.cell_name not in (
 select distinct cell_name from workspace.zb_wyd_geo_20191202
);

select * from workspace.zb_lon_lat_20191128 a where a.cell_name not in (
 select distinct cell_name from workspace.zb_wyd_20191128_cz_new
);


select count(*) from workspace.zb_wyd_geo_20191202;

select count(distinct cy_phone1),count(distinct cy_phone2),count(distinct cy_phone3)
from
(
    select phone_no,cy_phone1,cy_phone2,cy_phone3,rk 
    from 
    (
        select d.phone_no,d.cy_phone as cy_phone1,row_number() over (partition by phone_no order by rank ) as rk
        ,lead(cy_phone,1) over(partition by d.phone_no order by rank) as cy_phone2
        ,lead(cy_phone,2) over(partition by d.phone_no order by rank) as cy_phone3
        from
        (
            select phone_no,cy_phone,rank
            from workspace.th_family_phone_result a
            left join business.base_mobile_locale b 
            on substr(a.cy_phone,1,7) = b.mobilenumber 
            where a.rank<=3
            and b.mobiletype <> '中国移动' 
        ) d
    ) a 
    where a.rk = 1
   -- and a.phone_no in
    --(
    --    select distinct phone_no 
    --    from zb_wyd_geo_20191202
    --)
) m
;




---检查结果表异网号码数量
select m.*,a+b+c as n
from
(
select count(distinct cy_phone1) a,count(distinct cy_phone2) b,count(distinct cy_phone3) c
from workspace.zb_wyd_geo_20191202
) m 
;

9876
select count(distinct phone_no)
from workspace.zb_wyd_geo_20191202 
where cy_phone1 like '1%' ;
2418
select count(distinct phone_no)
from workspace.zb_wyd_geo_20191202 
where cy_phone2 like '1%';
283
select count(distinct phone_no)
from workspace.zb_wyd_geo_20191202 
where cy_phone3 like '1%' ;


select *
from workspace.zb_wyd_geo_20191202 
where cy_phone2 is not null limit 50;

select phone_no,cy_phone1 
from workspace.zb_wyd_geo_20191202 
where cy_phone1 is not null 
and cy_phone2 is not null  limit 20;
---检查家庭圈异网号码数量
11793
select count(distinct b.cy_phone) 
from
(
    select phone_no,cy_phone 
    from workspace.th_family_phone_result b
    left join business.base_mobile_locale b2 
    on substr(b.cy_phone,1,7) = b2.mobilenumber 
    where b2.mobiletype <> '中国移动'  
    and b.rank <=3
) b
where  b.phone_no in
(
    select distinct phone_no 
    from workspace.zb_wyd_geo_20191202
) 
;

select count(phone_no) as ph_num,phone_no from zb_wyd_geo_20191202 group by phone_no having ph_num>1;

select  * from workspace.th_deyang_alluser_cunzu_fenju_detail_night_2019 where phone_no='1366830Tkdy';

select count(*) from zb_wyd_geo_20191202 where   product_name is not null and county_name is null ;
1077
select count(*) from zb_wyd_geo_20191202 where county_name is null  and fenju is null;
3084
select count(*) from zb_wyd_geo_20191202 where  county_name is null and mobilecity = '德阳';
1091
select count(*) from zb_wyd_geo_20191202 where  county_name is not null and mobilecity = '德阳';
23310
select count(*) from zb_wyd_geo_20191202 where  county_name is not null and mobilecity = '德阳' and product_name is not null;
23216
select count(*) from zb_wyd_geo_20191202 where  county_name is not null and mobilecity <> '德阳';
3179
select count(*) from zb_wyd_geo_20191202 where  county_name is not null and mobilecity <> '德阳' and product_name is not null;
297
select count(*) from zb_wyd_geo_20191202 where  county_name is not null and phone_no is not null and product_name is null;


select count(*) from zb_wyd_geo_20191202 where   product_name is not null and county_name is null ;

select count(*) from zb_wyd_geo_20191202 where county_name is null  and fenju is null and product_name is null;
select * from zb_wyd_geo_20191202 where  phone_no is null;

select * from datamart.dw_user_prc_main_latest where phone_no='1830844GJXP';




------表头
区县,分局,常驻小区,用户号码,用户资费,宽带资费,宽带套餐,主付费/成员,宽带套餐成员数,宽带209,安装地址,是否4G用户,是否合约捆绑,MOU,DOU,ARPU,月均上网天数,使用终端型号,终端最近换机时间,家庭圈异网成员1,家庭圈异网成员2,家庭圈异网成员3,省份,城市,距离(到小区经纬度距离)

select phone_no,product_name,kd_family_name
        ,kd_family_prod,kd_main_phone,kd_cys,durations_m,flow,total_fee,online_days,machine_type,g4_user_flag,imei_chg_ym 
        from datamart.data_dw_user_base_info where dy='2019' and dm='10'








select count(phone_no),count(distinct phone_no) from zb_wyd_geo_20191128;

select cell_name,count(phone_no) from zb_wyd_geo_20191128 group by cell_name;

select * from (
select  quxian ,cell_name ,s_lon ,s_lat ,scb_geo ,geo_code ,juli ,phone_no 
,row_number() over (partition by phone_no order by juli ) num
from (
            select quxian ,cell_name ,s_lon ,a.s_lat ,a.scb_geo ,b.geo_code ,juli ,b.phone_no
            from workspace.zb_keep_geo_distance_20191128 a
            left join 
            (
                select phone_no,geo_code
                from workspace.zb_wyd_20191128_cz
                where geo_num>=7
            ) b on a.geo_code = b.geo_code
    ) a
) a where a.num=1





select count(*) from 
(
            select quxian ,cell_name ,s_lon ,a.s_lat ,a.scb_geo ,b.geo_code ,juli ,b.phone_no
            from workspace.zb_keep_geo_distance_20191128 a
            left join 
            (
                select phone_no,geo_code
                from workspace.zb_wyd_20191128_cz
                where geo_num>=7
            ) b on a.geo_code = b.geo_code
        ) b 





  select phone_no,kd_area_name,num from(
    select phone_no,kd_area_name,dy,dm,row_number() over (partition by phone_no order by dm desc ) num
    from datamart.data_kd_user_t where dy='2019' and dm='07'
) a where a.num=1




select count(*) from (
        select quxian ,cell_name ,s_lon ,a.s_lat ,a.scb_geo ,b.geo_code ,juli ,b.phone_no
        from workspace.zb_keep_geo_distance_20191128 a
        left join workspace.zb_wyd_20191128_cz b on a.geo_code = b.geo_code
         
) a

select count(*) from (
    select distinct phone_no,cy_phone1,cy_phone2,cy_phone3,rank from (
    select phone_no,cy_phone1,cy_phone2,cy_phone3,rank from (
        select d.phone_no,d.cy_phone as cy_phone1,d.rank
        ,lead(cy_phone,1,0) over(partition by d.phone_no order by rank) as cy_phone2
        ,lead(cy_phone,2,0) over(partition by d.phone_no order by rank) as cy_phone3
        from workspace.th_family_phone_result d
        ) a where a.rank = 1
    ) a
    left join business.base_mobile_locale b on substr(a.cy_phone1,1,7) = b.mobilenumber 
    left join business.base_mobile_locale b1 on substr(a.cy_phone2,1,7) = b1.mobilenumber 
    left join business.base_mobile_locale b2 on substr(a.cy_phone3,1,7) = b2.mobilenumber 
    where b.mobiletype <> '中国移动' and b1.mobiletype <> '中国移动' and b2.mobiletype <> '中国移动'
) a
lead

select count(*) from workspace.zb_wyd_geo_20191128 where juli <50;

select count(*) from 
(
        select quxian ,cell_name ,s_lon ,a.s_lat ,a.scb_geo ,b.geo_code ,juli ,b.phone_no
        from workspace.zb_keep_geo_distance_20191128 a
        left join workspace.zb_wyd_20191128_cz b on a.geo_code = b.geo_code
        ) b 


select * from (
select d.phone_no,d.cy_phone as cy_phone1,d.rank
,lead(cy_phone,1,0) over(partition by d.phone_no order by rank) as cy_phone2
,lead(cy_phone,2,0) over(partition by d.phone_no order by rank) as cy_phone3
  from workspace.th_family_phone_result d
  ) a where a.rank = 1
;

  1340814GHSy