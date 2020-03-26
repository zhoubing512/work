
drop table workspace.zb_lon_lat_20191128;
create table workspace.zb_lon_lat_20191128(quxian string,cell_name string,s_lon string,s_lat string,zyjuli string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
load data local inpath '/mnt/disk1/user/lj/temp_files/newxiaoqu.txt' into table  workspace.zb_lon_lat_20191128;
------营销活动
drop table workspace.zb_act_20191128;
create table workspace.zb_act_20191128(daima string,act_name string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
load data local inpath '/mnt/disk1/user/lj/temp_files/act.txt' into table  workspace.zb_act_20191128;

--经纬度转化为baidu_geo_code,匹配我们库自己的geo
drop table workspace.zb_keep_geo_20191128;
create table workspace.zb_keep_geo_20191128
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select a.quxian ,a.cell_name ,a.s_lon ,a.s_lat,a.zyjuli
    ,default.geo_Encode(a.s_lon,a.s_lat) as baidu_geo,b.geo_code
  from workspace.zb_lon_lat_20191128 a
  left join business.base_deyang_geocode b
  on default.geo_Encode(s_lon,s_lat) = b.geo_code_bd 
;

select * 
from business.base_deyang_geocode
where geo_code_bd = default.geo_Encode('104.39952','31.140591');
104.398635,31.140494
104.39952,31.140591


-- 经过小区geo_code的所有用户
drop table th_xiaoqu_pass_phone_2019;
create table workspace.th_xiaoqu_pass_phone_2019
    row format delimited fields terminated by '\001' stored as orc tblproperties ('orc.compress'='ZLIB') 
as
select distinct p.phone_no 
from  business.dwd_user_location_5minute p  
where dy=2019 and dm in ('10','11')
and (substring(date_time,12,2) in ('21','22','23','00','01','02','03','04','05','06')) 
and  p.geo_code in 
(
  select geo_code from zb_keep_geo_distance_20191128 where cell_name = '城市花园' and juli<150
);

select phone_no,geo_code,dm,dd
                      from business.dwd_user_location_day m
                      where dy=2019 and dm in ('10','11')
                      --and (substring(date_time,12,2) in ('21','22','23','00','01','02','03','04','05','06'))
                       and m.phone_no in
                       (
                            select phone_no
                            from th_xiaoqu_pass_phone_2019
                       );

------距离1000米的geo_code
drop table workspace.zb_keep_geo_distance_20191128;
create table workspace.zb_keep_geo_distance_20191128
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select quxian ,cell_name ,s_lon ,a.s_lat ,b.geo_code,a.zyjuli 
    ,default.geo_distance(a.geo_code,b.geo_code) as juli
  from workspace.zb_keep_geo_20191128 a ,(select distinct geo_code from business.base_deyang_country_geocode) b
  where  default.geo_distance(a.geo_code,b.geo_code) <= 1000
;


select * from zb_keep_geo_distance_20191128 where cell_name = '城市花园' and juli<150;
select * from zb_keep_geo_20191128 where cell_name = '城市花园';

select count(distinct phone_no)
    from business.dwd_user_location_day a  
    where a.dy='2019' and a.dm='11' and a.time_type in('night')
    and a.geo_code in
    (
      select geo_code from zb_keep_geo_distance_20191128 where cell_name = '城市花园' and juli<150
     );
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
                  where dy='2019' and dm='11' and time_type in('night')
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


--中间表,根据小区的大小，筛选距离小区适当范围的geo_code,
drop table workspace.zb_wyd_20191128_cz_zhongjianbiao;
create table workspace.zb_wyd_20191128_cz_zhongjianbiao 
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  select * from 
  ( 
    select quxian ,cell_name ,s_lon ,a.s_lat ,b.geo_code ,juli ,b.phone_no
    from 
    (
      select *  
      from workspace.zb_keep_geo_distance_20191128
      where juli<=250 and zyjuli>=200 and zyjuli <> 1000
    ) a
    left join 
    (
        select phone_no,geo_code
        from workspace.zb_wyd_20191128_cz
    ) b on a.geo_code = b.geo_code
  ) a

  union all
  select * from 
  (
    select quxian ,cell_name ,s_lon ,a.s_lat ,b.geo_code ,juli ,b.phone_no
    from 
    (
      select * 
      from workspace.zb_keep_geo_distance_20191128
      where juli<=200 and zyjuli>=150 and zyjuli <200
    ) a
    left join 
    (
        select phone_no,geo_code
        from workspace.zb_wyd_20191128_cz
    ) b on a.geo_code = b.geo_code
  ) b

  union all
  select * from 
  (
    select quxian ,cell_name ,s_lon ,a.s_lat ,b.geo_code ,juli ,b.phone_no
    from 
    (
      select *  
      from workspace.zb_keep_geo_distance_20191128
      where juli<=150 and zyjuli>=100 and zyjuli <150
    ) a
    left join 
    (
        select phone_no,geo_code
        from workspace.zb_wyd_20191128_cz
    ) b on a.geo_code = b.geo_code
  ) c

  union all
  select * from 
  (
    select quxian ,cell_name ,s_lon ,a.s_lat ,b.geo_code ,juli ,b.phone_no
    from 
    (
      select *  
      from workspace.zb_keep_geo_distance_20191128
      where juli<=100 and zyjuli <100
    ) a
    left join 
    (
        select phone_no,geo_code
        from workspace.zb_wyd_20191128_cz
    ) b on a.geo_code = b.geo_code
  ) d
  union all
  select * from 
  (
    select quxian ,cell_name ,s_lon ,a.s_lat ,b.geo_code ,juli ,b.phone_no
    from 
    (
      select *  
      from workspace.zb_keep_geo_distance_20191128
      where juli<=1000 and zyjuli =1000
    ) a
    left join 
    (
        select phone_no,geo_code
        from workspace.zb_wyd_20191128_cz
    ) b on a.geo_code = b.geo_code
  ) d
;


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
        select quxian ,cell_name ,s_lon ,b.s_lat ,b.geo_code ,juli ,b.phone_no
        ,d.county_name,d.fenju,c.product_name,c.kd_family_name
        ,c.kd_family_prod,c.kd_main_phone,c.kd_cys,c.durations_m,c.flow,c.total_fee,c.online_days,c.machine_type
        ,c.g4_user_flag,c.imei_chg_ym,d.mobileprovince,d.mobilecity 
        from (
            select * from 
            (
            select  quxian ,cell_name ,s_lon ,s_lat ,geo_code ,juli ,phone_no 
            ,row_number() over (partition by phone_no,cell_name order by juli ) num
            from (
                    --利用中间表匹配字段
                    select * 
                    from workspace.zb_wyd_20191128_cz_zhongjianbiao
                ) a
            ) a where a.num=1
        ) b 
        left join 
        (
            select phone_no,product_name,kd_family_name
            ,kd_family_prod,kd_main_phone,kd_cys,durations_m,flow,total_fee,online_days,machine_type,g4_user_flag,imei_chg_ym 
            from datamart.data_dw_user_base_info 
            where dy='2019' 
            and dm='11'
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



--统计没有找到用户的小区
select * from workspace.zb_lon_lat_20191128 a where a.cell_name not in (
 select distinct cell_name from workspace.zb_wyd_20191128_cz_zhongjianbiao
);
--中间表
zb_wyd_20191128_cz_zhongjianbiao
select count(*) from workspace.zb_wyd_geo_20191202;
--
select * from workspace.zb_lon_lat_20191128 a where a.cell_name not in (
select cell_name 
from(
select * from 
(
select  quxian ,cell_name ,s_lon ,s_lat ,geo_code ,juli ,phone_no 
,row_number() over (partition by phone_no order by juli ) num
from (
        --利用中间表匹配字段
        select * 
        from workspace.zb_wyd_20191128_cz_zhongjianbiao
    ) a
) a where a.num=1
)a
)
--统计每个小区的用户数量
select cell_name,count(cell_name) from workspace.zb_wyd_geo_20191202
group by cell_name;

------表头
区县,分局,常驻小区,用户号码,用户资费,宽带资费,宽带套餐,主付费/成员,宽带套餐成员数,宽带209,安装地址,是否4G用户,是否合约捆绑,MOU,DOU,ARPU,月均上网天数,使用终端型号,终端最近换机时间,家庭圈异网成员1,家庭圈异网成员2,家庭圈异网成员3,省份,城市,距离(到小区经纬度距离)
