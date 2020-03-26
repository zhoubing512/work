
drop table workspace.zb_lon_lat_20191128;
create table workspace.zb_lon_lat_20191128(quxian string,cell_name string,s_lon string,s_lat string,zyjuli string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
load data local inpath '/mnt/disk1/user/lj/temp_files/newxiaoqu.txt' into table  workspace.zb_lon_lat_20191128;

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

--验证geo
select default.geo_Encode(104.680064,31.059048);
726C172813
726C17281C
select default.geo_distance('726C172813','726C17281C');

select default.geo_Encode(104.279998,31.262385);
726C0EC828
726C0E9F49
select default.geo_distance('7266AAF52D','7266ABA259');

select quxian ,cell_name,county_name,town_name,village_name,geo_code_baidu
from workspace.zb_keep_geo_20191128 a
left join business.base_deyang_country_geocode  b
on a.scb_geo=b.geo_code;

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

--select * from business.base_deyang_country_geocode where geo_code ='726C07EFEF';
--select quxian,cell_name,count(geo_code) from zb_keep_geo_distance_20191128
--where juli<=150
-- group by quxian,cell_name;

--中间表,根据小区的大小，筛选距离小区适当范围的geo_code,
drop table workspace.zb_wyd__zhongjianbiao;
create table workspace.zb_wyd__zhongjianbiao 
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
    select quxian ,cell_name ,s_lon ,a.s_lat ,a.geo_code ,juli 
    from 
    (
      select *  
      from workspace.zb_keep_geo_distance_20191128
      where juli<=250 and zyjuli>=200 and zyjuli <>1000
    ) a

  union all

    select quxian ,cell_name ,s_lon ,a.s_lat ,a.geo_code ,juli 
    from 
    (
      select * 
      from workspace.zb_keep_geo_distance_20191128
      where juli<=200 and zyjuli>=150 and zyjuli <200
    ) a

  union all
    select quxian ,cell_name ,s_lon ,a.s_lat ,a.geo_code ,juli 
    from 
    (
      select *  
      from workspace.zb_keep_geo_distance_20191128
      where juli<=150 and zyjuli>=100 and zyjuli <150
    ) a

  union all
    select quxian ,cell_name ,s_lon ,a.s_lat ,a.geo_code ,juli 
    from 
    (
      select *  
      from workspace.zb_keep_geo_distance_20191128
      where juli<=100 and zyjuli <100
    ) a

    union all
    select quxian ,cell_name ,s_lon ,a.s_lat ,a.geo_code ,juli 
    from 
    (
      select *  
      from workspace.zb_keep_geo_distance_20191128
      where juli<=1000 and zyjuli = 1000
    ) a
;
-- 经过小区geo_code的所有用户
drop table th_xiaoqu_pass_phone_2019;
create table workspace.th_xiaoqu_pass_phone_2019
    row format delimited fields terminated by '\001' stored as orc tblproperties ('orc.compress'='ZLIB') 
as
select distinct p.phone_no 
from  business.dwd_user_location_5minute p  
where dy=2019 and dm in ('11')
--and (substring(date_time,12,2) in ('21','22','23','00','01','02','03','04','05','06')) 
and  p.geo_code in 
(
    select geo_code from workspace.zb_wyd__zhongjianbiao

);

select count(distinct phone_no) from th_xiaoqu_pass_phone_2019;

--select count(distinct geo_code),count(*) from business.dwd_user_location_5minute p
--where p.geo_code in (select g.scb_geo from workspace.zb_keep_geo_20191128  g);

--17   899794004

--所有经过小区的晚上时间出现过的人，取最多的前9条

th_xiaoqu_phone_rank9 原表 11月
20200110 新跑数  th_xiaoqu_phone_rank9_20200110

drop table th_xiaoqu_phone_rank9_20200110;
create table workspace.th_xiaoqu_phone_rank9_20200110
    row format delimited fields terminated by '\001' stored as orc tblproperties ('orc.compress'='ZLIB') 
as
select c.dm,c.dd,c.phone_no,time_type,c.geo_code,c.stays,c.rank from
(
  select f.* from
  (
    select e.*,row_number() over(distribute by e.dm,e.dd,e.phone_no,time_type sort by stays desc) rank from
    (
        select a.dm,a.dd,a.phone_no,time_type,a.geo_code,count(distinct a.date_time) as stays
        from
        (
            select n.phone_no,n.geo_code,n.date_time,n.dm,n.dd,
            case when int(substring(date_time,12,2))>=7 and int(substring(date_time,12,2))<=20 then 'day'
            else 'night' end as time_type
            from
                 (
                      select phone_no,geo_code,date_time,dm,dd
                      from business.dwd_user_location_5minute m
                      where dy=2019 and dm = '12'
                      --and (substring(date_time,12,2) in ('21','22','23','00','01','02','03','04','05','06'))
                       and m.phone_no in
                       (
                            select phone_no from th_xiaoqu_pass_phone_2019
                       )
                 ) n 

        ) a
        group by a.dm,a.dd,a.phone_no,a.geo_code,time_type
    ) e
  ) f
  where f.rank<9
) c;

---
select count(distinct cell_name)
from th_xiaoqu_phone_rank9 a
left join 
(
    select geo_code,cell_name from workspace.zb_wyd__zhongjianbiao
) b
on a.geo_code = b.geo_code;

select distinct cell_name from zb_keep_geo_distance_20191128;

--150米 54个 42个无
--200米 72个 24个无
--250米 84个
th_xiaoqu_zhongjian_juli  原表 
20200110 新跑数  th_xiaoqu_zhongjian_juli_20200110
--中间表计算距离
drop table th_xiaoqu_zhongjian_juli_20200110;
create table workspace.th_xiaoqu_zhongjian_juli_20200110
    row format delimited fields terminated by '\001' stored as orc tblproperties ('orc.compress'='ZLIB') 
as
select m.dm,m.dd,m.phone_no,time_type,geo_code,p_long from
(
select n.* from
(
    select d.*,row_number() over(distribute by d.dm,d.dd,d.phone_no,time_type sort by stays desc) rank from
    (
      select c.* from
      (
        select a.dm,a.dd,a.phone_no,a.time_type,a.geo_code,a.stays,default.geo_distance(a.geo_code,b.geo_code) as p_long 
        from th_xiaoqu_phone_rank9_20200110 a
        left join 
        (
            select dm,dd,phone_no,time_type,geo_code from th_xiaoqu_phone_rank9_20200110
            where rank=1
        )b
        on a.dm=b.dm and a.dd=b.dd and a.phone_no=b.phone_no and a.time_type=b.time_type
      ) c 
      where c.p_long<100
    ) d
  ) n
  where n.rank<9
)m;

--晚上小区常驻人员中间表1
th_xiaoqu_zhongjianbiao1  原表
20200110 新跑数  th_xiaoqu_zhongjianbiao1_20200110
drop table th_xiaoqu_zhongjianbiao1_20200110;
create table workspace.th_xiaoqu_zhongjianbiao1_20200110
    row format delimited fields terminated by '\001' stored as orc tblproperties ('orc.compress'='ZLIB') 
as
select dm,dd,phone_no,time_type,cell_name,quxian,day_xiaoqu_num
from
(
    select n.*,row_number()over(distribute by n.phone_no,dm,dd,cell_name,time_type sort by day_xiaoqu_num desc) as rank 
    from
    (
        select dm,dd,phone_no,time_type,cell_name,quxian,count(geo_code) as day_xiaoqu_num
        from
        (
            select a.dm,a.dd,a.phone_no,a.time_type,a.geo_code,p_long,b.quxian,b.cell_name
            from th_xiaoqu_zhongjian_juli_20200110 a
            left join 
            (    
                select geo_code,quxian,cell_name from workspace.zb_wyd__zhongjianbiao
            ) b
            on a.geo_code=b.geo_code
            where b.geo_code is not null
        ) m
        group by dm,dd,phone_no,time_type,cell_name,quxian
    ) n
) p
where p.rank = 1;


--统计没有找到用户的小区
select distinct a.cell_name,quxian from workspace.zb_lon_lat_20191128 a where a.cell_name not in (
 select distinct cell_name from workspace.th_xiaoqu_zhongjianbiao1_20200110
);

select count(*) from workspace.th_xiaoqu_zhongjianbiao1;
--晚上小区常驻人员中间表2
th_xiaoqu_zhongjianbiao2  原表
20200110 新跑数 th_xiaoqu_zhongjianbiao2_20200110
drop table th_xiaoqu_zhongjianbiao2_20200110;
create table workspace.th_xiaoqu_zhongjianbiao2_20200110
    row format delimited fields terminated by '\001' stored as orc tblproperties ('orc.compress'='ZLIB') 
as
select distinct e.phone_no,e.quxian,e.cell_name
from 
(
    select d.phone_no,d.quxian,d.cell_name,d.sum_day,d.sum_month,d.jq_days
    from
    (
        select  c.phone_no,c.time_type,c.quxian,c.cell_name,c.sum_day,c.sum_month,c.jq_days,row_number()over(distribute by c.phone_no sort by sum_day desc,sum_month desc) as rank 
        from
        (
            select m.phone_no,m.time_type,m.quxian,m.cell_name,sum(m.days) as sum_day,count(m.dm) as sum_month,concat(count(m.dm),sum(m.days)) as jq_days
            from
            (
                select dm,phone_no,time_type,quxian,cell_name,count(cell_name) as days
                from workspace.th_xiaoqu_zhongjianbiao1_20200110 
                group by dm,phone_no,time_type,cell_name,quxian
             )m
            group by m.phone_no,m.time_type,m.cell_name,m.quxian
        )c
    ) d
    where d.rank=1
    and d.sum_day>4
) e ;
--常驻用户表2
select count(*) from workspace.th_xiaoqu_zhongjianbiao2;

-----data_kd_user_t 最终结果

zb_wyd_geo_20191218  原表
20200110 新跑数 zb_wyd_geo_20200110
drop table workspace.zb_wyd_geo_20200110;
create table workspace.zb_wyd_geo_20200110
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
    select distinct d.county_name,d.fenju,d.cell_name,d.phone_no,d.product_name
    ,d.kd_family_name,d.kd_family_prod,d.kd_main_phone,d.kd_cys,e.kd_no1,g.kd_area_name,d.g4_user_flag
    ,case when f.phone_no is not null then '1' else '0' end as shifouheyue
    ,d.durations_m,d.flow,d.total_fee,d.online_days,d.machine_type,d.imei_chg_ym,h.cy_phone1,h.cy_phone2,h.cy_phone3
    ,m.mobileprovince,m.mobilecity
    from 
    (
        select quxian ,cell_name ,b.phone_no
        ,d.county_name,d.fenju,c.product_name,c.kd_family_name
        ,c.kd_family_prod,c.kd_main_phone,c.kd_cys,c.durations_m,c.flow,c.total_fee,c.online_days,c.machine_type
        ,c.g4_user_flag,c.imei_chg_ym,d.mobileprovince,d.mobilecity 
        from (
                select * from workspace.th_xiaoqu_zhongjianbiao2_20200110
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
        and dm='11'
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
 select distinct cell_name from workspace.zb_wyd_geo_20191202
);

select * from workspace.zb_wyd_geo_20191202 where cell_name = '德阳旌阳区保利国际海德花园';
select * from workspace.zb_wyd_geo_20191202 where cell_name = '德阳旌阳希望城玫瑰湾';
select * from workspace.zb_wyd_geo_20191202 where cell_name = '德阳旌阳天悦湾' and phone_no = '1398100GNvQ';

select * from workspace.th_xiaoqu_zhongjian_juli where phone_no = '1398100GNvQ';
th_xiaoqu_phone_rank9
th_xiaoqu_zhongjianbiao1
th_xiaoqu_zhongjian_juli
select count(*) from workspace.zb_wyd_geo_20191202;

select cell_name,count(cell_name) 
from workspace.zb_wyd_geo_20191218
group by cell_name
;

select count(phone_no),count(distinct phone_no) 
from workspace.zb_wyd_geo_20191202
;

--安装宽带地址跑用户

--德阳旌阳碧桂园1期电梯公寓 3
--水岸皇城  1
--德阳旌阳蓝兴园小区  23
--金岸湖畔  22
--中江金银山安置房
安国新村安置房
--德阳什邡市盛世豪庭
德阳广汉益好佳家属区
--德阳广汉那维亚半岛小区
德阳旌阳东电单身公寓
德阳旌阳九维蓝谷生活区
--德阳旌阳碧桂园2期电梯公寓
--东方星座小区
中江于嘉禾小区
德阳旌阳迅果电梯公司宿舍
中江杉春家园
--中江水岸上城
德阳什邡英伦郡
--德阳广汉东嘉苑增补
德阳广汉市小汉镇南湖馨苑
--德阳广汉紫荆苑
--德阳旌阳万嘉国际3期
--德阳旌阳城市花园
--德阳旌阳建馨家园1区
--德阳旌阳水岸花都3期
德阳旌阳金色维也纳一期
--金陵苑背后聚集区
中江城南一号
--德阳广汉恒昌金域蓝湾
--德阳广汉美景嘉园
--德阳绵竹孝德姑苏苑西区 
--欧洲半岛
豪景苑   无
--迎祥广场小区


drop table workspace.zb_zhongjian_20191218;
create table workspace.zb_zhongjian_20191218
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select distinct case when b.cell_name is not null then b.quxian else '旌阳' end as quxian,a.phone_no,a.kd_area_name
,case when b.cell_name is not null then a.cell_name else '德阳旌阳碧桂园电梯公寓' end  as cell_name
from
(
  select phone_no,kd_area_name,'迎祥广场小区' as cell_name
  from datamart.data_kd_user_t
  where kd_area_name like '%迎祥广场%'
  union all
  select phone_no,kd_area_name,'欧洲半岛' as cell_name
  from datamart.data_kd_user_t 
  where kd_area_name like '罗江县万安镇新开路欧洲半岛'
  union all
  select phone_no,kd_area_name,'德阳绵竹孝德姑苏苑西区' as cell_name 
  from datamart.data_kd_user_t
  where kd_area_name like '%姑苏苑西区%'
  union all
  select phone_no,kd_area_name,'德阳广汉美景嘉园' as cell_name 
  from datamart.data_kd_user_t
  where kd_area_name like '%美景嘉园%'
  union all
  select phone_no,kd_area_name,'德阳广汉恒昌金域蓝湾' as cell_name 
  from datamart.data_kd_user_t
  where kd_area_name like '%金域蓝湾%'
  union all
  select phone_no,kd_area_name,'金陵苑背后聚集区' as cell_name 
  from datamart.data_kd_user_t
  where kd_area_name like '%金陵苑小区%'
  union all
  select phone_no,kd_area_name,'德阳旌阳水岸花都3期' as cell_name 
  from datamart.data_kd_user_t
  where kd_area_name like '%水岸花都%'
  union all
  select phone_no,kd_area_name,'德阳旌阳建馨家园1区' as cell_name 
  from datamart.data_kd_user_t
  where kd_area_name like '%建馨家园一%'
  union all
  select phone_no,kd_area_name,'德阳旌阳城市花园' as cell_name 
  from datamart.data_kd_user_t
  where kd_area_name like '%旌阳%城市花园%'
  union all
  select phone_no,kd_area_name,'德阳旌阳万嘉国际3期' as cell_name 
  from datamart.data_kd_user_t
  where kd_area_name like '%万嘉国际%'
  union all
  select phone_no,kd_area_name,'德阳广汉紫荆苑' as cell_name 
  from datamart.data_kd_user_t
  where kd_area_name like '%广汉%保定路%紫荆苑%'
  union all
  select phone_no,kd_area_name,'德阳广汉东嘉苑增补' as cell_name 
  from datamart.data_kd_user_t
  where kd_area_name like '%东嘉苑%'
  union all
  select phone_no,kd_area_name,'中江水岸上城' as cell_name 
  from datamart.data_kd_user_t
  where kd_area_name like '%中江%水岸上城%'
  union all
  select phone_no,kd_area_name,'东方星座小区' as cell_name 
  from datamart.data_kd_user_t
  where kd_area_name like '%绵竹%东方星座%'
  union all
  select phone_no,kd_area_name,'德阳旌阳碧桂园电梯公寓' as cell_name 
  from datamart.data_kd_user_t
  where kd_area_name like '%旌阳%碧桂园%'
  union all
  select phone_no,kd_area_name,'德阳广汉那维亚半岛小区' as cell_name 
  from datamart.data_kd_user_t
  where kd_area_name like '%广汉%那维亚半岛%'
  union all
  select phone_no,kd_area_name,'德阳什邡市盛世豪庭' as cell_name 
  from datamart.data_kd_user_t
  where kd_area_name like '%什邡%盛世豪庭%'
  union all
  select phone_no,kd_area_name,'中江金银山安置房' as cell_name 
  from datamart.data_kd_user_t
  where kd_area_name like '%金银山%'
  union all
  select phone_no,kd_area_name,'金岸湖畔' as cell_name 
  from datamart.data_kd_user_t
  where kd_area_name like '%金岸湖畔%'
  union all
  select phone_no,kd_area_name,'德阳旌阳蓝兴园小区' as cell_name 
  from datamart.data_kd_user_t
  where kd_area_name like '%旌阳%蓝兴园%'
  union all
  select phone_no,kd_area_name,'水岸皇城' as cell_name 
  from datamart.data_kd_user_t
  where kd_area_name like '%罗江%水岸皇城%'
) a
left join  workspace.zb_lon_lat_20191128 b
on a.cell_name = b.cell_name
;


select distinct phone_no,kd_area_name 
from data_kd_user_t
where kd_area_name like '%迎祥广场%'
;
--307 地址，绵竹市迎祥广场西巷居民点，绵竹市迎祥广场西巷31号小区，绵竹市迎祥广场西巷29号，绵竹市迎祥广场西巷33号小区
select distinct phone_no,kd_area_name
from data_kd_user_t 
where kd_area_name like '罗江县万安镇新开路欧洲半岛'
; 
--391
select distinct phone_no,kd_area_name 
from data_kd_user_t
where kd_area_name like '%姑苏苑西区%'
; 
--225
select * 
from data_kd_user_t
where kd_area_name like '%美景嘉园%'
; 
--671
select * 
from data_kd_user_t
where kd_area_name like '%金域蓝湾%'
; 
--290
select * 
from data_kd_user_t
where kd_area_name like '%金陵苑小区%'
;
--3716 地址，绵竹市金陵苑小区，绵竹市金陵苑外民房
select * 
from data_kd_user_t
where kd_area_name like '%水岸花都%'
;
--3818 124期有，3期无  地址，旌阳区嵩山路33号水岸花都(爱家)，旌阳区蒙山街水岸花都四期，旌阳区嵩山街33号水岸花都1期，旌阳区嵩山街100号水岸花都2期
select * 
from data_kd_user_t
where kd_area_name like '%建馨家园一%'
;
--753
select * 
from data_kd_user_t
where kd_area_name like '%旌阳%城市花园%'
;
--2003
select * 
from data_kd_user_t
where kd_area_name like '%万嘉国际%'
--where kd_area_name = '旌阳区泰山南路三段247号万嘉国际社区'
;
--524 搜3期，无数据，地址，旌阳区泰山南路三段247号万嘉国际社区
select * 
from data_kd_user_t
where kd_area_name like '%广汉%保定路%紫荆苑%'
;
--1631  地址，广汉市保定路西二段9号紫荆苑(爱家)，广汉市保定路西二段128号紫荆苑小区
select * 
from data_kd_user_t
where kd_area_name like '%东嘉苑%'
;
--352
select * 
from data_kd_user_t
where kd_area_name like '%中江%水岸上城%'
;
--4018 地址，中江县北塔西路36号水岸上城，中江县北塔西路36号水岸上城一期，中江县水岸上城（爱家）,中江县北塔西路36号水岸上城1期(爱家)

select * 
from data_kd_user_t
where kd_area_name like '%绵竹%东方星座%'
;
--1044
select * 
from data_kd_user_t
where kd_area_name like '%旌阳%碧桂园%'
;
--4021  地址，旌阳区东一环路碧桂园小区,旌阳区一环路碧桂园小区1期B区
select *
from data_kd_user_t
where kd_area_name like '%广汉%那维亚半岛%'
;
--4159 地址，广汉市雒城镇深圳路东二段88号那维亚半岛，广汉市那维亚半岛，广汉市深圳路那维亚半岛
select *
from data_kd_user_t
where kd_area_name like '%什邡%盛世豪庭%'
;
--796
select *
from data_kd_user_t
where kd_area_name like '%金银山%'
;
--1366 地址，中江县南华镇金银山村5组，中江县南华镇金银山村，中江县南华镇金银山村7组
select *
from data_kd_user_t
where kd_area_name like '%金岸湖畔%'
;
--3490 地址，罗江县金岸湖畔小区，罗江县金岸湖畔(爱家)
select *
from data_kd_user_t
where kd_area_name like '%旌阳%蓝兴园%'
;
--677 地址，旌阳区沱江东路3号蓝兴园小区，旌阳区沱江东路3号蓝兴园小区(爱家)
select *
from data_kd_user_t
where kd_area_name like '%罗江%水岸皇城%'
;
--2129 地址，罗江县万安镇雨春东路南段388号水岸皇城小区，罗江县水岸皇城小区(爱家)，罗江县万安镇雨村东路388号水岸皇城二期罗江县万安镇雨春东路南段388号水岸皇城小区商铺
select distinct kd_area_name
from data_kd_user_t
where kd_area_name like '%德阳旌阳碧桂园1期电梯公寓 3%'
;
--德阳旌阳碧桂园1期电梯公寓 3
