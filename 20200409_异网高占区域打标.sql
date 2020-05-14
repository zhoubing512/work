
--3月移动通信用户导入
drop table workspace.zb_cmcc_user_202003;
create table workspace.zb_cmcc_user_202003(phone_no string,open_date string,city_id string,county_id string,area_id string,grid_id string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
putdata -f yidongguimouser.txt -t workspace.zb_cmcc_user_202003;

--加密后：th_en_zb_cmcc_user_202003 
--电信、联通3月用户导入
drop table workspace.zb_dx_lt_user_202003;
create table workspace.zb_dx_lt_user_202003(opp_phone_no string,opp_type string
                ,city_id string,county_id string,area_id string,grid_id string,type string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
putdata -f guowangdxlt.txt -t workspace.zb_dx_lt_user_202003;
--加密：th_en_zb_dx_lt_user_202003

--二、
drop table workspace.zb_dx_lt_user_202003_2;
create table workspace.zb_dx_lt_user_202003_2
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  select * from th_en_zb_dx_lt_user_202003;

--电信、联通1/2/3月新增用户
drop table workspace.zb_dx_lt_new_user_202001_02_03;
create table workspace.zb_dx_lt_new_user_202001_02_03(deal_date string,opp_phone_no string,city_id string,district_id string,area_id string
            ,cell_id string,chat_type string,first_innet_date string,last_innet_date string,new_innet_flag string
            ,month_new_innet_flag string,innet_flag string,month_innet_flag string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
putdata -f new_dx_lt_user_123.txt -t workspace.zb_dx_lt_new_user_202001_02_03;
--加密：th_en_zb_dx_lt_new_user_202001_02_03

--二、
drop table workspace.zb_dx_lt_new_user_202001_02_03_2;
create table workspace.zb_dx_lt_new_user_202001_02_03_2
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  select * from th_en_zb_dx_lt_new_user_202001_02_03;

--移动1、2、3月新增用户
drop table workspace.zb_yd_new_user_202001_02_03;
create table workspace.zb_yd_new_user_202001_02_03(phone_no string,open_date string,city_id string,county_id string,area_id string,grid_id string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
putdata -f yidong_1yue_new.txt -t workspace.zb_yd_new_user_202001_02_03;
--加密：th_en_zb_yd_new_user_202001_02_03

--3月常驻，多留几个geocode
--base_deyang_country_geocode  county_no,area_no 无
--4月 workspace.zb_deyang_user_202004_changzhu
--3月 workspace.zb_deyang_user_202003_changzhu
--2月 workspace.zb_deyang_user_202002_changzhu
--1月 workspace.zb_deyang_user_202001_changzhu
--1912 workspace.zb_deyang_user_201912_changzhu
--1911 workspace.zb_deyang_user_201911_changzhu
--1910 workspace.zb_deyang_user_201910_changzhu
--1909 workspace.zb_deyang_user_201909_changzhu
--1908 workspace.zb_deyang_user_201908_changzhu
--1907 workspace.zb_deyang_user_201907_changzhu
--1906 workspace.zb_deyang_user_201906_changzhu
--1905 workspace.zb_deyang_user_201905_changzhu
--1904 workspace.zb_deyang_user_201904_changzhu
--1903 workspace.zb_deyang_user_201903_changzhu
--1902 workspace.zb_deyang_user_201902_changzhu
--1901 workspace.zb_deyang_user_201901_changzhu


drop table workspace.zb_deyang_user_202004_changzhu;
create table workspace.zb_deyang_user_202004_changzhu
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select a.phone_no,a.geo_code,a.day_sum,b.county_name,b.area_name,b.town_name,b.village_name
from 
(
    select phone_no,count(day_time) as day_sum,geo_code
    from 
    (
        select phone_no,dd,geo_code,time_type,concat(dy,'-',dm,'-',dd) as day_time
        from business.dwd_user_location_day
        where dy = '2020' and dm = '04'
    ) a
    group by phone_no,geo_code,day_time
) a
left join 
( 
    select * 
    from business.base_deyang_country_geocode 
) b on a.geo_code = b.geo_code
;
--匹配3月德阳移动结果，区县、分局、乡镇、村
drop table workspace.zb_deyang_user_202003_detail;
create table workspace.zb_deyang_user_202003_detail
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  select *
  from 
  (
    select a.phone_no,a.county_name,a.area_name,a.town_name,a.village_name,
            row_number() over(partition by phone_no order by day_sum desc) rn
    from 
    (
        select *
        from 
        (
            select a.phone_no,a.county_id,a.area_id,b.county_name,b.area_name,c.day_sum
                ,c.county_name as county_name2,c.area_name as area_name2,c.town_name,c.village_name
            from workspace.th_en_zb_cmcc_user_202003 a
            left join
            (
                select distinct county_id,county_name,area_id,area_name
                from datamart.base_channel_info
                where county_id = '115' or county_id = '129' or county_id = '38' or county_id = '58' or county_id = '99' or county_id = '78'
            )  b on  a.area_id = b.area_id
            left join 
            (
                select *
                from workspace.zb_deyang_user_202004_changzhu
                union all
                select *
                from workspace.zb_deyang_user_202003_changzhu
                union all
                select *
                from workspace.zb_deyang_user_202002_changzhu
                union all
                select *
                from workspace.zb_deyang_user_202001_changzhu
                union all
                select *
                from workspace.zb_deyang_user_201912_changzhu
                union all
                select *
                from workspace.zb_deyang_user_201911_changzhu
                union all
                select *
                from workspace.zb_deyang_user_201910_changzhu
                union all
                select *
                from workspace.zb_deyang_user_201909_changzhu
                union all
                select *
                from workspace.zb_deyang_user_201908_changzhu
                union all
                select *
                from workspace.zb_deyang_user_201907_changzhu
                union all
                select *
                from workspace.zb_deyang_user_201906_changzhu
                union all
                select *
                from workspace.zb_deyang_user_201905_changzhu
                union all
                select *
                from workspace.zb_deyang_user_201904_changzhu
                union all
                select *
                from workspace.zb_deyang_user_201903_changzhu
                union all
                select *
                from workspace.zb_deyang_user_201902_changzhu
                union all
                select *
                from workspace.zb_deyang_user_201901_changzhu
            ) c on a.phone_no = c.phone_no
        ) a where substr(county_name,1,2) = county_name2 and substr(area_name,-4,4) = substr(area_name2,-4,4)
    ) a
  ) a where rn = 1
;
--检验
select count(*)
from
(
    select *
    from workspace.th_en_zb_cmcc_user_202003
    where phone_no not in
    (
    select phone_no
    from workspace.zb_deyang_user_202003_detail
    ) 
) a where a.phone_no in
(
    select phone_no
    from workspace.th_deyang_alluser_cunzu_fenju_detail_night_2019
)
;

select *
from 
(
    select a.phone_no,b.county_name,b.fenju,b.town,b.country_name
    from 
    (
        select *
        from workspace.th_en_zb_cmcc_user_202003
        where phone_no not in
        (
        select phone_no
        from workspace.zb_deyang_user_202003_detail
        ) 
    ) a
    left join workspace.th_deyang_alluser_cunzu_fenju_detail_night_2019 b
    on a.phone_no = b.phone_no
) a
where a.county_name is not null limit 5
;

--未定位的用户，直接用跑出的数据
drop table workspace.zb_deyang_user_202003_detail_part2;
create table workspace.zb_deyang_user_202003_detail_part2
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
      select a.phone_no,b.county_name,b.fenju as area_name,b.town as town_name,b.country_name as village_name
    from 
    (
        select *
        from workspace.th_en_zb_cmcc_user_202003
        where phone_no not in
        (
        select phone_no
        from workspace.zb_deyang_user_202003_detail
        ) 
    ) a
    left join workspace.th_deyang_alluser_cunzu_fenju_detail_night_2019 b
    on a.phone_no = b.phone_no
;
--所有移动用户归属区县、分局、乡镇、村
drop table workspace.zb_deyang_user_202003_detail_all;
create table workspace.zb_deyang_user_202003_detail_all
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  select distinct *
  from 
  (
    select phone_no,county_name,area_name,town_name,village_name from workspace.zb_deyang_user_202003_detail
    union all 
    select * from workspace.zb_deyang_user_202003_detail_part2 where county_name is not null
  ) a
  ;


--过网联通、电信通过话单判断
--分到乡镇、村组前提区县和分局相同
--2020/2019月的通话详情
drop table workspace.zb_deyang_user_202003_detail_voc;
create table workspace.zb_deyang_user_202003_detail_voc
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
    select a.opposite_no,a.phone_no,sum(a.call_times) as call_times_month,
            sum(a.call_duration)  as call_duration_month
    from 
    (
        select distinct a.opposite_no,a.phone_no,a.call_times,a.call_duration,a.cell_id,c.mobilecity,c.mobiletype
        from 
        (
            select distinct opposite_no,phone_no,call_times,call_duration,cell_id
            from datamart.data_dwb_cal_user_voc_yx_ds
            where dy = '2020' or dy = '2019'
        ) a
        inner join 
        (
            select *
            from business.base_mobile_locale
            where mobilecity = '德阳' and mobiletype <> '中国移动'
        ) c
        on substr(a.opposite_no,1,7) = c.mobilenumber
    ) a
    group by a.phone_no,a.opposite_no
    ;

--联通、电信归属3月结果
drop table workspace.zb_deyang_lt_dx_user_202003_detail;
create table workspace.zb_deyang_lt_dx_user_202003_detail
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  select a.opp_phone_no,a.opp_type,a.county_id,a.area_id,a.phone_no,a.call_times_month,a.call_duration_month
        ,b.county_name,b.area_name,b.town_name,b.village_name
  from 
  (
    select *
    from 
    (
        select a.opp_phone_no,a.opp_type,a.county_id,a.area_id,a.phone_no,a.call_times_month,a.call_duration_month
                ,row_number() over(partition by a.opp_phone_no order by call_times_month desc) rn
        from 
        (
            select  a.opp_phone_no,a.opp_type,a.county_id,a.area_id,b.phone_no,b.call_times_month,b.call_duration_month 
            from workspace.th_en_zb_dx_lt_user_202003_2 a
            left join workspace.zb_deyang_user_202003_detail_voc b
            on a.opp_phone_no = b.opposite_no
        ) a
    ) a where rn = 1
  ) a 
  left join workspace.zb_deyang_user_202003_detail_all b
  on a.phone_no = b.phone_no
;

--用户份额（3月）：区县、分局、乡镇、村、移动通信用户规模、电信过网用户规模、联通过网用户规模

--3月移动、联通、电信用户规模结果
drop table workspace.zb_deyang_tongji_user_202003_result;
create table workspace.zb_deyang_tongji_user_202003_result
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
    select county_name,area_name,town_name,village_name
            ,sum(case when opp_type = 'cdma' then sum_all else 0 end) as cdma_num
            ,sum(case when opp_type = 'cmcc' then sum_all else 0 end) as cmcc_num
            ,sum(case when opp_type = 'union' then sum_all else 0 end) as union_num
    from 
    (
            select county_name,area_name,town_name,village_name,opp_type,count(phone_no) as sum_all
            from 
            (
                select substr(county_name,1,2) as county_name,
                case when substr(county_name,1,2) = '中江' then substr(area_name,-4,4)
                when substr(county_name,1,2) = '什邡' then substr(area_name,-4,4)
                when substr(county_name,1,2) = '广汉' and substr(area_name,1,2) = '广汉' then substr(area_name,3)
                when substr(county_name,1,2) = '广汉' and substr(area_name,1,2) <> '广汉' then area_name
                when substr(county_name,1,2) = '旌阳' and substr(area_name,1,2) = '旌阳' then substr(area_name,3)
                when substr(county_name,1,2) = '旌阳' and substr(area_name,1,2) <> '旌阳' then area_name
                when substr(county_name,1,2) = '绵竹' and substr(area_name,1,2) = '绵竹' then substr(area_name,3)
                when substr(county_name,1,2) = '绵竹' and substr(area_name,1,2) <> '绵竹' then area_name
                when substr(county_name,1,2) = '罗江' and substr(area_name,1,2) = '罗江' then substr(area_name,3)
                when substr(county_name,1,2) = '罗江' and substr(area_name,1,2) <> '罗江' then area_name
                end as area_name,town_name,village_name,opp_type,phone_no
                from 
                (
                select phone_no,'cmcc' as opp_type,county_name,area_name,town_name,village_name
                from workspace.zb_deyang_user_202003_detail_all
                union all
                select opp_phone_no as phone_no,opp_type,county_name,area_name,town_name,village_name
                from workspace.zb_deyang_lt_dx_user_202003_detail
                where county_name is not null
                ) a 
            ) a
            group by county_name,area_name,town_name,village_name,opp_type
    ) a group by county_name,area_name,town_name,village_name
;

select county_name,area_name,town_name,village_name,opp_type,cdma_num,cmcc_num,union_num,rn 
from workspace.zb_deyang_tongji_user_202003_result order by county_name,area_name,town_name,village_name,opp_type limit 20;


--移动新增
drop table workspace.zb_deyang_yidong_new_user_2020_detail;
create table workspace.zb_deyang_yidong_new_user_2020_detail
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  select a.phone_no,a.open_date,b.county_name,b.area_name,b.town_name,b.village_name
  from workspace.th_en_zb_yd_new_user_202001_02_03 a
  inner join
  (
      select * 
      from workspace.zb_deyang_user_202003_detail_all
  ) b
  on a.phone_no = b.phone_no
;

--联通、电信新增
drop table workspace.zb_deyang_lt_dx_new_user_2020_detail;
create table workspace.zb_deyang_lt_dx_new_user_2020_detail
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
    select a.opp_phone_no as phone_no,a.deal_date as open_date,a.chat_type as opp_type,b.county_name,b.area_name,b.town_name,b.village_name
    from workspace.th_en_zb_dx_lt_new_user_202001_02_03_2 a
    inner join 
    (
        select * 
        from workspace.zb_deyang_lt_dx_user_202003_detail
    ) b
    on a.opp_phone_no = b.opp_phone_no
;


--temp 联通、电信新增（更全）
drop table workspace.zb_deyang_lt_dx__new_temp_user_202003_detail;
create table workspace.zb_deyang_lt_dx__new_temp_user_202003_detail
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  select a.opp_phone_no,a.deal_date,a.chat_type,a.district_id,a.area_id,a.phone_no,a.call_times_month,a.call_duration_month
        ,b.county_name,b.area_name,b.town_name,b.village_name
  from 
  (
    select *
    from 
    (
        select a.opp_phone_no,a.deal_date,a.chat_type,a.district_id,a.area_id,a.phone_no,a.call_times_month,a.call_duration_month
                ,row_number() over(partition by a.opp_phone_no order by call_times_month desc) rn
        from 
        (
            select  a.opp_phone_no,a.deal_date,a.chat_type,a.district_id,a.area_id,b.phone_no,b.call_times_month,b.call_duration_month 
            from workspace.th_en_zb_dx_lt_new_user_202001_02_03_2 a
            left join workspace.zb_deyang_user_202003_detail_voc b
            on a.opp_phone_no = b.opposite_no
        ) a
    ) a where rn = 1
  ) a 
  left join workspace.zb_deyang_user_202003_detail_all b
  on a.phone_no = b.phone_no
;

--移动，联通，电信 1,2,3月新增

drop table workspace.zb_deyang_new_user_2020_01_02_03_result;
create table workspace.zb_deyang_new_user_2020_01_02_03_result
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
    select county_name,area_name,town_name,village_name
            ,sum(case when opp_type = 'cdma' and dm = '202001' then sum_all else 0 end) as cdma_num_jan
            ,sum(case when opp_type = 'cmcc' and dm = '202001' then sum_all else 0 end) as cmcc_num_jan
            ,sum(case when opp_type = 'union' and dm = '202001' then sum_all else 0 end) as union_num_jan
            ,sum(case when opp_type = 'cdma' and dm = '202002' then sum_all else 0 end) as cdma_num_feb
            ,sum(case when opp_type = 'cmcc' and dm = '202002' then sum_all else 0 end) as cmcc_num_feb
            ,sum(case when opp_type = 'union' and dm = '202002' then sum_all else 0 end) as union_num_feb
            ,sum(case when opp_type = 'cdma' and dm = '202003' then sum_all else 0 end) as cdma_num_mar
            ,sum(case when opp_type = 'cmcc' and dm = '202003' then sum_all else 0 end) as cmcc_num_mar
            ,sum(case when opp_type = 'union' and dm = '202003' then sum_all else 0 end) as union_num_mar
    from 
    (
            select county_name,area_name,town_name,village_name,opp_type,dm,count(phone_no) as sum_all
            from 
            (
                select substr(county_name,1,2) as county_name,
                case when substr(county_name,1,2) = '中江' then substr(area_name,-4,4)
                when substr(county_name,1,2) = '什邡' then substr(area_name,-4,4)
                when substr(county_name,1,2) = '广汉' and substr(area_name,1,2) = '广汉' then substr(area_name,3)
                when substr(county_name,1,2) = '广汉' and substr(area_name,1,2) <> '广汉' then area_name
                when substr(county_name,1,2) = '旌阳' and substr(area_name,1,2) = '旌阳' then substr(area_name,3)
                when substr(county_name,1,2) = '旌阳' and substr(area_name,1,2) <> '旌阳' then area_name
                when substr(county_name,1,2) = '绵竹' and substr(area_name,1,2) = '绵竹' then substr(area_name,3)
                when substr(county_name,1,2) = '绵竹' and substr(area_name,1,2) <> '绵竹' then area_name
                when substr(county_name,1,2) = '罗江' and substr(area_name,1,2) = '罗江' then substr(area_name,3)
                when substr(county_name,1,2) = '罗江' and substr(area_name,1,2) <> '罗江' then area_name
                end as area_name,town_name,village_name,opp_type,phone_no,dm
                from 
                (
                    select phone_no,county_name,area_name,town_name,village_name,'cmcc' as opp_type,substr(open_date,1,6) as dm
                    from workspace.zb_deyang_yidong_new_user_2020_detail
                    union all 
                    select opp_phone_no as phone_no,county_name,area_name,town_name,village_name,chat_type as opp_type,substr(deal_date,1,6) as dm
                    from workspace.zb_deyang_lt_dx__new_temp_user_202003_detail
                    where county_name is not null
                ) a 
            ) a
            group by county_name,area_name,town_name,village_name,opp_type,dm
    ) a group by county_name,area_name,town_name,village_name
;

--检验
select county_name,area_name,town_name,village_name,opp_type,cdma_num_jan,cmcc_num_jan,union_num_jan,
cdma_num_feb,cmcc_num_feb,union_num_feb,cdma_num_mar,cmcc_num_mar,union_num_mar,rn 
from workspace.zb_deyang_new_user_2020_01_02_03_result order by county_name,area_name,town_name,village_name,opp_type limit 20;