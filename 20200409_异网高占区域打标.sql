
--3月移动通信用户导入
drop table workspace.zb_cmcc_user_202003;
create table workspace.zb_cmcc_user_202003(phone_no string,open_date string,city_id string,county_id string,area_id string,grid_id string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
putdata -f yidongguimouser.txt -t workspace.zb_cmcc_user_202003;

--电信、联通3月用户导入
drop table workspace.zb_dx_lt_user_202003;
create table workspace.zb_dx_lt_user_202003(opp_phone_no string,opp_type string
                ,city_id string,county_id string,area_id string,grid_id string,type string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
putdata -f guowangdxlt.txt -t workspace.zb_dx_lt_user_202003;

--电信、联通1/2/3月新增用户
drop table workspace.zb_dx_lt_new_user_202001_02_03;
create table workspace.zb_dx_lt_new_user_202001_02_03(deal_date string,opp_phone_no string,city_id string,district_id string,area_id string
            ,cell_id string,chat_type string,first_innet_date string,last_innet_date string,new_innet_flag string
            ,month_new_innet_flag string,innet_flag string,month_innet_flag string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
putdata -f new_dx_lt_user_123.txt -t workspace.zb_dx_lt_new_user_202001_02_03;

--3月常驻，多留几个geocode
--base_deyang_country_geocode  county_no,area_no 无
--3月 workspace.zb_deyang_user_202003_changzhu
--2月 workspace.zb_deyang_user_202002_changzhu
--1月 workspace.zb_deyang_user_202001_changzhu
drop table workspace.zb_deyang_user_202001_changzhu;
create table workspace.zb_deyang_user_202001_changzhu
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
        where dy = '2020' and dm = '01'
    ) a
    group by phone_no,geo_code,day_time
) a
left join 
( 
    select * 
    from business.base_deyang_country_geocode 
) b on a.geo_code = b.geo_code
;
--匹配3月德阳移动，区县、分局、乡镇、村
drop table workspace.zb_deyang_user_202003_detail;
create table workspace.zb_deyang_user_202003_detail
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  select a.phone_no,a.county_name,a.area_name,a.town_name,a.village_name,
    
  from 
  (
    select *
    from 
    (
        select a.phone_no,a.county_id,a.area_id,b.county_name,b.area_name,c.day_sum
            ,c.county_name as county_name2,c.area_name as area_name2,c.town_name,c.village_name
        from workspace.zb_cmcc_user_202003 a
        left join
        (
            select distinct county_id,county_name,area_id,area_name
            from datamart.base_channel_info
            where county_id = '115' or county_id = '129' or county_id = '38' or county_id = '58' or county_id = '99' or county_id = '78'
        )  b on  a.area_id = b.area_id
        left join 
        (
            select *
            from workspace.zb_deyang_user_202003_changzhu
        ) c on a.phone_no = c.phone_no
    ) a where substr(county_name,1,2) = county_name2 and area_name = area_name2
  ) a
;

--临时表
drop table workspace.zb_deyang_user_202003_detail_tmp;
create table workspace.zb_deyang_user_202003_detail_tmp
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
          select a.phone_no,a.county_id,a.area_id,b.county_name,b.area_name,c.day_sum
            ,c.county_name as county_name2,c.area_name as area_name2,c.town_name,c.village_name
        from workspace.zb_cmcc_user_202003 a
        left join
        (
            select distinct county_id,county_name,area_id,area_name
            from datamart.base_channel_info
            where county_id = '115' or county_id = '129' or county_id = '38' or county_id = '58' or county_id = '99' or county_id = '78'
        )  b on  a.area_id = b.area_id
        left join 
        (
            select *
            from workspace.zb_deyang_user_202003_changzhu
        ) c on a.phone_no = c.phone_no
        ;


--过网联通、电信通过话单判断
--分到乡镇、村组前提区县和分局相同
--1-3月的通话详情
drop table workspace.zb_deyang_user_202003_detail_voc;
create table workspace.zb_deyang_user_202003_detail_voc
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
    select a.opposite_no,a.phone_no,a.dm,sum(a.call_times) as call_times_month,
            sum(a.call_duration)  as call_duration_month
    from 
    (
        select distinct a.opposite_no,a.phone_no,a.call_times,a.call_duration,a.cell_id,a.dm,c.mobilecity,c.mobiletype
        from 
        (
            select distinct opposite_no,phone_no,call_times,call_duration,cell_id,dm
            from datamart.data_dwb_cal_user_voc_yx_ds
            where dy = '2020' and (dm = '03' or dm = '02' or dm = '01')
        ) a
        inner join 
        (
            select *
            from business.base_mobile_locale
            where mobilecity = '德阳' and mobiletype <> '中国移动'
        ) c
        on substr(a.opposite_no,1,7) = c.mobilenumber
    ) a
    group by a.phone_no,a.opposite_no,a.dm
    ;
