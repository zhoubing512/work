--联通过网数据跑数 
--申请人：	吴晓冬
--申请部门：	中江分公司
--数据需求目的：	用于分析异网新增分布
--数据筛选条件：	近6个月LT过网数据
--数据提取字段：	号码、常驻乡镇、月通话频次、网格归属


--电信、联通1/2/3月新增用户
--workspace.th_en_zb_dx_lt_new_user_202001_02_03_2
--电信、联通201912月新增用户
--201912 字段
--first_innet_date	district_id	area_id	cell_id	chat_type	opp_phone_no
drop table workspace.zb_dx_lt_new_user_201912;
create table workspace.zb_dx_lt_new_user_201912(first_innet_date string,district_id string,area_id string,cell_id string,chat_type string,opp_phone_no string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
putdata -f 201912LT_DX.txt -t workspace.zb_dx_lt_new_user_201912;
--加密：th_en_zb_dx_lt_new_user_201912

--电信、联通202004月新增用户
--202004
--city_id	long_code	opp_type	phone_no	new_netin_date	new_netin_day_flag	new_netin_mon_flag	netin_day_flag	netin_mon_flag	netin_days

drop table workspace.zb_dx_lt_new_user_202004;
create table workspace.zb_dx_lt_new_user_202004(city_id string,long_code string,opp_type string,phone_no string,new_netin_date string,new_netin_day_flag string,new_netin_mon_flag string,netin_day_flag string,netin_mon_flag string,netin_days string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
putdata -f 202004LT_DX_new.txt -t workspace.zb_dx_lt_new_user_202004;
--加密：th_en_zb_dx_lt_new_user_202004



--202005 近6个月的通话详情
drop table workspace.zb_deyang_user_202005_detail_voc;
create table workspace.zb_deyang_user_202005_detail_voc
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
            where dy = '2020' or (dy = '2019' and dm = '12')
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


--匹配结果表 详情
--zb_zhongjiang_lt_dx_newuser_result_20200512_temp 没有取排序取一个，可能出现一个手机号每月的常驻不一致
--zb_zhongjiang_lt_dx_newuser_result_20200512  基于19年12月到现在常驻
--zb_zhongjiang_lt_dx_newuser_result_20200512_19all 常驻：包括19年所有和20年的常驻
drop table workspace.zb_zhongjiang_lt_dx_newuser_result_20200512_19all;
create table workspace.zb_zhongjiang_lt_dx_newuser_result_20200512_19all
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  select distinct a.opp_phone_no,a.chat_type,a.phone_no,a.call_duration_month,a.call_times_month,b.county_name
            ,b.area_name,b.town_name,b.village_name,b.grid_name
  from 
  (
    select a.opp_phone_no,a.chat_type,a.phone_no,a.call_times_month,a.call_duration_month
    from 
    (
        select a.opp_phone_no,a.chat_type,a.phone_no,a.call_times_month,a.call_duration_month
                    ,row_number() over(partition by a.opp_phone_no order by call_times_month desc) rn
        from 
        (
            select a.opp_phone_no,a.chat_type,b.phone_no,b.call_times_month,b.call_duration_month
            from 
            (
                    select opp_phone_no,chat_type
                    from th_en_zb_dx_lt_new_user_202001_02_03_2
                    union all
                    select opp_phone_no,chat_type
                    from th_en_zb_dx_lt_new_user_201912
                    union all
                    select phone_no as opp_phone_no,case when opp_type = '1' then 'cdma' when opp_type = '2' then 'union' end as chat_type
                    from th_en_zb_dx_lt_new_user_202004 where new_netin_mon_flag = 1
            ) a
            left join workspace.zb_deyang_user_202005_detail_voc b
            on a.opp_phone_no = b.opposite_no
        ) a
    ) a
    where rn = 1
  ) a
  left join
  (
        select b.*,c.grid_name from
            (
                select *
                from 
                (
                    select phone_no,geo_code,day_sum,county_name,area_name,town_name,village_name,
                    row_number() over(partition by phone_no order by day_sum desc) as rn
                    from 
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
                    ) a
                ) a where rn = 1
            ) b
            left join 
                ( 
                    select * 
                    from business.base_deyang_country_geocode 
                ) c on b.geo_code = c.geo_code
  ) b
  on a.phone_no = b.phone_no
  ;

--中江联通匹配结果
--zb_zhongjiang_lt_dx_newuser_result_20200512_finally 19年12到现在
--zb_zhongjiang_lt_dx_newuser_result_20200512_finally_19all
drop table workspace.zb_zhongjiang_lt_dx_newuser_result_20200512_finally_19all;
create table workspace.zb_zhongjiang_lt_dx_newuser_result_20200512_finally_19all
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select a.opp_phone_no,a.chat_type,a.phone_no,a.call_duration_month,a.call_times_month,a.county_name,a.area_name,a.town_name
        ,a.village_name,a.grid_name,b.call_times_month_04
from 
(
    select *
    from workspace.zb_zhongjiang_lt_dx_newuser_result_20200512_19all
    where chat_type = 'union' and county_name = '中江'
 ) a
left join
(
    select a.opposite_no,sum(a.call_times) as call_times_month_04,
            sum(a.call_duration)  as call_duration_month
    from 
    (
        select distinct a.opposite_no,a.phone_no,a.call_times,a.call_duration,a.cell_id,c.mobilecity,c.mobiletype
        from 
        (
            select distinct opposite_no,phone_no,call_times,call_duration,cell_id
            from datamart.data_dwb_cal_user_voc_yx_ds
            where dy = '2020' and (dm = '04' or dm = '05')
        ) a
        inner join 
        (
            select *
            from business.base_mobile_locale
            where mobilecity = '德阳' and mobiletype <> '中国移动'
        ) c
        on substr(a.opposite_no,1,7) = c.mobilenumber
    ) a
    group by a.opposite_no
) b
on a.opp_phone_no = b.opposite_no
;

--剔除重复数据
drop table workspace.zb_zhongjiang_lt_dx_newuser_result_20200512_finally_2;
create table workspace.zb_zhongjiang_lt_dx_newuser_result_20200512_finally_2
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  select distinct *
  from zb_zhongjiang_lt_dx_newuser_result_20200512_finally
  ;

select opp_phone_no,phone_no,county_name,area_name,town_name
    ,village_name,grid_name,call_times_month_04
from zb_zhongjiang_lt_dx_newuser_result_20200512_finally_2
limit 5;

--检查
select count(distinct opp_phone_no)
from 
(
        select opp_phone_no,chat_type
        from workspace.th_en_zb_dx_lt_new_user_202001_02_03_2
        union all
        select opp_phone_no,chat_type
        from workspace.th_en_zb_dx_lt_new_user_201912
        union all
        select phone_no as opp_phone_no,case when opp_type = '1' then 'cdma' when opp_type = '2' then 'union' end as chat_type
        from workspace.th_en_zb_dx_lt_new_user_202004 where new_netin_mon_flag = 1
) a
where chat_type = 'union'
;



--检验
select sum(union_num_jan),sum(union_num_feb),sum(union_num_mar) from zb_deyang_new_user_2020_01_02_03_result where county_name = '中江'