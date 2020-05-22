--提取绵竹区域基站过往话单数据

--申请人：	代琴
--申请部门：	绵竹分公司
--数据需求目的：	烦请帮忙提取绵竹区域基站过往话单数据，用于分析Y号客户区域，便于开展外销
--数据筛选条件：	小区id、基站id、移动号码、过往话单号码、近三个月月均通话频率（3次及以上）、单次通话时长（30秒及以上）
--数据提取字段：	小区id、基站id、移动号码、过往话单号码、近三个月月均通话频率（3次及以上）、单次通话时长（30秒及以上）


-- zb_wangluo_user_20200519_changzhu_minute 通过5分钟表，2020 3、4、5月 跑出的基站常驻用户（未剔除重复）

--近两月常驻绵竹
drop table workspace.zb_mianzhu_user_20200521_changzhu;
create table workspace.zb_mianzhu_user_20200521_changzhu
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select distinct a.phone_no,a.cell_id,b.bs_id,b.bs_name,b.county
from 
(
    select phone_no,cell_id
    from 
    (
        select phone_no,cell_id,row_number() over(partition by phone_no order by days desc) rn
        from 
        (
            select phone_no,cell_id,days
            from workspace.lj_dwd_obode_kb_202003
            union all
            select phone_no,cell_id,days
            from workspace.lj_dwd_obode_kb_202004
        ) a
    ) a where rn = 1
) a 
inner join business.base_cell_info b
on a.cell_id = b.cell_id
where b.county = '绵竹'
;

--匹配结果
drop table workspace.zb_mianzhu_user_20200521_result;
create table workspace.zb_mianzhu_user_20200521_result
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select  b.cell_id,b.bs_id,b.bs_name,b.county,a.*
from 
(
    select a.*
    from 
    (
        select a.phone_no,a.opposite_no,avg(a.call_times_month)  as call_times_month_avg
                ,avg(a.call_duration_month) as call_duration_month_avg
        from 
        (
            select a.opposite_no,a.phone_no,sum(a.call_times) as call_times_month,
                    sum(a.call_duration)  as call_duration_month
            from 
            (
                select distinct a.opposite_no,a.phone_no,a.call_times,a.call_duration,a.cell_id,a.dm,b.county,c.mobilecity,c.mobiletype
                from 
                (
                    select distinct opposite_no,phone_no,call_times,call_duration,cell_id,dm
                    from datamart.data_dwb_cal_user_voc_yx_ds
                    where dy = '2020' and (dm = '04' or dm = '03' or dm = '02')
                ) a
                inner join 
                (
                    select * 
                    from business.base_cell_info
                    where county = '绵竹'
                ) b
                on a.cell_id = b.cell_id
                inner join 
                (
                    select *
                    from business.base_mobile_locale
                    where mobilecity = '德阳' and mobiletype <> '中国移动'
                ) c
                on substr(a.opposite_no,1,7) = c.mobilenumber
            ) a
            group by a.phone_no,a.opposite_no,a.dm
        ) a
        group by a.phone_no,a.opposite_no
    ) a
    where a.call_duration_month_avg >= 30 and a.call_times_month_avg >= 3
) a
inner join workspace.zb_mianzhu_user_20200521_changzhu b
on a.phone_no = b.phone_no
;
--剔重复
drop table workspace.zb_mianzhu_user_20200521_result_temp;
create table workspace.zb_mianzhu_user_20200521_result_temp
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select distinct cell_id,bs_id,phone_no,opposite_no,call_times_month_avg,call_duration_month_avg
from workspace.zb_mianzhu_user_20200521_result
where phone_no <> opposite_no
;
--总量 

--检查

select count(*)
from
(
select phone_no,opposite_no,count(phone_no)
from workspace.zb_mianzhu_user_20200521_result_temp
group by phone_no,opposite_no
having count(phone_no) = 2
) a
limit 5;
--在常驻时，两个月常驻基站不同的，已经做了剔除(取两个常驻基站时间最长（待的天数最多的）的基站 作为该用户的常驻基站)   如此 基站不会出现重复
-- 存在一条记录 238464

select cell_id,bs_id,phone_no,opposite_no,call_times_month_avg,call_duration_month_avg
from workspace.zb_mianzhu_user_20200521_result_temp
limit 5;

