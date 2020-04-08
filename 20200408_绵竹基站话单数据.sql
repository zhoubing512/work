--绵竹区域基站过往话单数据

--基站代码、过往话单号码、通话时间（12:00-14:30,18:30-23:00）、通话频率（3次/月及以上）、通话时长（30秒及以上）

--近两月常驻绵竹
drop table workspace.zb_mianzhu_user_20200408_changzhu;
create table workspace.zb_mianzhu_user_20200408_changzhu
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select distinct a.phone_no,a.cell_id,b.bs_id,b.bs_name,b.county
from 
(
    select phone_no,cell_id
    from workspace.lj_dwd_obode_kb_202003
    union all
    select phone_no,cell_id
    from workspace.dwd_obode_kb_202002
) a 
left join business.base_cell_info b
on a.cell_id = b.cell_id
where b.county = '绵竹'
;


--匹配结果
drop table workspace.zb_mianzhu_user_20200408_result;
create table workspace.zb_mianzhu_user_20200408_result
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
                    where dy = '2020' and (dm = '03' or dm = '02' or dm = '01')
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
inner join workspace.zb_mianzhu_user_20200408_changzhu b
on a.phone_no = b.phone_no
;

--检验
select count(*)
from
(
select phone_no,opposite_no,count(phone_no)
from zb_mianzhu_user_20200408_result_tmp
group by phone_no,opposite_no
having count(phone_no) = 1
) a
limit 5;
--存在两条记录  60381
--存在一条记录  
--两个异网联系人及以上 
--一个异网联系人
+--------------+--------------+------+
|   phone_no   | opposite_no  | _c2  |
+--------------+--------------+------+
| 1301819FIcL  | 1301819FIcL  | 2    |
| 1303825zwXt  | 1532869Ykju  | 2    |
| 1303829qUbA  | 1530826lkvQ  | 2    |
| 1315448Ynjy  | 1731157pHcQ  | 2    |
| 1315868RwWQ  | 1809003RNWP  | 2    |
+--------------+--------------+------+



select *
from zb_mianzhu_user_20200408_result_tmp
where phone_no = '1301819FIcL' and opposite_no = '1301819FIcL'
;
select distinct opposite_no,phone_no,call_times,call_duration,cell_id,dm
from datamart.data_dwb_cal_user_voc_yx_ds
where dy = '2020' and dm = '03' and phone_no = '1301819FIcL'
;

6512654

select count(*)
from 
(
select distinct  *
from zb_mianzhu_user_20200408_result_tmp where phone_no <> opposite_no
) a
;
select *
from workspace.zb_mianzhu_user_20200408_result_tmp
where phone_no = '1301819FIcL'


drop table workspace.zb_mianzhu_user_20200408_result_tmp;
create table workspace.zb_mianzhu_user_20200408_result_tmp
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select distinct cell_id,bs_id,phone_no,opposite_no,call_times_month_avg,call_duration_month_avg
from workspace.zb_mianzhu_user_20200408_result
;
323697
323695

select *
from workspace.zb_mianzhu_user_20200408_result_tmp
