--数据需求目的：	用于宣传短信发送
--数据筛选条件：	基站小区常驻用户号码
--数据提取字段：	需要提取附件内涉及所有基站小区的常驻用户号码（五一节具有特殊性，请避开）

--导入数据
drop table workspace.zb_networks_cell_id_info_20200511;
create table workspace.zb_networks_cell_id_info_20200511(cell_id string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
putdata -f 20200511_cell_id.txt -t workspace.zb_networks_cell_id_info_20200511;

--常驻用户
drop table workspace.zb_near_month_20200511_cz;
create table workspace.zb_near_month_20200511_cz 
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as 
  select *from (
    select dd.phone_no,dd.ci,dd.ci_num,row_number() over (partition by phone_no order by ci_num desc ) num  from (
      select bb.phone_no,bb.ci,count(ci) ci_num from (
        select * from (
          select phone_no,ci,time_type,duration,dd,row_number() over (partition by phone_no,dd order by duration desc ) num  
          from business.dwd_user_location_day  where dy='2020' and time_type = 'sf' and ((dm='04' and (dd <> '29' or dd <> '30')) 
          or (dm='05' and (dd <> '01' or dd <> '02' or dd <> '03' or dd <> '04' or dd <> '05')))
          ) cc
        where cc.num = 1
        ) bb
      group by phone_no,ci
      ) dd
    ) as ff
  where ff.num=1 and ff.phone_no not like '10%' and ff.phone_no like '1%' 
  ;

--5分钟表跑常驻 路过不算，每天至少两条数据，一个小时出现1次或多次算 1小时;
--晚上 zb_dy_user_202004_permanent_minute
--全天 zb_dy_user_202004_permanent_minute_allday 
drop table workspace.zb_dy_user_202004_permanent_minute_allday;
create table workspace.zb_dy_user_202004_permanent_minute_allday
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select phone_no,cell_id,count(data_date) as appear_times
from
(
    select cell_id,phone_no,data_date
    from 
    (
        select cell_id,phone_no,data_date,row_number() over(partition by phone_no,data_date order by stay_hour desc) as rn
        from
        (
            select cell_id,phone_no,data_date,count(distinct data_hour) as stay_hour
            from 
            (
                select distinct cell_id,phone_no,substr(date_time,1,10) as data_date,substr(date_time,12,2) as data_hour
                from business.dwd_user_location_5minute
                where dy = '2020' and dm = '04' --and substr(date_time,12,2) in ('22','23','00','01','02','03','04','05','06','07')
            ) a
            group by cell_id,phone_no,data_date
            having count(distinct data_hour) > 1
        ) a
    ) a
    where a.rn = 1
) a
group by phone_no,cell_id
;




--小时表  常驻用户
drop table workspace.zb_dy_user_202004_permanent_hour_night;
create table workspace.zb_dy_user_202004_permanent_hour_night
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select phone_no,ci,count(data_date) as appear_times
from
(
    select ci,phone_no,data_date
    from 
    (
        select ci,phone_no,data_date,row_number() over(partition by phone_no,data_date order by stay_time desc) as rn
        from
        (
            select ci,phone_no,data_date,sum(duration) as stay_time
            from 
            (
                select distinct ci,phone_no,duration,substr(date_time,1,10) as data_date,substr(date_time,12,2) as data_hour
                from business.dwd_user_location_hour
                where dy = '2020' and dm = '04' and substr(date_time,12,2) in ('22','23','00','01','02','03','04','05','06','07')
            ) a
            group by ci,phone_no,data_date
        ) a
    ) a
    where a.rn = 1
) a
group by phone_no,ci
;


--匹配结果
drop table workspace.zb_near_month_20200511_cz_result;
create table workspace.zb_near_month_20200511_cz_result 
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as 
    select distinct a.cell_id,b.phone_no
    from workspace.zb_networks_cell_id_info_20200511 a
    left join (
        select *
        from workspace.zb_dy_user_202004_permanent_minute_allday a
        where a.appear_times >= 20
    ) b
    on a.cell_id = b.cell_id
    ;

--小时表，常驻，匹配结果
drop table workspace.zb_near_month_20200511_cz_result_hour;
create table workspace.zb_near_month_20200511_cz_result_hour 
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as 
    select distinct a.cell_id,b.phone_no
    from workspace.zb_networks_cell_id_info_20200511 a
    left join (
        select *
        from workspace.zb_dy_user_202004_permanent_hour_night a
        where a.appear_times >= 15
    ) b
    on a.cell_id = b.ci
    ;

--检查
select count(*) from zb_near_month_20200511_cz_result limit 5;
select count(*) from zb_near_month_20200511_cz_result_hour limit 5;


select count(distinct cell_id) from zb_near_month_20200511_cz_result where phone_no is null limit 5;
select count(distinct cell_id) from zb_near_month_20200511_cz_result_hour where phone_no is null limit 5;


select cell_id from zb_near_month_20200511_cz_result where phone_no is null limit 20;


--未找到用户的cell_id
+------------+
|  cell_id   |
+------------+
| 31183665   |
| 387161132  |
| 53695019   |
| 758391131  |
| 758925129  |
| 788039193  |
| 792454193  |
| 758642130  |
| 758780130  |
| 792454192  |
| 9699545    |
| 31204766   |
| 32402468   |
| 387053130  |
| 3922233    |
| 392341131  |
| 392430131  |
| 5259372    |
| 5365981    |
| 540894192  |
+------------+




select phone_no,cell_id,geo_code,date_time,cnt,dy,dm,dd
from business.dwd_user_location_5minute
where dy = '2020' and dm = '04' and cell_id = '758752133'
order by phone_no,dd
limit 50;

386948129
select phone_no,cell_id,geo_code,date_time,cnt,dy,dm,dd
from business.dwd_user_location_5minute
where dy = '2020' and dm = '04' and dd = '28' and phone_no = '1345898FkEP'
order by phone_no,dd,cell_id
limit 50;
1345898FkEP


select count(distinct phone_no)
from business.dwd_user_location_5minute
where dy = '2020' and dm = '04' and dd = '28'
limit 50;

select count(distinct phone_no)
from business.dwd_user_location_5minute
where dy = '2020' and dm = '04' and dd = '28' and cell_id = '32402468'
limit 50;

select count(distinct phone_no)
from business.dwd_user_location_hour
where dy = '2020' and dm = '04' and dd = '28' and ci = '758752133'
limit 50;

select count(phone_no)
from business.dwd_user_location_hour
where dy = '2020' and dm = '04' and dd = '28' and ci = '758752133'
limit 50;


--5分钟表结果+小时表结果
drop table workspace.zb_near_month_20200511_cz_result_finally;
create table workspace.zb_near_month_20200511_cz_result_finally 
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select distinct a.cell_id,b.phone_no
from workspace.zb_networks_cell_id_info_20200511 a
left join
(
select *
from workspace.zb_near_month_20200511_cz_result_hour where phone_no is not null
union all
select *
from workspace.zb_near_month_20200511_cz_result where phone_no is not null
) b
on a.cell_id = b.cell_id
;


drop table workspace.zb_near_month_20200511_cz_temp;
create table workspace.zb_near_month_20200511_cz_temp 
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select cell_id,phone_no,count(data_date) as stay_days
from 
(
    select cell_id,phone_no,data_date,count(data_hour) as stay_hour
    from 
    (
        select a.cell_id,b.phone_no,b.data_date,b.data_hour
        from 
        (
        select cell_id
        from workspace.zb_near_month_20200511_cz_result_finally
        where phone_no is null
        ) a
        left join 
        (
            select distinct ci,phone_no,duration,substr(date_time,1,10) as data_date,substr(date_time,12,2) as data_hour
            from business.dwd_user_location_hour
            where dy = '2020' and dm = '04' --and substr(date_time,12,2) in ('22','23','00','01','02','03','04','05','06','07')
        ) b
        on a.cell_id = b.ci
    ) a
    group by cell_id,phone_no,data_date
    having count(data_hour) > 2
)a
group by cell_id,phone_no
;


drop table workspace.zb_near_month_20200511_cz_result_finally_reult;
create table workspace.zb_near_month_20200511_cz_result_finally_reult 
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  select a.cell_id,b.phone_no
  from workspace.zb_networks_cell_id_info_20200511 a
  left join
  (
    select cell_id,phone_no
    from workspace.zb_near_month_20200511_cz_result_finally
    where phone_no is not null
    union all
    select distinct cell_id,phone_no
    from workspace.zb_near_month_20200511_cz_temp
    where stay_days > 7
  ) b on a.cell_id = b.cell_id
;



select count(cell_id)
from workspace.zb_near_month_20200511_cz_result_finally
where phone_no is null
;


--20200514 第二批需求
--数据需求目的：	用于宣传短信发送
--数据筛选条件：	基站小区常驻用户号码
--数据提取字段：	需要提取附件内涉及所有基站小区的常驻用户号码（五一节具有特殊性，请避开）

--导入数据
drop table workspace.zb_networks_cell_id_info_20200514;
create table workspace.zb_networks_cell_id_info_20200514(cell_id string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
putdata -f 20200514_cell_id.txt -t workspace.zb_networks_cell_id_info_20200514;

--基于小区id跑数，跑每个小区的常驻号码
--4月 zb_near_month_202004_cz
--5月 zb_near_month_202005_cz
drop table workspace.zb_near_month_202005_cz;
create table workspace.zb_near_month_202005_cz 
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select ci,phone_no,count(data_date) as stay_days
from 
(
    select ci,phone_no,data_date,count(data_hour) as stay_hour
    from 
    (
      select distinct ci,phone_no,duration,substr(date_time,1,10) as data_date,substr(date_time,12,2) as data_hour
      from business.dwd_user_location_hour
      where dy = '2020' and dm = '05' --and substr(date_time,12,2) in ('22','23','00','01','02','03','04','05','06','07')
    ) a
    group by ci,phone_no,data_date
    having count(data_hour) > 5
)a
group by ci,phone_no
;
--仅匹配，保留天数 stay_days
drop table workspace.zb_near_month_20200514_cz_result_days;
create table workspace.zb_near_month_20200514_cz_result_days 
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  select a.cell_id,b.phone_no,b.stay_days
  from workspace.zb_networks_cell_id_info_20200514 a
  left join
  (
    select distinct ci,phone_no,stay_days
    from workspace.zb_near_month_202004_cz
  ) b
  on a.cell_id = b.ci
  ;

--每天5个小时以上，一月25天 结果
drop table workspace.zb_near_month_20200514_cz_result_days_finally;
create table workspace.zb_near_month_20200514_cz_result_days_finally 
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  select * 
  from zb_near_month_20200514_cz_result_days 
  where stay_days > 25 and phone_no not like '10%' and phone_no like '1%' 
  ;

--4月全量常驻基站小区用户
--4月 zb_near_month_202004_cz_all
--5月 zb_near_month_202005_cz_all
drop table workspace.zb_near_month_202005_cz_all;
create table workspace.zb_near_month_202005_cz_all
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
    select distinct ci,phone_no,stay_days
    from workspace.zb_near_month_202005_cz
    where stay_days > 25 and phone_no not like '10%' and phone_no like '1%' 
  ;

select count(distinct phone_no) from zb_near_month_20200514_cz_result_days where stay_days > 20 and phone_no is not null limit 5;
select count(distinct cell_id) from zb_near_month_20200514_cz_result_days where phone_no is null limit 5;

-- 7天 zb_near_month_20200514_cz_result
--15天 zb_near_month_20200514_cz_result_15
drop table workspace.zb_near_month_20200514_cz_result_15;
create table workspace.zb_near_month_20200514_cz_result_15 
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  select a.cell_id,b.phone_no
  from workspace.zb_networks_cell_id_info_20200514 a
  left join
  (
    select distinct ci,phone_no
    from workspace.zb_near_month_202004_cz
    where stay_days > 14
  ) b
  on a.cell_id = b.ci
  ;


--20200516 第三批需求
--关于2772攻坚居民区常驻客户号码的提取
--数据需求目的：	用于宣传短信发送
--数据筛选条件：	基站小区常驻用户号码
--数据提取字段：	需要提取附件内涉及所有基站小区的常驻用户号码（五一节具有特殊性，请避开）

--导入数据
drop table workspace.zb_networks_cell_id_info_20200516;
create table workspace.zb_networks_cell_id_info_20200516(cell_name string,cell_id string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
putdata -f 20200516_cell_id.txt -t workspace.zb_networks_cell_id_info_20200516;

--匹配4月结果
--每天5个小时以上，一月25天 结果
drop table workspace.zb_near_month_20200516_cz_result_days_finally;
create table workspace.zb_near_month_20200516_cz_result_days_finally 
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  select distinct a.cell_name,a.cell_id,b.phone_no
  from workspace.zb_networks_cell_id_info_20200516 a
  left join workspace.zb_near_month_202004_cz_all b
  on a.cell_id = b.ci
  ;

--20200518 第四批需求
--关于城区网络质优物业点短信发送客户号码提取的申请
--数据需求目的：	用于质优小区客户短信宣传
--数据筛选条件：	附件所有基站小区的常驻客户号码
--数据提取字段：	需提取附件所有基站小区的常驻客户号码（去除五一）

--导入数据
drop table workspace.zb_networks_cell_id_info_20200518;
create table workspace.zb_networks_cell_id_info_20200518(cell_id string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
putdata -f 20200518_cell_id.txt -t workspace.zb_networks_cell_id_info_20200518;

--匹配4月结果
--每天5个小时以上，一月25天 结果
drop table workspace.zb_near_month_20200518_cz_result_days_finally;
create table workspace.zb_near_month_20200518_cz_result_days_finally 
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  select distinct a.cell_id,b.phone_no
  from workspace.zb_networks_cell_id_info_20200518 a
  left join workspace.zb_near_month_202004_cz_all b
  on a.cell_id = b.ci
  ;

--检查
select count(distinct cell_id) from zb_near_month_20200518_cz_result_days_finally where phone_no is null  limit 5;
select count(distinct phone_no) from zb_near_month_20200518_cz_result_days_finally limit 5;


--20200526 第五批需求
--关于城区网络质优物业点短信发送客户号码提取的申请
--数据需求目的：	用于质优小区客户短信宣传
--数据筛选条件：	附件所有基站小区的常驻客户号码
--数据提取字段：	需提取附件所有基站小区的常驻客户号码（去除五一）

--导入数据
drop table workspace.zb_networks_cell_id_info_20200526;
create table workspace.zb_networks_cell_id_info_20200526(cell_name string,cell_id string,quxian string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
putdata -f shujubiao_nongcun.txt -t workspace.zb_networks_cell_id_info_20200526;

--匹配4月结果
--每天5个小时以上，一月25天 结果
drop table workspace.zb_near_month_20200526_cz_result_days_finally;
create table workspace.zb_near_month_20200526_cz_result_days_finally 
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  select distinct a.cell_id,b.phone_no
  from workspace.zb_networks_cell_id_info_20200526 a
  left join workspace.zb_near_month_202004_cz_all b
  on a.cell_id = b.ci
  ;
--检查
select count(distinct cell_id) from zb_near_month_20200526_cz_result_days_finally where phone_no is null  limit 5;
select count(distinct phone_no) from zb_near_month_20200526_cz_result_days_finally limit 5;

--20200526 农村第1批需求 
--关于城区网络质优物业点短信发送客户号码提取的申请
--数据需求目的：	用于质优小区客户短信宣传
--数据筛选条件：	附件所有基站小区的常驻客户号码
--数据提取字段：	需提取附件所有基站小区的常驻客户号码（去除五一）

--导入数据
drop table workspace.zb_networks_cell_id_info_20200526_nongcun;
create table workspace.zb_networks_cell_id_info_20200526_nongcun(cell_name string,cell_id string,quxian string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
putdata -f nongcun_1_info.txt -t workspace.zb_networks_cell_id_info_20200526_nongcun;

--匹配4月结果
--每天5个小时以上，一月25天 结果
drop table workspace.zb_near_month_20200526_cz_result_days_finally_nongcun;
create table workspace.zb_near_month_20200526_cz_result_days_finally_nongcun 
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  SELECT a.cell_id,a.phone_no
  FROM
  (
    select distinct a.cell_id,b.phone_no
    from workspace.zb_networks_cell_id_info_20200526_nongcun a
    left join workspace.zb_near_month_202004_cz_all b
    on a.cell_id = b.ci
  ) a
  LEFT JOIN workspace.th_en_zb_sensitive_phone_info_20200521 b
  ON a.phone_no = b.phone_no
  WHERE b.phone_no IS NULL
  ;

--检查
select count(distinct cell_id) from zb_near_month_20200526_cz_result_days_finally_nongcun where phone_no is null  limit 5;
--483
select count(distinct phone_no) from zb_near_month_20200526_cz_result_days_finally_nongcun limit 5;
--130726

--20200529 农村第2批需求 
--数据需求目的：	附件小区手机号码提取
--数据用途：	只用于分析统计(号码将加密提供)
--是否进行脱敏处理：	敏感客户全部剔除数据筛选条件及提取字段：	常驻客户号码提取，避开五一

--导入数据
drop table workspace.zb_networks_cell_id_info_20200529_nongcun;
create table workspace.zb_networks_cell_id_info_20200529_nongcun(cell_name string,cell_id string,quxian string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
putdata -f rural_second_info.txt -t workspace.zb_networks_cell_id_info_20200529_nongcun;

--剔除敏感客户
--datamart.data_dw_zz_all_vipuser_nocall_latest   敏感客户
--datamart.data_dw_zz_all_outbound_flag_hz_latest    多次营销

--匹配4月结果
--每天5个小时以上，一月25天 结果
drop table workspace.zb_near_month_20200529_cz_result_days_finally_nongcun;
create table workspace.zb_near_month_20200529_cz_result_days_finally_nongcun 
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  SELECT DISTINCT a.cell_id,a.phone_no
  FROM
  (
    select distinct a.cell_id,b.phone_no
    from workspace.zb_networks_cell_id_info_20200529_nongcun a
    left join workspace.zb_near_month_202004_cz_all b
    on a.cell_id = b.ci
  ) a
  LEFT JOIN datamart.data_dw_zz_all_outbound_flag_hz_latest b
  ON a.phone_no = b.opposite_no
  LEFT JOIN datamart.data_dw_zz_all_vipuser_nocall_latest c
  ON a.phone_no = c.phone_no
  WHERE c.phone_no IS NULL OR b.opposite_no IS NULL
  ;

--检查
select count(distinct cell_id) from zb_near_month_20200529_cz_result_days_finally_nongcun where phone_no is null  limit 5;
--277
select count(distinct phone_no) from zb_near_month_20200529_cz_result_days_finally_nongcun limit 5;
--123415

--20200601 农村第3批需求 
--数据需求目的：	附件小区手机号码提取
--数据用途：	只用于分析统计(号码将加密提供)
--是否进行脱敏处理：	敏感客户全部剔除数据筛选条件及提取字段：	常驻客户号码提取，避开五一

--导入数据
drop table workspace.zb_networks_cell_id_info_20200601_nongcun;
create table workspace.zb_networks_cell_id_info_20200601_nongcun(cell_name string,cell_id string,quxian string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
putdata -f rural_third_info.txt -t workspace.zb_networks_cell_id_info_20200601_nongcun;

--剔除敏感客户
--datamart.data_dw_zz_all_vipuser_nocall_latest   敏感客户
--datamart.data_dw_zz_all_outbound_flag_hz_latest    多次营销

--匹配4月结果
--每天5个小时以上，一月25天 结果
drop table workspace.zb_near_month_20200601_cz_result_days_finally_nongcun;
create table workspace.zb_near_month_20200601_cz_result_days_finally_nongcun 
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  SELECT DISTINCT a.cell_id,a.phone_no
  FROM
  (
    select distinct a.cell_id,b.phone_no
    from workspace.zb_networks_cell_id_info_20200601_nongcun a
    left join workspace.zb_near_month_202005_cz_all b
    on a.cell_id = b.ci
  ) a
  LEFT JOIN datamart.data_dw_zz_all_outbound_flag_hz_latest b
  ON a.phone_no = b.opposite_no
  LEFT JOIN datamart.data_dw_zz_all_vipuser_nocall_latest c
  ON a.phone_no = c.phone_no
  WHERE c.phone_no IS NULL OR b.opposite_no IS NULL
  ;

--检查
select count(distinct cell_id) from zb_near_month_20200601_cz_result_days_finally_nongcun where phone_no is null  limit 5;
--1404
select count(distinct phone_no) from zb_near_month_20200601_cz_result_days_finally_nongcun limit 5;
--148572


--20200604 农村第4批需求 
--数据需求目的：	烦请提取常驻客户号码
--数据用途：	只用于分析统计(号码将加密提供)
--是否进行脱敏处理：	敏感客户全部剔除
--数据筛选条件及提取字段：	附件基站小区常驻客户号码

--导入数据
drop table workspace.zb_networks_cell_id_info_20200604_nongcun;
create table workspace.zb_networks_cell_id_info_20200604_nongcun(cell_name string,cell_id string,quxian string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
putdata -f rural_four_info.txt -t workspace.zb_networks_cell_id_info_20200604_nongcun;

--剔除敏感客户
--datamart.data_dw_zz_all_vipuser_nocall_latest   敏感客户
--datamart.data_dw_zz_all_outbound_flag_hz_latest    多次营销

--匹配4月结果
--每天5个小时以上，一月25天 结果
drop table workspace.zb_near_month_20200604_cz_result_days_finally_nongcun;
create table workspace.zb_near_month_20200604_cz_result_days_finally_nongcun 
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  SELECT DISTINCT a.cell_id,a.phone_no
  FROM
  (
    select distinct a.cell_id,b.phone_no
    from workspace.zb_networks_cell_id_info_20200604_nongcun a
    left join workspace.zb_near_month_202005_cz_all b
    on a.cell_id = b.ci
  ) a
  LEFT JOIN datamart.data_dw_zz_all_outbound_flag_hz_latest b
  ON a.phone_no = b.opposite_no
  LEFT JOIN datamart.data_dw_zz_all_vipuser_nocall_latest c
  ON a.phone_no = c.phone_no
  WHERE c.phone_no IS NULL OR b.opposite_no IS NULL
  ;

--检查
select count(distinct cell_id) from zb_near_month_20200604_cz_result_days_finally_nongcun where phone_no is null  limit 5;
--610
select count(distinct phone_no) from zb_near_month_20200604_cz_result_days_finally_nongcun limit 5;
--120327


--20200605 农村第5批需求 
--数据需求目的：	烦请提取常驻客户号码
--数据用途：	只用于分析统计(号码将加密提供)
--是否进行脱敏处理：	敏感客户全部剔除
--数据筛选条件及提取字段：	附件基站小区常驻客户号码

--导入数据
drop table workspace.zb_networks_cell_id_info_20200605_nongcun;
create table workspace.zb_networks_cell_id_info_20200605_nongcun(cell_name string,cell_id string,quxian string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
putdata -f rural_five_info.txt -t workspace.zb_networks_cell_id_info_20200605_nongcun;

--剔除敏感客户
--datamart.data_dw_zz_all_vipuser_nocall_latest   敏感客户
--datamart.data_dw_zz_all_outbound_flag_hz_latest    多次营销

--匹配4月结果
--每天5个小时以上，一月25天 结果
drop table workspace.zb_near_month_20200605_cz_result_days_finally_nongcun;
create table workspace.zb_near_month_20200605_cz_result_days_finally_nongcun 
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  SELECT DISTINCT a.cell_id,a.phone_no
  FROM
  (
    select distinct a.cell_id,b.phone_no
    from workspace.zb_networks_cell_id_info_20200605_nongcun a
    left join workspace.zb_near_month_202005_cz_all b
    on a.cell_id = b.ci
  ) a
  LEFT JOIN datamart.data_dw_zz_all_outbound_flag_hz_latest b
  ON a.phone_no = b.opposite_no
  LEFT JOIN datamart.data_dw_zz_all_vipuser_nocall_latest c
  ON a.phone_no = c.phone_no
  WHERE c.phone_no IS NULL OR b.opposite_no IS NULL
  ;

--检查
select count(distinct cell_id) from zb_near_month_20200605_cz_result_days_finally_nongcun where phone_no is null  limit 5;
--647
select count(distinct phone_no) from zb_near_month_20200605_cz_result_days_finally_nongcun limit 5;
--169439


--20200609 农村第6批需求 
--数据需求目的：	烦请提取常驻客户号码
--数据用途：	只用于分析统计(号码将加密提供)
--是否进行脱敏处理：	敏感客户全部剔除
--数据筛选条件及提取字段：	附件基站小区常驻客户号码

--导入数据
drop table workspace.zb_networks_cell_id_info_20200609_nongcun;
create table workspace.zb_networks_cell_id_info_20200609_nongcun(cell_name string,cell_id string,quxian string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
putdata -f rural_six_info.txt -t workspace.zb_networks_cell_id_info_20200609_nongcun;

--剔除敏感客户
--datamart.data_dw_zz_all_vipuser_nocall_latest   敏感客户
--datamart.data_dw_zz_all_outbound_flag_hz_latest    多次营销

--匹配4月结果
--每天5个小时以上，一月25天 结果
drop table workspace.zb_near_month_20200609_cz_result_days_finally_nongcun;
create table workspace.zb_near_month_20200609_cz_result_days_finally_nongcun 
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  SELECT DISTINCT a.cell_id,a.phone_no
  FROM
  (
    select distinct a.cell_id,b.phone_no
    from workspace.zb_networks_cell_id_info_20200609_nongcun a
    left join workspace.zb_near_month_202005_cz_all b
    on a.cell_id = b.ci
  ) a
  LEFT JOIN datamart.data_dw_zz_all_outbound_flag_hz_latest b
  ON a.phone_no = b.opposite_no
  LEFT JOIN datamart.data_dw_zz_all_vipuser_nocall_latest c
  ON a.phone_no = c.phone_no
  WHERE c.phone_no IS NULL OR b.opposite_no IS NULL
  ;

--检查
select count(distinct cell_id) from zb_near_month_20200609_cz_result_days_finally_nongcun where phone_no is null  limit 5;
--825
select count(distinct phone_no) from zb_near_month_20200609_cz_result_days_finally_nongcun limit 5;
--118350