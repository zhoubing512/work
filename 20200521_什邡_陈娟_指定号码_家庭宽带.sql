--为提高5G资费、高危合约等重点指标修复，现申请提取以下几类客户数据，请领导审批！
--1、指定号码（员工、渠道）常用通话联系号码与5G客户目标、高危客户、无宽带客户对应的号码信息；
--2、家庭宽带：什邡存量宽带号码的套餐、合约、消费信息；
--提取数据纬度见附件


--导入数据 宽带信息数据
drop table workspace.zb_shifang_kd_phone_info_20200521;
create table workspace.zb_shifang_kd_phone_info_20200521(kd_209 string,kd_cell string,fenju string,key_phone_no string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
putdata -f 20200521_shifang_kd_phone_info.txt -t workspace.zb_shifang_kd_phone_info_20200521;
--加密 th_en_zb_shifang_kd_phone_info_20200521

--导入数据 指定号码数据
drop table workspace.zb_shifang_zhiding_phone_info_20200521;
create table workspace.zb_shifang_zhiding_phone_info_20200521(phone_no string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
putdata -f 20200521_shifang_zhiding_phone_info.txt -t workspace.zb_shifang_zhiding_phone_info_20200521;
--加密 th_en_zb_shifang_zhiding_phone_info_20200521

--导入数据 5G目标数据
drop table workspace.zb_shifang_5g_target_phone_info_20200521;
create table workspace.zb_shifang_5g_target_phone_info_20200521(phone_no string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
putdata -f 20200521_shifang_5G_target_phone.txt -t workspace.zb_shifang_5g_target_phone_info_20200521;
--加密 th_en_zb_shifang_5g_target_phone_info_20200521

--导入数据 高危目标数据
drop table workspace.zb_shifang_gaowei_target_phone_info_20200521;
create table workspace.zb_shifang_gaowei_target_phone_info_20200521(phone_no string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
putdata -f 20200521_shifang_gaowei_phone.txt -t workspace.zb_shifang_gaowei_target_phone_info_20200521;
--加密 th_en_zb_shifang_gaowei_target_phone_info_20200521

--导入数据 无宽带目标数据
drop table workspace.zb_shifang_nokd_target_phone_info_20200521;
create table workspace.zb_shifang_nokd_target_phone_info_20200521(phone_no string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
putdata -f 20200521_shifang_nokd_phone_info.txt -t workspace.zb_shifang_nokd_target_phone_info_20200521;
--加密 th_en_zb_shifang_nokd_target_phone_info_20200521

--导入数据 需要剔除的敏感数据 th_en_zb_sensitive_phone_info_20200514 (之前的一个版本)
PHONE_NO	WD_13N_ORIGIN	WD_13N_SEN_CLASS	WD_LW_ORIGIN	WD_LW_SEN_CLASS	NBYG_ORIGIN	NBYG_SEN_CLASS

drop table workspace.zb_sensitive_phone_info_20200521;
create table workspace.zb_sensitive_phone_info_20200521(phone_no string,wd_13n_origin string,wd_13n_sen_class string
                ,wd_lw_origin string,wd_lw_sen_class string,nbyg_origin string,nbyg_sen_class string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
putdata -f VIPUSER.txt -t workspace.zb_sensitive_phone_info_20200521;
--加密 th_en_zb_sensitive_phone_info_20200521


--取下月宽带资费
--data_user_prc 根据eff_time排序 最新的时间即为下月的资费套餐


----1、指定号码（员工、渠道）常用通话联系号码与5G客户目标、高危客户、无宽带客户对应的号码信息；
--主叫大于3次/月，且存在被叫(2,3,4三个月)
--01主叫 02被叫
drop table workspace.zb_3months_call_avg_234;
create table workspace.zb_3months_call_avg_234
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select phone_no,
        opposite_no,
        call_times_months_avg_caller,
        call_duration_months_avg_caller,
        call_times_months_avg_called,
        call_duration_months_avg_called
from 
(
    select a.phone_no,
            a.opposite_no,
            call_times_months_avg as call_times_months_avg_caller,
            call_duration_months_avg as call_duration_months_avg_caller,
            lead(call_times_months_avg,1) over(partition by a.phone_no,a.opposite_no order by a.calltype_id) as call_times_months_avg_called,
            lead(call_duration_months_avg,1) over(partition by a.phone_no,a.opposite_no order by a.calltype_id) as call_duration_months_avg_called,
            row_number() over (partition by a.phone_no,a.opposite_no order by a.calltype_id ) as rank
    from 
    (
        select a.phone_no,a.opposite_no,a.calltype_id,avg(call_times_months) as call_times_months_avg,
                avg(call_duration_months) as call_duration_months_avg
        from 
        (
            select a.phone_no,a.opposite_no,a.calltype_id,sum(call_times) as call_times_months,sum(call_duration) as call_duration_months
            from 
            (
                select opposite_no,phone_no,calltype_id,call_times,call_duration,dm
                from datamart.data_dwb_cal_user_voc_yx_ds a
                where dy = '2020' and (dm = '02' or dm = '03' or dm = '04')
            ) a
            group by a.phone_no,a.opposite_no,a.calltype_id,a.dm
        ) a 
        group by a.phone_no,a.opposite_no,a.calltype_id
    ) a
) a where rank = 1
;

--匹配指定号码的5g、高危、无宽带客户（剔除了敏感客户）
drop table workspace.zb_zhiding_phone_result_20200521;
create table workspace.zb_zhiding_phone_result_20200521
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  select *
  from 
  (
    select distinct a.phone_no,
            b.phone_no as phone_no_5g,
            c.phone_no as phone_no_gaowei,
            d.phone_no as phone_no_nokd
    from 
    (
        select a.phone_no,b.opposite_no
        from workspace.th_en_zb_shifang_zhiding_phone_info_20200521 a
        left join 
        (
            select *
            from workspace.zb_3months_call_avg_234
            where call_times_months_avg_caller > 3 and call_times_months_avg_called > 0
        ) b
        on a.phone_no = b.phone_no
    ) a
    left join 
    (
        select a.phone_no
        from workspace.th_en_zb_shifang_5g_target_phone_info_20200521 a
        where a.phone_no not in (
            select phone_no from workspace.th_en_zb_sensitive_phone_info_20200521
        )
    ) b
    on a.opposite_no = b.phone_no
    left join 
    (
        select a.phone_no
        from workspace.th_en_zb_shifang_gaowei_target_phone_info_20200521 a
        where a.phone_no not in (
            select phone_no from workspace.th_en_zb_sensitive_phone_info_20200521
        )
    ) c
    on a.opposite_no = c.phone_no
    left join 
    (
        select a.phone_no
        from workspace.th_en_zb_shifang_nokd_target_phone_info_20200521 a
        where a.phone_no not in (
            select phone_no from workspace.th_en_zb_sensitive_phone_info_20200521
        )
    ) d
    on a.opposite_no = d.phone_no
  ) a where phone_no_5g is not null or phone_no_gaowei is not null or phone_no_nokd is not null
;

--检查 
select count(distinct phone_no)
from workspace.zb_zhiding_phone_result_20200521
where phone_no_5g is null;
--102
select count(distinct phone_no)
from workspace.zb_zhiding_phone_result_20200521
where phone_no_gaowei is null;
--116
select count(distinct phone_no)
from workspace.zb_zhiding_phone_result_20200521
where phone_no_nokd is null;
--82

select count(distinct phone_no)
from workspace.zb_zhiding_phone_result_20200521
where phone_no_nokd is null and  phone_no_gaowei is null and phone_no_5g is null;
--0
select phone_no,phone_no_5g,phone_no_gaowei,phone_no_nokd
from workspace.zb_zhiding_phone_result_20200521 
where phone_no = '1878380GUXQ'
;
select phone_no,count(phone_no)
from workspace.zb_zhiding_phone_result_20200521 
group by phone_no
;
select * from workspace.zb_zhiding_phone_result_20200521
where phone_no = '1340814lUXr'
;


--存量宽带用户数据
--需要匹配的字段：宽带套餐、状态、宽带套餐名称、下一月宽带套餐、宽带套餐价值、家庭消费、宽带上网时长、电视上网时长（分）、家庭成员数、升舱推荐
--关键号码套餐、合约活动、合约活动到期时间、关键号码3月平均消费、子成员1、子成员1资费、子成员1 3月平均消费、子成员2、子成员资费2、子成员2 3月平均消费
--子成员3、子成员3资费、子成员3 3月平均消费、子成员4、子成员4资费、子成员4 3月平均消费、子成员5、子成员资费5、子成员5 3月平均消费
/*
例：
+--------------+----------------------+----------+-----------------+
|   a.kd_209   |      a.kd_cell       | a.fenju  | a.key_phone_no  |
+--------------+----------------------+----------+-----------------+
| 2096380lIvt  | 什邡市隐峰镇新建村            | 师古分局     | 1528289qUvy     |
| 2096310ToXu  | 什邡市京什西路北段789号金河北苑小区  | 城北分局     | 1356821qJXA     |
| 2096277RsEu  | 什邡市湔氐镇桐林村1组(合建)      | 洛水分局     | 1878103RJcg     |
| 2096348RoSu  | 什邡市湔氐镇龙居洛小路龙居场口      | 洛水分局     | 1389021lNbu     |
| 2096323pIXu  | 什邡市马祖镇场镇             | 双盛分局     | 1588340zkWt     |
+--------------+----------------------+----------+-----------------+
*/
--通过kd_209查询

select * 
from datamart.data_user_info
where dy = '2020' and dm = '05' and phone_no = '2096323pIXu'
limit 5;

select phone_no,pricing_id,prod_prcid,parprc_id,prod_id,state,state_time,prod_main_flag,order_time,eff_time
from datamart.data_user_prc
where phone_no = '1528289qUvy'
order by eff_time
limit 50;

1528289qUvy
+--------------+-------------+------------+--------+----------------------+-----------------+----------------------+----------------------+
|   phone_no   | prod_prcid  |  prod_id   | state  |      state_time      | prod_main_flag  |      order_time      |       eff_time       |
+--------------+-------------+------------+--------+----------------------+-----------------+----------------------+----------------------+
| 2096323pIXu  | ACBZ12915   | APBZ05355  | A      | 2018-06-08 13:30:36  | 1               | 2018-06-08 13:30:36  | 2018-07-01 00:00:00  |
| 2096323pIXu  | ACAZ26694   | APAZ11140  | A      | 2018-06-07 10:37:48  | 1               | 2018-06-07 10:37:48  | 2018-06-07 10:37:36  |
| 2096323pIXu  | ACAZ25634   | APAZ12397  | A      | 2018-06-07 10:37:48  | 0               | 2018-06-07 10:37:48  | 2018-06-07 10:37:15  |
+--------------+-------------+------------+--------+----------------------+-----------------+----------------------+----------------------+

select phone_no,prod_prcid,state_time
from datamart.data_user_prc
where prod_prcid = 'ACBZ14261'
limit 10;

select prod_prcid,prod_prc_name,prod_prc_type,prod_price
from datamart.base_prc_info
where prod_prcid = 'ACBZ14261'
limit 10;

--通过宽带主账号
select phone_no,member_role_name,state,key_phone_no,prod_prcid_kdtc,prod_prc_name_kdtc,kd_no1,prod_prcid_kdzf1,prod_prc_name_kdzf1
from datamart.dw_user_kd
where dy = '2020' and dm = '04' and key_phone_no = '1528289qUvy'
limit 5;
+--------------+-------------------+--------+---------------+------------------+---------------------+--------------+-------------------+----------------------+
|   phone_no   | member_role_name  | state  | key_phone_no  | prod_prcid_kdtc  | prod_prc_name_kdtc  |    kd_no1    | prod_prcid_kdzf1  | prod_prc_name_kdzf1  |
+--------------+-------------------+--------+---------------+------------------+---------------------+--------------+-------------------+----------------------+
| 1518100Gojr  | 全家福普通成员           | A      | 1528289qUvy   | ACBZ14261        | 新爱家38               | 2096380lIvt  | ACAZ25635         | 融合宽带50M              |
| 2096380lIvt  | 全家福宽带成员           | A      | 1528289qUvy   | ACBZ14261        | 新爱家38               | 2096380lIvt  | ACAZ25635         | 融合宽带50M              |
| 1500839qHbr  | 全家福普通成员           | A      | 1528289qUvy   | ACBZ14261        | 新爱家38               | 2096380lIvt  | ACAZ25635         | 融合宽带50M              |
| 1528289qUvy  | 全家福关键人            | A      | 1528289qUvy   | ACBZ14261        | 新爱家38               | 2096380lIvt  | ACAZ25635         | 融合宽带50M              |
+--------------+-------------------+--------+---------------+------------------+---------------------+--------------+-------------------+----------------------+




--检验 家庭宽带上网时长
select *
from 
(
    select a.key_phone_no,b.dur
    from datamart.dw_user_kd_latest a
    inner join 
    (
        select *
        from datamart.data_dw_kd_user_analyze_detail
        where dy = '2020' and dm = '03'
    ) b on a.id_no = b.id_no
) a where a.key_phone_no = '1528289qUvy'
;

--合约活动
select phone_no,act_id_a,act_name_a,means_id_a,means_name_a,fee_code_a,fee_name_a,innet_date_a
from datamart.dw_user_act a
where dy = '2020' and dm = '05'
limit 10;
--电视上网时长
select kd_no,avg(watch_duration) as tv_watch_times
from 
(
    select kd_no,tv_no,watch_duration,dy,dm,dd
    from datamart.data_dmp_user_watch_detail
    where dy = '2020' and dm ='05'
) a
group by kd_no
limit 1;

--检查
select phone_no,run_code,onnet_month
from datamart.data_dm_uv_info_m a
where dy = '2020' and dm = '04' and phone_no = '2096323pIXu'
;

select phone_no,key_phone_no,prod_prcid_kdtc,prod_prc_name_kdtc,prod_prcid_kdzf1,prod_prc_name_kdzf1
from datamart.dw_user_kd_latest
where phone_no = key_phone_no
limit 5;

--下月的宽带宽带套餐
select a.phone_no,a.prod_prcid,b.prod_prc_name
from 
(
    select phone_no,prod_prcid,eff_time
    from 
    (
        select phone_no,prod_prcid,eff_time,row_number() over(partition by phone_no order by eff_time desc) rn
        from datamart.data_user_prc
        where prod_prcid in
        (
            select distinct prod_prcid_kdtc
            from datamart.dw_user_kd
            where dy = '2020' and dm = '04' and phone_no = key_phone_no
        )
    ) a where rn = 1
) a 
left join datamart.base_prc_info b
on a.prod_prcid = b.prod_prcid


--成员数
select distinct phone_no,kd_no1,count(phone_no) over(partition by kd_no1) as chengyuan_num
from datamart.dw_user_kd_latest 
where phone_no = key_phone_no

--子成员 主资费，主资费价格，近三月平均消费
drop table workspace.zb_kd_child_num_info;
create table workspace.zb_kd_child_num_info
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select key_phone_no,
        cy_phone1,prod_prc_name1,main_prc_fee1,avg_3mon_arpu1,
        cy_phone2,prod_prc_name2,main_prc_fee2,avg_3mon_arpu2,
        cy_phone3,prod_prc_name3,main_prc_fee3,avg_3mon_arpu3,
        cy_phone4,prod_prc_name4,main_prc_fee4,avg_3mon_arpu4,
        cy_phone5,prod_prc_name5,main_prc_fee5,avg_3mon_arpu5
from 
(
    select key_phone_no,phone_no as cy_phone1,prod_prc_name as prod_prc_name1,main_prc_fee as main_prc_fee1,avg_3mon_arpu as avg_3mon_arpu1,
            lead(phone_no,1) over(partition by key_phone_no order by member_role_id) as cy_phone2,
            lead(prod_prc_name,1) over(partition by key_phone_no order by member_role_id) as prod_prc_name2,
            lead(main_prc_fee,1) over(partition by key_phone_no order by member_role_id) as main_prc_fee2,
            lead(avg_3mon_arpu,1) over(partition by key_phone_no order by member_role_id) as avg_3mon_arpu2,
            lead(phone_no,2) over(partition by key_phone_no order by member_role_id) as cy_phone3,
            lead(prod_prc_name,2) over(partition by key_phone_no order by member_role_id) as prod_prc_name3,
            lead(main_prc_fee,2) over(partition by key_phone_no order by member_role_id) as main_prc_fee3,
            lead(avg_3mon_arpu,2) over(partition by key_phone_no order by member_role_id) as avg_3mon_arpu3,
            lead(phone_no,3) over(partition by key_phone_no order by member_role_id) as cy_phone4,
            lead(prod_prc_name,3) over(partition by key_phone_no order by member_role_id) as prod_prc_name4,
            lead(main_prc_fee,3) over(partition by key_phone_no order by member_role_id) as main_prc_fee4,
            lead(avg_3mon_arpu,3) over(partition by key_phone_no order by member_role_id) as avg_3mon_arpu4,
            lead(phone_no,4) over(partition by key_phone_no order by member_role_id) as cy_phone5,
            lead(prod_prc_name,4) over(partition by key_phone_no order by member_role_id) as prod_prc_name5,
            lead(main_prc_fee,4) over(partition by key_phone_no order by member_role_id) as main_prc_fee5,
            lead(avg_3mon_arpu,4) over(partition by key_phone_no order by member_role_id) as avg_3mon_arpu5,
            row_number() over(partition by key_phone_no order by member_role_id) as rn
    from 
    (
        select distinct a.key_phone_no,a.phone_no,a.member_role_id,b.prod_prc_name,b.main_prc_fee,b.avg_3mon_arpu
        from 
        (
            select *
            from 
            (
                select *
                from datamart.dw_user_kd_latest
                where phone_no like '1%'
            ) a where a.phone_no not in (
                select phone_no from workspace.th_en_zb_sensitive_phone_info_20200521
            )
        ) a 
        left join 
        (
            select a.*,b.prod_prc_name
            from 
            (
                select *
                from datamart.data_dm_uv_info_m 
                where dy = '2020' and dm = '04'
            ) a
            left join datamart.base_prc_info b
            on a.main_prod_prcid = b.prod_prcid
        ) b on a.phone_no = b.phone_no
    ) a
) a where rn = 1
;
--下月宽带套餐
drop table workspace.zb_kd_next_month_20200521;
create table workspace.zb_kd_next_month_20200521
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
    select a.phone_no,a.prod_prcid,b.prod_prc_name
    from 
    (
        select phone_no,prod_prcid,eff_time
        from 
        (
            select phone_no,prod_prcid,eff_time,row_number() over(partition by phone_no order by eff_time desc) rn
            from datamart.data_user_prc
            where prod_prcid in
            (
                select distinct prod_prcid_kdtc
                from datamart.dw_user_kd
                where dy = '2020' and dm = '04' and phone_no = key_phone_no
            )
        ) a where rn = 1
    ) a 
    left join datamart.base_prc_info b
    on a.prod_prcid = b.prod_prcid
    ;

--家庭消费  近三月家庭平均消费
drop table workspace.zb_jiating_avg_xiaofei_20200521;
create table workspace.zb_jiating_avg_xiaofei_20200521
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
    select key_phone_no,avg(avg_3mon_arpu) as jiating_xiaofei
    from 
    (
        select distinct a.key_phone_no,a.phone_no,b.avg_3mon_arpu
        from datamart.dw_user_kd_latest a 
        left join 
        (
            select a.*,b.prod_prc_name
            from 
            (
                select *
                from datamart.data_dm_uv_info_m 
                where dy = '2020' and dm = '04'
            ) a
            left join datamart.base_prc_info b
            on a.main_prod_prcid = b.prod_prcid
        ) b on a.phone_no = b.phone_no
    ) a
    group by key_phone_no
    ;

--匹配结果表

--需要匹配的字段：宽带套餐、状态、宽带套餐名称、下一月宽带套餐、宽带套餐价值、家庭消费、宽带上网时长、电视上网时长（分）、家庭成员数、升舱推荐
--关键号码套餐、合约活动、合约活动到期时间、关键号码3月平均消费、子成员1、子成员1资费、子成员1 3月平均消费、子成员2、子成员资费2、子成员2 3月平均消费
--子成员3、子成员3资费、子成员3 3月平均消费、子成员4、子成员4资费、子成员4 3月平均消费、子成员5、子成员资费5、子成员5 3月平均消费
drop table workspace.zb_shifang_kd_info_20200522_result;
create table workspace.zb_shifang_kd_info_20200522_result
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select a.kd_209,a.kd_cell,a.fenju,a.key_phone_no,
        b.prod_prcid_kdtc, --宽带套餐代码
        b.prod_prc_name_kdtc,--宽带套餐名称
        d.run_code as kd_state, --状态
        --b.prod_prcid_kdzf1, --宽带资费代码
        --b.prod_prc_name_kdzf1, --宽带资费名称
        c.prod_prcid,   --下月宽带套餐代码
        c.prod_prc_name as next_month_kd_prod_prc_name,  --下月宽带套餐
        d.broad_prod_fee,  --宽带套餐价值
        round(e.jiating_xiaofei,2), --家庭消费
        f.dur as jiating_kd_shangwang_times, --家庭宽带上网时长
        g.tv_watch_times,  --电视上网时长
        h.chengyuan_num,  --成员数
        d.prod_prc_name as key_prod_prc_name, --关键号码套餐
        m.means_id_a,  --合约活动代码
        m.means_name_a,  --合约活动
        m.innet_date_a,  --合约活动到期时间
        d.avg_3mon_arpu as key_phone_avg_3mon_arpu,  --关键号码近三月的平均消费
        n.cy_phone1,n.prod_prc_name1,n.main_prc_fee1,n.avg_3mon_arpu1,
        n.cy_phone2,n.prod_prc_name2,n.main_prc_fee2,n.avg_3mon_arpu2,
        n.cy_phone3,n.prod_prc_name3,n.main_prc_fee3,n.avg_3mon_arpu3,
        n.cy_phone4,n.prod_prc_name4,n.main_prc_fee4,n.avg_3mon_arpu4,
        n.cy_phone5,n.prod_prc_name5,n.main_prc_fee5,n.avg_3mon_arpu5
from workspace.th_en_zb_shifang_kd_phone_info_20200521 a
left join 
( --宽带套餐信息
    select *
    from datamart.dw_user_kd_latest
    where phone_no = key_phone_no
) b on a.key_phone_no = b.phone_no
left join workspace.zb_kd_next_month_20200521 c on a.key_phone_no = c.phone_no --下月宽带套餐
left join
(  --关键人账号信息
    select a.*,b.prod_prc_name
    from 
    (
        select *
        from datamart.data_dm_uv_info_m 
        where dy = '2020' and dm = '04'
    ) a
    left join datamart.base_prc_info b
    on a.main_prod_prcid = b.prod_prcid
) d on a.key_phone_no = d.phone_no
left join workspace.zb_jiating_avg_xiaofei_20200521 e on a.key_phone_no = e.key_phone_no  --家庭消费
left join
( --家庭上网时长
    select a.key_phone_no,b.dur
    from datamart.dw_user_kd_latest a
    inner join 
    (
        select *
        from datamart.data_dw_kd_user_analyze_detail
        where dy = '2020' and dm = '03'
    ) b on a.id_no = b.id_no
) f on a.key_phone_no = f.key_phone_no
left join
(
    --电视上网时长
    select kd_no,round(avg(watch_duration)/60,2) as tv_watch_times
    from 
    (
        select kd_no,tv_no,watch_duration,dy,dm,dd
        from datamart.data_dmp_user_watch_detail
        where dy = '2020' and dm ='05'
    ) a
    group by kd_no
) g on a.kd_209 = g.kd_no
left join
( --成员数
    select *
    from
    (
        select distinct phone_no,key_phone_no,kd_no1,count(phone_no) over(partition by kd_no1) as chengyuan_num
        from datamart.dw_user_kd_latest
        where phone_no like '1%' 
    ) a
    where a.phone_no = a.key_phone_no
) h on a.key_phone_no = h.phone_no
left join 
( --合约
    select phone_no,act_id_a,act_name_a,means_id_a,means_name_a,fee_code_a,fee_name_a,innet_date_a
    from datamart.dw_user_act a
    where dy = '2020' and dm = '05'
) m on a.key_phone_no = m.phone_no
left join workspace.zb_kd_child_num_info n 
on a.key_phone_no = n.key_phone_no  --子成员信息
;

--检查重复

select key_phone_no,cy_phone1
from workspace.zb_shifang_kd_info_20200522_result
where key_phone_no = '1341900Focu';

select * 
from workspace.zb_shifang_kd_info_20200522_result 
where means_id_a is not null
limit 5;

