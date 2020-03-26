和动漫

--导出表到文件
insert overwrite local directory '/mnt/disk1/user/lj/results/zb.zb_dm_predict_two' row format delimited fields 
terminated by ',' 
select * from workspace.zb_dm_predict_two where 1=1 
;
--退出hive压缩文件
merge_reduce.sh /mnt/disk1/user/lj/results/zb.zb_dm_predict_two;

select distinct phone_no,key_phone_no,prod_prc_names_tvb from dw_user_kd_latest where prod_prc_names_tvb is not null;


--导出表workspace.zb_dm_last ，workspace.zb_dmph_user


select * from datamart.data_dw_xdr_gprs_app limit 20;
select count(distinct phone_no) from dw_user_kd_latest where prod_prc_names_tvb is not null;
--宽带账号：2092755zHjt
data_ods_demand_tv_pay

data_xmm_tv

1828383pNbL 2098102lUSQ


select wlan_account,count(wlan_account) from datamart.data_ods_demand_tv_pay 
where city_name = '德阳'
group by wlan_account 
having count(wlan_account) >2;

--电视节目种类
select distinct content_name from datamart.data_ods_demand_tv_pay 
where city_name = '德阳'

--电视节目产品订购，电影，电视剧，综艺，。。。
select * from datamart.data_ods_demand_tv_pay 
where city_name = '德阳' 
and wlan_account = '2090583RNSt'
--and tv_account = '2096272awcZa'
and dy = '2019' and dm in ('08','07')
;

select * from datamart.data_xmm_tv 
where tv_account = '1828383pNbL'
limit 5
;

select * from datamart.dw_user_act
where phone_no = '1828383pNbL'
limit 5
;

select * from datamart.dw_user_fee
where phone_no = '1828383pNbL'
;

--
select * from datamart.dw_user_kd
where kd_no1 = '2090583RNSt'
;

select * from data_dw_xdr_gprs_app where app_name='%饿了%';
--通过点播的电视，电影节目 匹配宽带账号信息
select count(distinct wlan_account)
from
(
    select a.wlan_account 
    from
    (
        select * 
        from datamart.data_ods_demand_tv_pay 
        where city_name = '德阳'
    ) a
    inner join 
    (
        select * 
        from datamart.dw_user_kd
        where dy='2019'
    ) b
    on a.wlan_account = b.kd_no1
) c
;
--28147个数据;

select a.phone_no,b.prod_prc_name,b.prod_prc_desc,b.prod_price
from data_user_prc a
inner join 
(
    select * 
    from base_prc_info
    where prod_prc_name like '%动漫%'
) b
on a.prod_prcid = b.prod_prcid
where a.phone_no is not null
limit 10
;
--14907  订购了动漫类业务
select distinct prod_prc_name from base_prc_info  limit 100;

select count(distinct prod_prc_name) from base_prc_info where prod_prc_name like '%动漫%';

select count(*)
from datamart.data_user_prc a
inner join 
(
    select * 
    from datamart.base_prc_info
    where prod_prc_name like '%动漫%'
) b
on a.prod_prcid = b.prod_prcid
where a.phone_no is not null;
--订购了动漫类业务的用户(标记为动漫用户)
--安装使用动漫类app的用户(连续三月均有使用的，标记为动漫用户)(app的种类，包含动漫就可以)

--未安装过视频类的app用户，或未订购过相关业务的用户，认为是非动漫用户群体

--用户基础信息，不同年龄段看动漫不同
--消费费用、业务消费分析、消费占比 动漫类业务在消费中的比例
--app流量分析，app会员消费，动漫杂志书
--熬夜情况，点外卖情况(美团、饿了么等)

select * from datamart.base_prc_info where prod_prc_name like '%青春卡%';
select count(distinct tv_no) from data_dmp_user_watch_detail;
--动漫类app
[\u52a8][\u6f2b]:[0-9]\.?[0-9]*



--按偏好打标签，动漫和非动漫(选手机号码状态正常的用户 run_code=A)
--非动漫用户判断为看动漫比例为0(其中包括看电视不看动漫的及不看电视的)
--提取字段：open_date(开户日期),age,网龄,sex,近三月动漫类月均消费(手机资费)，套外消费，近三月平均消费，近三月平均套外消费，近三月平均套内消费，闲时流量(日数据)，非闲时流量(日数据)，上网次数，上网时长(data_user_flow)
--，电视账号当月使用时长，是否存在营销活动，电视月总时长，使用天数，当月每日平均时长(data_xmm_tv)，通话时长(通话时长越长的用户，看动漫概率越小)，通话次数，语音费，短彩信费，上网费
--，观看电视节目时长，搜索动漫节目次数，近6月单点次数，近2月定包次数，电视包累计资费，电视包订购时间累计时长，最近90天点播观看时长，最近90天点播观看次数
--，常见视频App使用流量，视频App会员消费，外卖App使用
--对于日数据，需要求得   均值 标准差 振幅/均值 标准差/均值 趋势

--宽带用户和非宽带用户分开建模
--对样本数据采用偏好打标签，对于宽带用户 电视包，以及相关的点播次数数据可以获取，则采用所有维度进行建模；对于非宽带用户则 不用电视相关字段。
--(非宽带用户量比较大，不能采用均值补充或者其他补充的方式进行缺失值处理)
--采用分类学习器进行模型训练



--动漫类视频占比30%以上的用户
select count(regexp_extract(movie_pre,'动漫:[0-9]\.?[0-9]*',0))
from data_dmp_user_program_preference
where substr(regexp_extract(movie_pre,'动漫:[0-9]\.?[0-9]*',0),4) > 0.3 
and tv_no not like '2%' and tv_no not like '10%'
limit 5;
30% 75824 去掉10开头和2开头的单宽带用户

30% 88520
20% 有109683样本

--根据用户号码，提取相应的字段信息
--偏好，搜索次数，电视包订购，回看详情  4个表是月表，并且只有12月份数据
--tv账号，搜索节目次数，近6月单点次数，近2月定包次数，电视包累计资费，电视订购包累计时长，90天回看时长，90回看次数，点播观看时长，点播观看次数

--观看电视节目的数据信息
drop table workspace.zb_all_user_tv;
create table workspace.zb_all_user_tv
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  select a.tv_no
    ,case when a.mon_search_times is not null then a.mon_search_times else 0 end as mon_search_times
    ,case when a.six_no is not null then a.six_no else 0 end as six_no
    ,case when a.pay_no is not null then a.pay_no else 0 end as pay_no
    ,case when a.tv_pay is not null then a.tv_pay else 0 end as tv_pay
    ,case when a.pay_time is not null then a.pay_time else 0 end as pay_time
    ,case when a.demand_time is not null then a.demand_time else 0 end as demand_time
    ,case when a.demand_no is not null then a.demand_no else 0 end as demand_no
    ,case when a.back_time is not null then a.back_time else 0 end as back_time
    ,case when a.back_no is not null then a.back_no else 0 end as back_no
  from 
  (
    select a.tv_no
        ,b.mon_search_times
        ,c.six_no
        ,c.pay_no
        ,c.tv_pay
        ,c.pay_time
        ,d.demand_time
        ,d.demand_no
        ,d.back_time
        ,d.back_no
    from
    (
    select tv_no
    from datamart.data_dmp_user_program_preference
    where tv_no not like '2%' and tv_no not like '10%' and tv_no is not null and tv_no <> ''
    ) a
    left join 
    (
        select kd_no,sum(search_times) as mon_search_times 
        from datamart.data_dmp_user_search_programset 
        group by kd_no
    ) b
    on a.tv_no = b.kd_no
    left join 
    (
        select a.kd_no,round(sum(a.six_no),3) as six_no,round(sum(a.pay_no),3) as pay_no,round(sum(a.tv_pay),3) as tv_pay,round(sum(a.pay_time),3) as pay_time
        from datamart.data_dmp_user_payment a
        group by a.kd_no
    ) c
    on a.tv_no = c.kd_no
    left join 
    (
        select kd_no,round(sum(demand_time)/count(*),3) as demand_time,round(sum(demand_no)/count(*),3) as demand_no
            ,round(sum(back_time)/count(*),3) as back_time,round(sum(back_no)/count(*),3) as back_no
        from datamart.data_dmp_user_active_basic
        group by kd_no
    ) d
    on a.tv_no = d.kd_no
) a
;
select count(distinct tv_no) from workspace.zb_all_user_tv;
--观看时长（日数据） 均值 标准差 振幅 振幅/均值  趋势
drop table workspace.zb_tv_watch_time;
create table workspace.zb_tv_watch_time
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  select a.tv_no,a.watch_times_means,a.ft,sqrt(sum(power(a.watch_times_day-a.watch_times_means,2)/(a.ft-1))) as watch_times_std,max(a.watch_times_day)-min(a.watch_times_day) as watch_times_day_dif,(max(a.watch_times_day)-min(a.watch_times_day))/watch_times_means as watch_times_day_dif_std,(a.ft*sum(a.rn*a.watch_times_day)-sum(a.rn)*sum(a.watch_times_day))/(a.ft*sum(a.rn*a.rn)-sum(a.rn)*sum(a.rn)) as watch_times_trend
  from
  (
    select a.tv_no,a.watch_times_day,a.rn
        ,avg(a.watch_times_day) over(partition by a.tv_no order by a.rn rows between unbounded preceding and unbounded following) as watch_times_means
        ,sum(1) over(partition by a.tv_no order by a.rn rows between unbounded preceding and unbounded following) as ft
    from 
    (
        select a.tv_no,a.watch_times_day,row_number() over(partition by a.tv_no order by a.dd asc) as rn 
        from 
        (
            select a.tv_no,a.dd,round(sum(a.watch_times)/60,3) as watch_times_day
            from 
            (
                select a.tv_no,a.dy,a.dm,a.dd,a.start_time,a.stop_time,unix_timestamp(a.stop_time)-unix_timestamp(a.start_time) as watch_times
                from datamart.data_dmp_user_watch_detail a
                where a.tv_no in 
                (
                    select tv_no
                    from datamart.data_dmp_user_program_preference
                    where tv_no not like '2%' and tv_no not like '10%' and tv_no is not null and tv_no <> ''
                )
            ) a
            group by a.tv_no,a.dd
        ) a
    ) a
  ) a
  where a.ft >= 3
  group by a.tv_no,a.watch_times_means,a.ft
;

--上两表汇总，用户观看电视数据
drop table workspace.zb_watch_all;
create table workspace.zb_watch_all
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select a.tv_no
    ,a.mon_search_times
    ,a.six_no
    ,a.pay_no
    ,a.tv_pay
    ,a.pay_time
    ,a.demand_time
    ,a.demand_no
    ,a.back_time
    ,a.back_no
    ,b.watch_times_means
    ,b.watch_times_std
    ,b.watch_times_day_dif
    ,b.watch_times_day_dif_std
    ,b.watch_times_trend
    ,1 as watch
from workspace.zb_all_user_tv a
left join workspace.zb_tv_watch_time b
on a.tv_no = b.tv_no
;


select count(distinct a.tv_no) from zb_tv_watch_time a
where a.tv_no not in (
select tv_no
from datamart.data_dmp_user_program_preference
where substr(regexp_extract(movie_pre,'动漫:[0-9]\.?[0-9]*',0),4) > 0.3 
)
limit 20;
315916
看动漫比例在30% 以下或者不看的 259362

--数据分块：有宽带的用户数据(需要多找观看电视以及宽带使用情况的数据)，无宽带的用户数据(如果显示有宽带，但没有查到相关数据，则并入无宽带用户数据)

--提取字段：open_date(开户日期),age,网龄(onnet_month),sex,近三月动漫类月均消费(手机资费)，套外消费(main_prc_outfee)，近三月平均消费(avg_3mon_arpu)，近三月平均套外消费(avg_3mon_main_prc_outfee)，近三月平均套内消费(avg_3mon_main_prc_infee)，闲时流量(日数据)，非闲时流量(日数据)，上网次数，上网时长(data_user_flow)

--，电视账号当月使用时长(mon_dur)，是否存在营销活动(shifouyingxiao)，使用天数(online_days)，当月每日平均时长(day_avg_dur)，通话时长(通话时长越长的用户，看动漫概率越小)，通话次数，套外语音费(over_voc_fee)，短彩信费
套外溢出流量费(over_gprs_fee)，近三月平均溢出语音费(avg_3mon_over_voc_fee)，近三月平均溢出流量费(avg_3mon_over_gprs_fee)

--，观看电视节目时长，搜索动漫节目次数，近6月单点次数，近2月定包次数，电视包累计资费，电视包订购时间累计时长，最近90天点播观看时长，最近90天点播观看次数

搜索节目次数，近6月单点次数，近2月定包次数，电视包累计资费，电视订购包累计时长，90天回看时长，90回看次数，点播观看时长，点播观看次数
select count(distinct tv_no) from datamart.data_dmp_user_program_preference;
531537
select count(distinct tv_no) from datamart.data_dmp_user_watch_detail;
428742
--，常见视频App使用流量，视频App会员消费，外卖App使用


还未找的字段：
闲时流量(日数据 data_user_flow)，非闲时流量(日数据 data_user_flow)
，上网次数(日数据 data_dwb_cal_user_gprs_yx_ds)，上网时长(日数据 data_dwb_cal_user_gprs_yx_ds)
，通话时长(日数据 data_dwb_cal_user_voc_yx_ds)，通话次数(日数据 data_dwb_cal_user_voc_yx_ds)
，常见视频App使用流量，视频App会员消费，外卖App使用

--有宽带的用户数据
open_date(开户日期)，age，网龄(onnet_month)，sex，套外消费(main_prc_outfee)，近三月平均消费(avg_3mon_arpu)，近三月平均套外消费(avg_3mon_main_prc_outfee)，近三月平均套内消费(avg_3mon_main_prc_infee)，电视账号当月使用时长(mon_dur)，是否存在营销活动(shifouyingxiao)，使用天数(online_days)，当月每日平均时长(day_avg_dur)，

drop table workspace.zb_hdm_temp1;
create table workspace.zb_hdm_temp1
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select distinct a.phone_no
    ,a.open_date
    ,c.onnet_month
    ,c.sex
    ,c.main_prc_outfee
    ,c.avg_3mon_arpu
    ,c.avg_3mon_main_prc_infee
    ,c.avg_3mon_main_prc_outfee
    ,c.over_voc_fee,c.over_gprs_fee
    ,c.avg_3mon_over_voc_fee
    ,c.avg_3mon_over_gprs_fee
    ,b.mon_dur
    ,b.online_days
    ,b.day_avg_dur
    ,b.key_phone_no
    ,case when b.phone_no is not null then 1 else 0 end as shifoukuandai
    ,case when d.phone_no is not null then 1 else 0 end as shifouyingxiao
from 
(
    select distinct phone_no,open_date
    from datamart.data_user_info
    where dy = '2019' and dm = '12'
    and phone_no not like '2%' and phone_no not like '10%' and phone_no like '1%' and brand_id <> 'wl'
    and phone_no is not null and phone_no <> '' and run_code = 'A'
) a
left join
(
    select a.phone_no,a.key_phone_no,b.mon_dur,b.online_days,b.day_avg_dur
    from 
    (
        select * 
        from datamart.dw_user_kd
        where dy = '2019' and dm = '11' and group_type <> 'IMS0'
    ) a
    left join 
    (
        select * 
        from datamart.data_xmm_tv 
        where dy = '2019' and dm = '11'
    ) b
    on a.key_phone_no = b.tv_account
) b
on a.phone_no = b.phone_no
left join 
(
    select *
    from datamart.data_dm_uv_info_m
    where dy = '2019' and dm = '11'
) c
on a.phone_no = c.phone_no
left join
(
    select *
    from datamart.dw_user_act
) d
on a.phone_no = d.phone_no
;
--检验
select count(*) 
from workspace.zb_hdm_temp1
where mon_dur is null and shifoukuandai = 1
limit 20;
262398
select count(*) 
from workspace.zb_hdm_temp1
where shifoukuandai = 1
limit 20;
1357012
select count(distinct phone_no),count(phone_no) 
from workspace.zb_hdm_temp1
limit 20;
2761514 2761516
select phone_no,count(phone_no) 
from workspace.zb_hdm_temp1
group by phone_no
having count(phone_no) > 1
limit 20;
1377824GoEL 2
1528145anvu 2
select * 
from workspace.zb_hdm_temp1
where phone_no = '1528145anvu'
limit 20;
select * from datamart.dw_user_kd where phone_no = '1528145anvu' and dy = '2019' and dm = '11';


闲时流量(日数据 data_user_flow)，非闲时流量(日数据 data_user_flow)
，上网次数(日数据 data_dwb_cal_user_gprs_yx_ds)，上网时长(日数据 data_dwb_cal_user_gprs_yx_ds)
，通话时长(日数据 data_dwb_cal_user_voc_yx_ds)，通话次数(日数据 data_dwb_cal_user_voc_yx_ds)
，常见视频App使用流量，视频App会员消费，外卖App使用
drop table workspace.zb_hdm_temp2;
create table workspace.zb_hdm_temp2
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select a.phone_no
    ,b.call_times_gprs_means
    ,b.call_times_gprs_std
    ,b.call_times_gprs_dif
    ,b.call_times_gprs_dif_std
    ,b.call_times_gprs_trend
    ,b.call_duration_gprs_means
    ,b.call_duration_gprs_std
    ,b.call_duration_gprs_dif
    ,b.call_duration_gprs_dif_std
    ,b.call_duration_gprs_trend
    ,c.call_times_voc_means
    ,c.call_times_voc_std
    ,c.call_times_voc_dif
    ,c.call_times_voc_dif_std
    ,c.call_times_voc_trend
    ,c.call_duration_voc_means
    ,c.call_duration_voc_std
    ,c.call_duration_voc_dif
    ,c.call_duration_voc_dif_std
    ,c.call_duration_voc_trend
from 
(
    select distinct phone_no,open_date
    from datamart.data_user_info
    where dy = '2019' and dm = '12'
    and phone_no not like '2%' and phone_no not like '10%' and phone_no like '1%' and brand_id <> 'wl'
    and phone_no is not null and phone_no <> '' and run_code = 'A'
) a
left join
(
    select a.phone_no,a.call_times_gprs_means,a.call_duration_gprs_means,a.ft
    ,sqrt(sum(power(a.call_times_gprs_day-a.call_times_gprs_means,2)/(a.ft-1))) as call_times_gprs_std,max(a.call_times_gprs_day)-min(a.call_times_gprs_day) as call_times_gprs_dif,(max(a.call_times_gprs_day)-min(a.call_times_gprs_day))/call_times_gprs_means as call_times_gprs_dif_std,(a.ft*sum(a.rn*a.call_times_gprs_day)-sum(a.rn)*sum(a.call_times_gprs_day))/(a.ft*sum(a.rn*a.rn)-sum(a.rn)*sum(a.rn)) as call_times_gprs_trend
    ,sqrt(sum(power(a.call_duration_gprs_day-a.call_duration_gprs_means,2)/(a.ft-1))) as call_duration_gprs_std,max(a.call_duration_gprs_day)-min(a.call_duration_gprs_day) as call_duration_gprs_dif,(max(a.call_duration_gprs_day)-min(a.call_duration_gprs_day))/call_duration_gprs_means as call_duration_gprs_dif_std,(a.ft*sum(a.rn*a.call_duration_gprs_day)-sum(a.rn)*sum(a.call_duration_gprs_day))/(a.ft*sum(a.rn*a.rn)-sum(a.rn)*sum(a.rn)) as call_duration_gprs_trend
    from
    (
        select a.phone_no,a.call_times_gprs_day,a.call_duration_gprs_day,a.rn
        ,avg(a.call_times_gprs_day) over(partition by a.phone_no order by a.rn rows between unbounded preceding and unbounded following) as call_times_gprs_means
        ,avg(a.call_duration_gprs_day) over(partition by a.phone_no order by a.rn rows between unbounded preceding and unbounded following) as call_duration_gprs_means
        ,sum(1) over(partition by a.phone_no order by a.rn rows between unbounded preceding and unbounded following) as ft
        from 
        (
            select a.phone_no,a.call_times_gprs_day,a.call_duration_gprs_day
            ,row_number() over(partition by a.phone_no order by a.dd asc) as rn 
            from 
            (
                select phone_no,dd
                ,sum(a.call_times) as call_times_gprs_day
                ,round(sum(a.call_duration)/(60*1000),3) as call_duration_gprs_day
                from 
                (
                    select phone_no,call_times,call_duration,substr(data_date,1,4) as dy
                        ,substr(data_date,6,2) as dm,substr(data_date,9,10) as dd
                    from datamart.data_dwb_cal_user_gprs_yx_ds
                    where dy = '2019' and dm = '11'
                ) a
                group by a.phone_no,a.dd
            ) a
        ) a
    ) a
    where a.ft >= 3
    group by a.phone_no,call_times_gprs_means,a.call_duration_gprs_means,a.ft
) b
on a.phone_no = b.phone_no
left join
(
    select a.phone_no,a.call_times_voc_means,a.call_duration_voc_means,a.ft
        ,sqrt(sum(power(a.call_times_voc_day-a.call_times_voc_means,2)/(a.ft-1))) as call_times_voc_std,max(a.call_times_voc_day)-min(a.call_times_voc_day) as call_times_voc_dif,(max(a.call_times_voc_day)-min(a.call_times_voc_day))/call_times_voc_means as call_times_voc_dif_std,(a.ft*sum(a.rn*a.call_times_voc_day)-sum(a.rn)*sum(a.call_times_voc_day))/(a.ft*sum(a.rn*a.rn)-sum(a.rn)*sum(a.rn)) as call_times_voc_trend
        ,sqrt(sum(power(a.call_duration_voc_day-a.call_duration_voc_means,2)/(a.ft-1))) as call_duration_voc_std,max(a.call_duration_voc_day)-min(a.call_duration_voc_day) as call_duration_voc_dif,(max(a.call_duration_voc_day)-min(a.call_duration_voc_day))/call_duration_voc_means as call_duration_voc_dif_std,(a.ft*sum(a.rn*a.call_duration_voc_day)-sum(a.rn)*sum(a.call_duration_voc_day))/(a.ft*sum(a.rn*a.rn)-sum(a.rn)*sum(a.rn)) as call_duration_voc_trend
    from
    (
        select a.phone_no,a.call_times_voc_day,a.call_duration_voc_day,a.rn
            ,avg(a.call_times_voc_day) over(partition by a.phone_no order by a.rn rows between unbounded preceding and unbounded following) as call_times_voc_means
            ,avg(a.call_duration_voc_day) over(partition by a.phone_no order by a.rn rows between unbounded preceding and unbounded following) as call_duration_voc_means
            ,sum(1) over(partition by a.phone_no order by a.rn rows between unbounded preceding and unbounded following) as ft
        from 
        (
            select a.phone_no,a.call_times_voc_day,a.call_duration_voc_day
                ,row_number() over(partition by a.phone_no order by a.dd asc) as rn 
            from 
            (
                select phone_no,dd
                    ,sum(a.call_times) as call_times_voc_day
                    ,round(sum(a.call_duration)/60,3) as call_duration_voc_day
                from 
                (
                    select phone_no,call_times,call_duration,dd
                    from datamart.data_dwb_cal_user_voc_yx_ds
                    where dy = '2019' and dm = '11'
                ) a
                group by a.phone_no,a.dd
            ) a
        ) a
    ) a
    where a.ft >= 3
    group by a.phone_no,call_times_voc_means,a.call_duration_voc_means,a.ft
) c
on a.phone_no = c.phone_no
;



--常见视频App使用流量，视频App会员消费，外卖App使用
drop table workspace.zb_hdm_flow_temp3;
create table workspace.zb_hdm_flow_temp3
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
    select phone_no,dd,round((sum(flow)/1024),0) as flow_day
    from (
        select phone_no,flow,dd
        from 
        (
            select phone_no
               ,flow
               ,times
               ,time_duration
               ,app_id
               ,dd
            from  datamart.data_dw_xdr_gprs
            where dy='2019' and dm='12' and
            (app_id = '05-7210'
            or app_id = '05-0071'
            or app_id = '5-16'
            or app_id = '05-48924'
            or app_id = '05-0002'
            or app_id = '05-0003'
            or app_id = '5-6'
            or app_id = '05-0094'
            or app_id = '05-0060'
            or app_id = '10-9'
            or app_id = '05-0006'
            or app_id = '05-0060'
            or app_id = '5-60'
            )
        ) a
    ) c
    group by phone_no,dd
;
--常用视频app 均值 标准差 振幅 振幅/均值  趋势
drop table workspace.zb_hdm_flow_temp4;
create table workspace.zb_hdm_flow_temp4
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  select a.phone_no,a.flow_means,a.ft,sqrt(sum(power(a.flow_day-a.flow_means,2)/(a.ft-1))) as flow_std,max(a.flow_day)-min(a.flow_day) as flow_day_dif,(max(a.flow_day)-min(a.flow_day))/flow_means as flow_day_dif_std,(a.ft*sum(a.rn*a.flow_day)-sum(a.rn)*sum(a.flow_day))/(a.ft*sum(a.rn*a.rn)-sum(a.rn)*sum(a.rn)) as flow_trend
  from
  (
    select a.phone_no,a.flow_day,a.rn
        ,avg(a.flow_day) over(partition by a.phone_no order by a.rn rows between unbounded preceding and unbounded following) as flow_means
        ,sum(1) over(partition by a.phone_no order by a.rn rows between unbounded preceding and unbounded following) as ft
    from 
    (
        select a.phone_no,a.flow_day,row_number() over(partition by a.phone_no order by a.dd asc) as rn 
        from 
        (
            select a.phone_no,dd,flow_day
            from workspace.zb_hdm_flow_temp3 a
        ) a
    ) a
  ) a
  where a.ft >= 3
  group by a.phone_no,a.flow_means,a.ft
;

--汇总表
drop table workspace.zb_dm_last;
create table workspace.zb_dm_last
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select  distinct a.phone_no
    ,a.open_date
    ,cast(a.onnet_month/12.0 as double) as onnet_month
    ,case when a.sex = '男' then 1 else 0 end as sex
    ,cast(a.main_prc_outfee as double) as main_prc_outfee
    ,cast(a.avg_3mon_arpu as double) as avg_3mon_arpu
    ,cast(a.avg_3mon_main_prc_infee as double) as avg_3mon_main_prc_infee
    ,cast(a.avg_3mon_main_prc_outfee as double) as avg_3mon_main_prc_outfee
    ,cast(a.over_voc_fee as double) as over_voc_fee
    ,cast(a.over_gprs_fee as double) as over_gprs_fee
    ,cast(a.avg_3mon_over_voc_fee as double) as avg_3mon_over_voc_fee
    ,cast(a.avg_3mon_over_gprs_fee as double) as avg_3mon_over_gprs_fee
    ,cast(a.mon_dur as double) as mon_dur
    ,a.online_days
    ,a.day_avg_dur
    ,a.key_phone_no
    ,a.shifoukuandai
    ,a.shifouyingxiao 
    ,b.call_times_gprs_means
    ,b.call_times_gprs_std
    ,b.call_times_gprs_dif
    ,b.call_times_gprs_dif_std
    ,b.call_times_gprs_trend
    ,b.call_duration_gprs_means
    ,b.call_duration_gprs_std
    ,b.call_duration_gprs_dif
    ,b.call_duration_gprs_dif_std
    ,b.call_duration_gprs_trend
    ,b.call_times_voc_means
    ,b.call_times_voc_std
    ,b.call_times_voc_dif
    ,b.call_times_voc_dif_std
    ,b.call_times_voc_trend
    ,b.call_duration_voc_means
    ,b.call_duration_voc_std
    ,b.call_duration_voc_dif
    ,b.call_duration_voc_dif_std
    ,b.call_duration_voc_trend
    ,c.flow_means
    ,c.flow_std
    ,c.flow_day_dif
    ,c.flow_day_dif_std
    ,c.flow_trend
    ,d.mon_search_times
    ,d.six_no
    ,d.pay_no
    ,d.tv_pay
    ,d.pay_time
    ,d.demand_time
    ,d.demand_no
    ,d.back_time
    ,d.back_no
    ,d.watch_times_means
    ,d.watch_times_std
    ,d.watch_times_day_dif
    ,d.watch_times_day_dif_std
    ,d.watch_times_trend
    ,case when d.tv_no is not null then d.watch else 0 end as watch
from workspace.zb_hdm_temp1 a
left join workspace.zb_hdm_temp2 b
on a.phone_no = b.phone_no
left join workspace.zb_hdm_flow_temp4 c
on a.phone_no = c.phone_no
left join workspace.zb_watch_all d
on a.key_phone_no = d.tv_no
;
2761516

--动漫用户表
drop table workspace.zb_dmph_user;
create table workspace.zb_dmph_user
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select a.*,substr(regexp_extract(movie_pre,'动漫:[0-9]\.?[0-9]*',0),4) as dmph
from datamart.data_dmp_user_program_preference a
where substr(regexp_extract(movie_pre,'动漫:[0-9]\.?[0-9]*',0),4) > 0.3 
and  tv_no not like '2%' and tv_no not like '10%' and tv_no is not null and tv_no <> ''
;


select count(*)
from datamart.data_dmp_user_program_preference a
where substr(regexp_extract(movie_pre,'动漫:[0-9]\.?[0-9]*',0),4) <= 0.3
and  substr(regexp_extract(movie_pre,'动漫:[0-9]\.?[0-9]*',0),4) > 0
and  tv_no not like '2%' and tv_no not like '10%' and tv_no is not null and tv_no <> ''
;
82295 （仅只有主账号）
select count(*) 
from workspace.zb_dm_last a
where a.key_phone_no in
(
    select a.tv_no
    from datamart.data_dmp_user_program_preference a
    where substr(regexp_extract(a.movie_pre,'动漫:[0-9]\.?[0-9]*',0),4) <= 0.3
    and  substr(regexp_extract(a.movie_pre,'动漫:[0-9]\.?[0-9]*',0),4) > 0
    and  a.tv_no not like '2%' and a.tv_no not like '10%' and a.tv_no is not null and a.tv_no <> ''
) ;
177909 (包括成员账号) --待定用户，且有观看的记录
select count(*)
from datamart.data_dmp_user_program_preference a
where substr(regexp_extract(movie_pre,'动漫:[0-9]\.?[0-9]*',0),4) = ''
and  tv_no not like '2%' and tv_no not like '10%' and tv_no is not null and tv_no <> ''
limit 5;
283099（仅只有主账号）
select count(*) 
from workspace.zb_dm_last a
where a.key_phone_no in
(
    select a.tv_no
    from datamart.data_dmp_user_program_preference a
    where substr(regexp_extract(a.movie_pre,'动漫:[0-9]\.?[0-9]*',0),4) = ''
    and  a.tv_no not like '2%' and a.tv_no not like '10%' and a.tv_no is not null and a.tv_no <> ''
) ;
581796（包括了成员账号） --认为是不看动漫的用户(因其有电视，但从未看过动漫类节目)
select count(*)
from datamart.data_dmp_user_program_preference a
where substr(regexp_extract(movie_pre,'动漫:[0-9]\.?[0-9]*',0),4) > 0.3
and  tv_no not like '2%' and tv_no not like '10%' and tv_no is not null and tv_no <> ''
limit 5;
75186（仅只有主账号）
select count(*) 
from workspace.zb_dm_last a
where a.key_phone_no in
(
    select a.tv_no
    from datamart.data_dmp_user_program_preference a
    where substr(regexp_extract(a.movie_pre,'动漫:[0-9]\.?[0-9]*',0),4) > 0.3
    and  a.tv_no not like '2%' and a.tv_no not like '10%' and a.tv_no is not null and a.tv_no <> ''
) ;
160191 (包括成员账号) --认为是动漫用户，看动漫的比例达30%以上
--训练样本
drop table workspace.zb_dm_train;
create table workspace.zb_dm_train
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select a.*, 1 as dm_label
from workspace.zb_dm_last a
where a.key_phone_no in
(
    select a.tv_no
    from datamart.data_dmp_user_program_preference a
    where substr(regexp_extract(a.movie_pre,'动漫:[0-9]\.?[0-9]*',0),4) > 0.3
    and  a.tv_no not like '2%' and a.tv_no not like '10%' and a.tv_no is not null and a.tv_no <> ''
)
union all
select a.*, 0 as dm_label
from workspace.zb_dm_last a
where a.key_phone_no in
(
    select a.tv_no
    from datamart.data_dmp_user_program_preference a
    where substr(regexp_extract(a.movie_pre,'动漫:[0-9]\.?[0-9]*',0),4) = ''
    and  a.tv_no not like '2%' and a.tv_no not like '10%' and a.tv_no is not null and a.tv_no <> ''
)
;


--predict_one 有观看记录的
drop table workspace.zb_dm_predict_one;
create table workspace.zb_dm_predict_one
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select * 
from workspace.zb_dm_last a
where a.key_phone_no in
(
    select a.tv_no
    from datamart.data_dmp_user_program_preference a
    where substr(regexp_extract(a.movie_pre,'动漫:[0-9]\.?[0-9]*',0),4) <= 0.3
    and  substr(regexp_extract(a.movie_pre,'动漫:[0-9]\.?[0-9]*',0),4) > 0
    and  a.tv_no not like '2%' and a.tv_no not like '10%' and a.tv_no is not null and a.tv_no <> ''
) ;177909
--predict_two 无观看记录的
drop table workspace.zb_dm_predict_two;
create table workspace.zb_dm_predict_two
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select * 
from workspace.zb_dm_last a
where a.key_phone_no not in
(
    select a.tv_no
    from datamart.data_dmp_user_program_preference a
) ;




--生成样本数据
drop table workspace.zb_dmph_user;
create table workspace.zb_dmph_user
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as

select count(*) 
from workspace.zb_dm_last a
where a.key_phone_no in 
(
select a.tv_no
from datamart.data_dmp_user_program_preference a 
where a.tv_no not like '2%' and a.tv_no not like '10%' and a.tv_no is not null and a.tv_no <> ''
) ;
select count(a.tv_no)
from datamart.data_dmp_user_program_preference a 
where a.tv_no not like '2%' and a.tv_no not like '10%' and a.tv_no is not null and a.tv_no <> ''

select count(distinct phone_no),count(distinct a.key_phone_no) 
from workspace.zb_dm_last a
where a.key_phone_no in 
(
    select tv_no
    from workspace.zb_dmph_user
) 
;


select phone_no,count(phone_no)
from workspace.zb_dm_last a
where a.phone_no in 
(
    select tv_no
    from workspace.zb_dmph_user
) 
group by phone_no
having count(phone_no) > 1
limit 5
;
1528382zJdQ 2
1518101qJcA 2
1377823RJWA 2
1830840akbg 2
1588381zwSA 2
select * from workspace.zb_dm_last
where phone_no = '1528382zJdQ';
select *
from datamart.data_dw_xdr_gprs
where dy = '2019' and dm = '12' limit 5;


select *
from datamart.data_dwb_cal_user_voc_yx_ds
where phone_no = '1589247qNbt' and dy ='2019' and dm = '11' and dd = '30';

select tv_no
from datamart.data_dmp_user_program_preference
where substr(regexp_extract(movie_pre,'动漫:[0-9]\.?[0-9]*',0),4) > 0.3
and tv_no not like '2%' and tv_no not like '10%' and tv_no is not null and tv_no <> ''




select *
from datamart.data_dmp_user_program_preference
where kd_no = '2091789qwWZ'
;





select *
from data_dmp_user_program_preference
where movie_pre not like '%动漫%' and movie_pre <> ''
limit 5;
348821
select *
from data_dmp_user_program_preference
where movie_pre = ''
limit 10;


--外卖  
select *
from datamart.data_dw_xdr_gprs_app 
where app_name like '%口碑%';
select distinct app_type
from datamart.data_dw_xdr_gprs_app
limit 2;
15-327  浏览下载    美团外卖
18-480  其他  美团酒店
18-0008 购物  美团
15-0327 浏览下载    美团外卖
15-70   浏览下载    美团
21-7080 其他  美团酒店

15-326  浏览下载    百度外卖
15-327  浏览下载    美团外卖
15-8345 浏览下载    我有外卖
15-45808    浏览下载    乐外卖
15-7654 浏览下载    拼豆夜宵外卖
21-7461 其他  外卖超人
18-2966 其他  外卖超人
15-0326 浏览下载    百度外卖
15-0327 浏览下载    美团外卖
15-2859 浏览下载    上美家外卖
15-8446 浏览下载    零号线外卖
15-2240 浏览下载    零号线外卖
15-8983 浏览下载    上美家外卖



select count(*),count(distinct phone_no)
from datamart.data_dw_xdr_gprs a
where dy = '2019' and dm = '12'
and a.app_id in (
    select app_id 
    from datamart.data_dw_xdr_gprs_app 
    where app_name like '%动漫%'
)
;
710570 140621 --15号一天的数据

--订购过动漫业务的人
select count(distinct a.phone_no)
from 
(
    select a.* 
    from datamart.data_user_prc a
    where a.phone_no in(
        select a.tv_no
        from datamart.data_dmp_user_program_preference a
        where a.tv_no not like '2%' and a.tv_no not like '10%' and a.tv_no is not null and a.tv_no <> ''
        )
) a
inner join 
(
    select *
    from datamart.base_prc_info
    where prod_prc_name like '%动漫%'
) b
on a.prod_prcid = b.prod_prcid 
;
1739
select count(distinct app_id,app_type,app_name)
from datamart.data_dw_xdr_gprs_app 
where app_type like '%动漫%'
;

select app_id,app_type,app_name 
from datamart.data_dw_xdr_gprs_app 
where app_name like '%动漫%' and app_type = '视频';

select count(distinct a.phone_no,a.app_id)
from datamart.data_dw_xdr_gprs a
where dy = '2019' and dm = '12'
and app_id = '21-7926';



select count(*),count(distinct phone_no)
from datamart.data_dw_xdr_gprs a
where dy = '2019' and dm = '12'
and  (app_id = '08-45914' or app_id = '08-7559' or app_id = '8-315' or app_id = '21-7926' or app_id = '18-1937'
or app_id = '08-0225' or app_id = '15-8912' or app_id = '15-2780' or app_id = '15-45678' or app_id = '08-7137' or app_id = '8-461')
21-7926 其他  pixiv社区
18-1937 其他  pixiv社区
08-7012 游戏  崩坏学院
8-315   游戏  崩坏学院
08-45914    游戏  崩坏学园2 2645
08-7559 游戏  崩坏学园2
8-895   游戏  崩坏学园2
08-0225 游戏  阴阳师网易官方版
15-8912 浏览下载    阴阳师同人圈
15-2780 浏览下载    阴阳师同人圈 7417
15-45678    浏览下载    战舰少女官网
08-7137 游戏  战舰少女R
8-461   游戏  战舰少女R 7458
08-0021 游戏  剑侠情缘
08-7474 游戏  克鲁赛德战记
8-816   游戏  克鲁赛德战记

10-302  动漫  火影忍者中文网
10-0002 动漫  火影忍者中文网
15-8934 浏览下载    火影忍者动态壁纸
08-0222 游戏  火影忍者
08-7140 游戏  火影忍者
8-464   游戏  火影忍者
8-663   游戏  龙珠时代
08-7494 游戏  龙珠炫斗
08-7325 游戏  龙珠时代


select distinct app_type
from 
(
    select *  
    from datamart.data_dw_xdr_gprs a
    where dy = '2019' and dm = '12'
) a
left join datamart.data_dw_xdr_gprs_app b
on a.app_id = b.app_id
limit 100
;
动漫类型的app
10-332  动漫  人人漫画家
10-326  动漫  第一弹
10-320  动漫  新漫画
10-334  动漫  布丁动画
10-338  动漫  BL漫画
10-40670    动漫  Mangago动漫阅读
10-0004 动漫  漫漫漫画
10-0001 动漫  咪咕动漫
10-314  动漫  猫团动漫
10-0002 动漫  火影忍者中文网
--动漫类app用户，流量，次数
drop table workspace.zb_dm_appflow_1;
create table workspace.zb_dm_appflow_1
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select *  
from 
(
    select *
    from datamart.data_dw_xdr_gprs
    where dy = '2019' and dm = '12' and dd = '31'
) a
left semi join
(
    select app_id 
    from datamart.data_dw_xdr_gprs_app 
    where app_name like '%动漫%'
) b
on a.app_id = b.app_id
;


drop table workspace.zb_dm_appflow_all;
create table workspace.zb_dm_appflow
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  select distinct *
  from 
  (
    select * 
    from workspace.zb_dm_appflow_1
    union all
    select * 
    from workspace.zb_dm_appflow_2
    union all
    select * 
    from workspace.zb_dm_appflow_3
    union all
    select * 
    from workspace.zb_dm_appflow_4
    union all
    select * 
    from workspace.zb_dm_appflow_5
    union all
    select * 
    from workspace.zb_dm_appflow_6
) a
;


zb_dm_train