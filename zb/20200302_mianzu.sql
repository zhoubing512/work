drop table workspace.zb_mianzu_phoneNo_20200302;
create table workspace.zb_mianzu_phoneNo_20200302(name string,phone_no string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
load data local inpath '/mnt/disk1/user/zhoubing/temp_files/phone_no.txt' into table  workspace.zb_mianzu_phoneNo_20200302;


--腾讯，优酷，爱奇艺，芒果，乐视，土豆，百度，哔哩哔哩
select *
from datamart.data_dw_xdr_gprs_app
where app_name like '%哔哩哔哩%' or app_name like '%bilibili%'
;


05-0016 视频  芒果TV
5-16    视频  芒果TV
05-0017 视频  爱奇艺视频
5-17    视频  奇艺影音
05-0006 视频  腾讯视频
5-6 视频  腾讯视频
05-0003 视频  优酷
5-3 视频  优酷网
05-0014 视频  乐视视频
5-14    视频  乐视网
5-399   视频  乐视视频
05-0004 视频  土豆视频
5-4 视频  土豆网
05-0060 视频  百度视频
5-60    视频  百度视频
05-48924    视频  哔哩哔哩弹幕视频网
05-0071 视频  哔哩哔哩动画



drop table workspace.zb_gprs_20200302_shipin;
create table workspace.zb_gprs_20200302_shipin
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  select a.phone_no,a.shipin_type,sum(a.flow) as flow_sum
  from 
  (
      select a.phone_no,a.flow
            ,case when app_id = '05-0016' or app_id = '5-16' then '芒果tv'
                when app_id = '05-0017' or app_id = '5-17' then '爱奇艺'
                when app_id = '05-0006' or app_id = '5-6' then '腾讯视频'
                when app_id = '05-0003' or app_id = '5-3' then '优酷'
                when app_id = '05-0014' or app_id = '5-14' or app_id = '5-399' then '乐视'
                when app_id = '05-0004' or app_id = '5-4' then '土豆视频'
                when app_id = '05-0060' or app_id = '5-60' then '百度视频'
                when app_id = '05-48924' or app_id = '05-0071' then '哔哩哔哩'
                else 'qita' end as shipin_type
      from 
      (
        select *
        from datamart.data_dw_xdr_gprs
        where dy = '2020' and dm = '02'
        and (app_id = '05-0016'
        or app_id = '5-16'
        or app_id = '05-0017'
        or app_id = '5-17'
        or app_id = '05-0006'
        or app_id = '5-6'
        or app_id = '05-0003'
        or app_id = '5-3'
        or app_id = '05-0014'
        or app_id = '5-14'
        or app_id = '5-399'
        or app_id = '05-0004'
        or app_id = '5-4'
        or app_id = '05-0060'
        or app_id = '5-60'
        or app_id = '05-48924'
        or app_id = '05-0071'
        )
      ) a
  ) a
  group by a.phone_no,a.shipin_type
;

drop table workspace.zb_mianzu_20200302_result;
create table workspace.zb_mianzu_20200302_result
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select a.name,a.phone_no,b.main_prod_prcid,b.prod_prc_name
    ,c.prod_prcid_main as ciyue_prod_prcid_main
    ,c.prod_prc_name_main as ciyue_prod_prc_name_main
    ,b.avg_3mon_arpu,b.avg_3mon_dou
    ,d.shipin_type
from workspace.th_encrypt_zb_mianzu_phoneno_20200302 a
left join
(
    select distinct a.phone_no,a.main_prod_prcid,a.avg_3mon_arpu,a.avg_3mon_dou,b.prod_prc_name
    from 
    (
        select *
        from datamart.data_dm_uv_info_m
        where dy = '2020' and dm = '01'
    ) a
    left join datamart.base_prc_info b
    on a.main_prod_prcid = b.prod_prcid 
) b
on a.phone_no = b.phone_no
left join datamart.dw_user_prc_main_latest c
on a.phone_no = c.phone_no
left join
(
    select a.phone_no,a.shipin_type
    from 
    (
    select phone_no,shipin_type,row_number() over(partition by phone_no order by flow_sum desc) rn
    from workspace.zb_gprs_20200302_shipin
    ) a
    where a.rn = 1
) d
on a.phone_no = d.phone_no
;



select a.* ,b.prod_prcid_main,b.prod_prc_name_main,b.data_date
from workspace.th_encrypt_zb_mianzu_phoneno_20200302 a
left join
datamart.dw_user_prc_main_latest b
on a.phone_no = b.phone_no
;