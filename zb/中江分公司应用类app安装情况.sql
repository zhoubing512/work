--中江分公司

--中间表，所有开通了国漫或者省漫的用户
drop table workspace.app_zhongjian1;
create table workspace.app_zhongjian1
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select a.phone_no
,case when guoman_sum > 0 then 1 else 0 end as guoman
,case when shengman_sum > 0 then 1 else 0 end as shengman
from
(
    select a.phone_no,sum(a.guoman) as guoman_sum,sum(a.shengman) as shengman_sum
    from
    (
        select a.phone_no,a.prod_prcid,b.guoman,b.shengman
        from
        (
            select phone_no,prod_prcid,exp_time 
            from datamart.data_user_prc 
            where phone_no like '1%' and substr(exp_time,1,4) >= 2019
        ) a
        inner join 
        (
            select prod_prcid
            ,case when prod_prc_name like '%国%漫游%' then 1 else 0 end guoman
            ,case when prod_prc_name like '%省%漫游%' then 1 else 0 end shengman
            from datamart.base_prc_info
            where prod_prc_name like '%国%漫游%' or prod_prc_name like '%省%漫游%'
        ) b
        on a.prod_prcid = b.prod_prcid
    ) a
    group by phone_no
) a
;
--中间表2
drop table workspace.app_zhongjian2;
create table workspace.app_zhongjian2
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select a.phone_no,a.app_id
from 
(
    select distinct a.phone_no,a.app_id
    from datamart.data_dw_xdr_gprs a
    where dy = '2019' and dm in ('12','11') 
    and a.phone_no in (
                        select distinct phone_no
                        from datamart.data_dm_uv_info_m 
                        where county_id = '115'
                    )
) a
where a.app_id in
( 
    select app_id 
    from datamart.data_dw_xdr_gprs_app
    where app_name like '%百度网盘%' or app_name like '%天翼云%' or app_name like '%小米服务%' 
        or  app_name like '%华为云服务%' or  app_name like '%华为服务%' or  app_name like '%华为网盘%' 
        or  app_name like '%华为桌面云%' or  app_name like '%携程%' or  app_name like '%途牛%' 
        or  app_name like '%去哪儿%' or  app_name like '%Airbnb%' or  app_name like '%缤客%' 
        or  app_name like '%蚂蜂窝%' or  app_name like '%淘在路上%' or app_name like '%穷游%' 
        or  app_name like '%蝉游%' or app_name like '%驴妈妈%' or app_name like '%阿里旅行%' 
        or app_name like '%飞猪旅行%' or  app_name like '%骑记%' or  app_name like '%美拍%' 
        or  app_name like '%美图秀秀%' 
)
;

--中间表3
--百度云，天翼云，小米云，华为云，携程，途牛，去哪儿，爱彼迎，BOOKing.com缤客，马蜂窝，淘在路上社区，穷游，蝉游记，驴妈妈，阿里旅行，骑记，美拍，美图秀秀

drop table workspace.app_zhongjian3;
create table workspace.app_zhongjian3
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  select a.phone_no
        ,sum(case when app_time=1 then app_times ELSE 0 END) as baiduyun
        ,sum(case when app_time=2 then app_times ELSE 0 END) as tianyiyun
        ,sum(case when app_time=3 then app_times ELSE 0 END) as xiaomiyun
        ,sum(case when app_time=4 then app_times ELSE 0 END) as huaweiyun
        ,sum(case when app_time=5 then app_times ELSE 0 END) as xiecheng
        ,sum(case when app_time=6 then app_times ELSE 0 END) as tuniu
        ,sum(case when app_time=7 then app_times ELSE 0 END) as qunaer
        ,sum(case when app_time=8 then app_times ELSE 0 END) as aibiying
        ,sum(case when app_time=9 then app_times ELSE 0 END) as BOOKing
        ,sum(case when app_time=10 then app_times ELSE 0 END) as mafengwo
        ,sum(case when app_time=11 then app_times ELSE 0 END) as taozailushang
        ,sum(case when app_time=12 then app_times ELSE 0 END) as qiongyou
        ,sum(case when app_time=13 then app_times ELSE 0 END) as chanyouji
        ,sum(case when app_time=14 then app_times ELSE 0 END) as lvmama
        ,sum(case when app_time=15 then app_times ELSE 0 END) as alilvyou
        ,sum(case when app_time=16 then app_times ELSE 0 END) as qiji
        ,sum(case when app_time=17 then app_times ELSE 0 END) as meipai
        ,sum(case when app_time=18 then app_times ELSE 0 END) as meituxiuxiu
  from
  (
      select a.phone_no,a.app_time,count(app_time) as app_times
      from
      (
            select a.phone_no
                ,case 
                when a.app_name like '%百度云盘%' then 1 
                when a.app_name like '%天翼云%' then 2
                when a.app_name like '%小米服务%' then 3
                when app_name like '%华为云服务%' or  app_name like '%华为服务%' or  app_name like '%华为网盘%' 
                        or  app_name like '%华为桌面云%' then 4
                when a.app_name like '%携程%' then 5
                when a.app_name like '%途牛%' then 6
                when a.app_name like '%去哪儿%' then 7
                when a.app_name like '%Airbnb%' then 8
                when a.app_name like '%缤客%' then 9
                when a.app_name like '%蚂蜂窝%' then 10
                when a.app_name like '%淘在路上%' then 11
                when a.app_name like '%穷游%' then 12
                when a.app_name like '%蝉游%' then 13
                when a.app_name like '%驴妈妈%' then 14
                when a.app_name like '%阿里旅行%' or a.app_name like '%飞猪旅行%' then 15
                when a.app_name like '%骑记%' then 16
                when a.app_name like '%美拍%' then 17
                when a.app_name like '%美图秀秀%' then 18
                else 0 end as app_time
            from
            (
                select a.phone_no,b.app_name 
                from workspace.app_zhongjian2 a 
                left join 
                ( 
                    select app_id,app_name
                    from datamart.data_dw_xdr_gprs_app
                    where app_name like '%百度网盘%' or app_name like '%天翼云%' or app_name like '%小米服务%' 
                        or  app_name like '%华为云服务%' or  app_name like '%华为服务%' or  app_name like '%华为网盘%' 
                        or  app_name like '%华为桌面云%' or  app_name like '%携程%' or  app_name like '%途牛%' 
                        or  app_name like '%去哪儿%' or  app_name like '%Airbnb%' or  app_name like '%缤客%' 
                        or  app_name like '%蚂蜂窝%' or  app_name like '%淘在路上%' or app_name like '%穷游%' 
                        or  app_name like '%蝉游%' or app_name like '%驴妈妈%' or app_name like '%阿里旅行%' 
                        or app_name like '%飞猪旅行%' or  app_name like '%骑记%' or  app_name like '%美拍%' 
                        or  app_name like '%美图秀秀%' 
                ) b on a.app_id = b.app_id
            ) a
        ) a
      group by a.phone_no,a.app_time
    ) a
    group by a.phone_no
;


--所有中江开户用户
drop table workspace.app_anzhuang;
create table workspace.app_anzhuang
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  select * 
  from
  (
        select a.phone_no,b.prod_prc_name_main,b.prod_prc_price_main
            ,case when c.phone_no is not null then c.guoman else 0 end as guoman
            ,case when c.phone_no is not null then c.shengman else 0 end as shengman
            ,case when d.phone_no is not null then d.baiduyun else 0 end as baiduyun
            ,case when d.phone_no is not null then d.tianyiyun else 0 end as tianyiyun
            ,case when d.phone_no is not null then d.xiaomiyun else 0 end as xiaomiyun
            ,case when d.phone_no is not null then d.huaweiyun else 0 end as huaweiyun
            ,case when d.phone_no is not null then d.xiecheng else 0 end as xiecheng
            ,case when d.phone_no is not null then d.tuniu else 0 end as tuniu
            ,case when d.phone_no is not null then d.qunaer else 0 end as qunaer
            ,case when d.phone_no is not null then d.aibiying else 0 end as aibiying
            ,case when d.phone_no is not null then d.BOOKing else 0 end as BOOKing
            ,case when d.phone_no is not null then d.mafengwo else 0 end as mafengwo
            ,case when d.phone_no is not null then d.taozailushang else 0 end as taozailushang
            ,case when d.phone_no is not null then d.qiongyou else 0 end as qiongyou
            ,case when d.phone_no is not null then d.chanyouji else 0 end as chanyouji
            ,case when d.phone_no is not null then d.lvmama else 0 end as lvmama
            ,case when d.phone_no is not null then d.alilvyou else 0 end as alilvyou
            ,case when d.phone_no is not null then d.qiji else 0 end as qiji
            ,case when d.phone_no is not null then d.meipai else 0 end as meipai
            ,case when d.phone_no is not null then d.meituxiuxiu else 0 end as meituxiuxiu
        from
        (--所有中江开户用户
            select distinct phone_no
            from datamart.data_dm_uv_info_m 
            where county_id = '115'
        ) a
        left join datamart.dw_user_prc_main_latest b
        on a.phone_no = b.phone_no
        left join workspace.app_zhongjian1 c
        on a.phone_no = c.phone_no
        left join workspace.app_zhongjian3 d
        on a.phone_no = d.phone_no
    ) a where a.prod_prc_name_main is not null
;


select * from workspace.app_anzhuang 
where baiduyun > 0 or tianyiyun > 0  or huaweiyun > 0 or xiecheng > 0 or tuniu > 0 or qunaer > 0 or aibiying > 0 or BOOKing > 0 or mafengwo > 0 or taozailushang > 0 or qiongyou > 0 or chanyouji > 0 or lvmama > 0 or alilvyou > 0 or qiji > 0 or meipai > 0 or meituxiuxiu > 0 or xiaomiyun > 0
limit 50
;

select count(*) from workspace.app_anzhuang where shengman > 0;

select * from datamart.dw_user_prc_main_latest
where phone_no = '1341900qsMQ';

1345898FJSL
1341900qsMQ

select * from datamart.data_dm_uv_info_m where phone_no = '1377824pHEA';

1344014lHcZ NULL    NULL    0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0      0
1345849TUdA NULL    NULL    0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0      0
1355061poMr NULL    NULL    0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0      0
1355062aoWr NULL    NULL    0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0      0
1356822lwjy NULL    NULL    0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0      0
1377824pHEA NULL    NULL    0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0      0
1377828TIbA NULL    NULL    0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0      0
1377828YwjP NULL    NULL    0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0      0
1377842FNWt NULL    NULL    0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0      0
1388102aJdu NULL    NULL    0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0      0
1389027RNEQ NULL    NULL    0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0      0
1390810aHvr NULL    NULL    0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0      0
1398102GwSA NULL    NULL    0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0      0
1398109Goju NULL    NULL    0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0      0
1518109lISr NULL    NULL    0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0      0
1518382awcu NULL    NULL    0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0      0
1577554RNbL NULL    NULL    0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0      0
1577554pkMZ NULL    NULL    0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0      0
1577556qncQ NULL    NULL    0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0   0      0
1588361ToMZ 
select count(*) from workspace.app_anzhuang  where alilvyou =1;


select * from data_user_prc where phone_no like '1%' limit 5;


select * 
from datamart.base_prc_info
where prod_prc_name like '%省%漫游%'
limit 20
;


select distinct city_id,city_name,county_id,county_name 
from base_channel_info
where city_name like '%德阳%'
where county_name like '%罗江%'
limit 1;

--百度云，天翼云，小米云，华为云，携程，途牛，去哪儿，爱彼迎，缤客，马蜂窝，淘在路上社区，穷游，蝉游记，驴妈妈，阿里旅行，骑记，美拍，美图秀秀
select app_id ,app_name
from datamart.data_dw_xdr_gprs_app
where app_name like '%骑记%' or app_name like '%icycle%';

百度网盘，天翼云，天翼云盘，小米服务，华为桌面云，华为网盘，华为云服务，华为服务，Airbnb，蚂蜂窝，淘在路上，蝉游，驴妈妈，飞猪旅行