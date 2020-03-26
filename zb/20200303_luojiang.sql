
drop table workspace.zb_luojiang_20200303;
create table workspace.zb_luojiang_20200303
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select distinct a.phone_no,a.chl_name,a.dm,b.prod_prc_name
from 
(
    select phone_no,chl_name,substr(create_date,6,2) as dm 
    from datamart.data_user_channel
    where dy = '2019' and county_id = '58' and substr(create_date,1,4) = '2019'
    and phone_no like '1%' and phone_no not like '10%'
) a
left join 
(
select distinct a.phone_no,b.prod_prc_name 
from datamart.data_dm_uv_info_m a
left join datamart.base_prc_info b
on a.main_prod_prcid = b.prod_prcid
) b
on a.phone_no = b.phone_no 
where b.prod_prc_name not like '%校园%'
;

drop table workspace.zb_luojiang_20200303_result;
create table workspace.zb_luojiang_20200303_result
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select chl_name,dm,count(phone_no) as num_sum
from 
(
    select distinct phone_no,chl_name,dm
    from workspace.zb_luojiang_20200303
) a
group by chl_name,dm
order by chl_name,dm
;

--检查
select * from datamart.data_dm_uv_info_m  where phone_no = '1878108TkML';

select distinct a.phone_no,a.chl_name,b.open_time,a.dm
from 
(
select * from workspace.zb_luojiang_20200303 where dm = '06' and chl_name like '德阳罗江分公司金蜂星通讯手机大卖场（合作大卖场）' 
) a
left join
datamart.data_dm_uv_info_m b
on a.phone_no = b.phone_no
;

--导出文件
insert overwrite local directory '/mnt/disk1/user/zhoubing/results/zb_luojiang_20200303_result' row format delimited fields 
terminated by ',' 
select * from workspace.zb_luojiang_20200303_result where 1=1 
;
--退出hive压缩文件
merge_reduce.sh /mnt/disk1/user/zhoubing/results/zb_luojiang_20200303_result;