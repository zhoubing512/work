##中江城区小区用户信息
data_dm_uv_info_m
dwd_user_location_month
dw_user_prc_bxl_latest
base_cell_info

create table workspace.zb_zj_20191126(ci string)
  row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;

  load data local inpath '/mnt/disk1/user/lj/temp_files/ci.txt' into table workspace.zb_zj_20191126;

------生成最新小区信息表
drop table workspace.zb_zj_20191126_bcell;
create table workspace.zb_zj_20191126_bcell 
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as 
  select ci,cell_name from 
  (select ci,cell_name,day,row_number() over ( partition by ci order by day desc ) num from
    (select a.ci,b.cell_name,b.day from workspace.zb_zj_20191126 a
     left join business.base_cell_info b on a.ci = b.cell_id) c
    ) last where last.num=1
  ;
------筛选常驻人口号码
drop table workspace.zb_zj_20191126_cz;
create table workspace.zb_zj_20191126_cz 
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as 
  select *from (
    select dd.phone_no,dd.ci,dd.ci_num,row_number() over (partition by phone_no order by ci_num desc ) num  from (
      select bb.phone_no,bb.ci,count(ci) ci_num from (
        select * from (
          select phone_no,ci,time_type,duration,dd,row_number() over (partition by phone_no,dd order by duration desc ) num  
          from business.dwd_user_location_day  where dy='2019' and dm='11' and time_type='sf'
          ) cc
        where cc.num = 1
        ) bb
      group by phone_no,ci
      ) dd
    ) as ff
  where ff.num=1 and ff.phone_no not like '10%' and ff.phone_no like '1%' 
  ;

------匹配是否宽带、不限量、宽带成员数、月均消费
drop table workspace.zb_zj_20191126_result_test;
create table workspace.zb_zj_20191126_result_test 
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as 
  select i.ci,i.cell_name,i.phone_no,i.sfkd,
  case when j.phone_no is not null then '1' else '0' end as sfbxl,
  cy_num,k.avg_3mon_arpu 
  from 
  (
    select  ci,cell_name,phone_no,sfkd,cy_num
    from 
    (
      select f.ci,f.cell_name,f.phone_no,g.key_phone_no,
      case when g.phone_no is not null then '1' else '0' end as sfkd
      from 
      (
        select  a.ci,a.cell_name,b.phone_no 
        from workspace.zb_zj_20191126_bcell a
        left join workspace.zb_zj_20191126_cz b 
        on a.ci = b.ci
        ) f
      left join datamart.dw_user_kd_latest g 
      on f.phone_no=g.phone_no
      ) as h
    left join (    select gg.key_phone_no,count(distinct gg.phone_no) cy_num
      from 
      (
        select  a.ci,a.cell_name,b.phone_no 
        from workspace.zb_zj_20191126_bcell a
        left join workspace.zb_zj_20191126_cz b 
        on a.ci = b.ci
        ) f
      left join datamart.dw_user_kd_latest gg 
      on f.phone_no=gg.key_phone_no
      group by gg.key_phone_no) aa
    on h.key_phone_no=aa.key_phone_no
    ) i
  left join datamart.dw_user_prc_bxl_latest j 
  on i.phone_no = j.phone_no
  left join datamart.data_dm_uv_info_m k 
  on i.phone_no = k.phone_no 
  where k.dy='2019' and k.dm='10'
  ;


select * from  workspace.zb_zj_20191126_result where sfkd ='0';





