--绵竹 基站常驻用户
--基站信息
drop table workspace.zb_mianzhu_bsinfo_20200327;
create table workspace.zb_mianzhu_bsinfo_20200327(bs_name string,cell_id string,4g_or_2g string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
putdata -f bs_info_mianzhu.txt -t workspace.zb_mianzhu_bsinfo_20200327;


--用5分钟表跑数，基站用户
--dwd_user_location_5minute
drop table workspace.zb_bs_user_mianzhu_20200327_minute;
create table workspace.zb_bs_user_mianzhu_20200327_minute
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select distinct a.phone_no,b.cell_id,b.bs_name,b.4g_or_2g
from 
(
    select phone_no,cell_id
    from 
    (
        select phone_no,count(dd) as day_sum,cell_id
        from 
        (
            select distinct phone_no,cell_id,dd
            from 
            (
                select phone_no,count(date_time) as date_time_sum,cell_id,dd
                from 
                (
                    select *
                    from business.dwd_user_location_5minute
                    where dy = '2020' and dm = '03'
                ) a
                group by phone_no,cell_id,dd
                having count(date_time) >= 48
            ) a
        )  a
        group by phone_no,cell_id
    ) a 
    where a.day_sum > 2
) a
right join
(
    select  a.bs_name,a.cell_id,a.4g_or_2g
    from workspace.zb_mianzhu_bsinfo_20200327 a
    
) b
on a.cell_id = b.cell_id
;

--检查无用户的基站
select bs_name,count(distinct phone_no)
from workspace.zb_bs_user_mianzhu_20200327_minute
group by bs_name
having count(distinct phone_no) = 0
;

--匹配结果
drop table workspace.zb_bs_user_mianzhu_20200327_result;
create table workspace.zb_bs_user_mianzhu_20200327_result
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  select distinct a.phone_no,b.run_name,c.prod_prc_name_main,c.prod_prc_price_main
  from workspace.zb_bs_user_mianzhu_20200327_minute a
  left join (
      select *
      from datamart.data_user_info
      where dy = '2020' and dm = '03'
  ) b 
  on a.phone_no = b.phone_no
  left join(
      select * from 
      datamart.dw_user_prc_main_latest
  ) c
  on a.phone_no = c.phone_no
  ;
--非德阳用户数多少
select count(phone_no)
from workspace.zb_bs_user_mianzhu_20200327_result a
left join business.base_mobile_locale b
on substr(a.phone_no,0,7) = b.mobilenumber
where b.mobilecity not like '%德阳%'
;
