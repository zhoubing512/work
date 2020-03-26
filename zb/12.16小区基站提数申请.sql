--12.16小区基站提数申请

ci  归属小区    用户号码    用户主资费   是否有宽带   近3月月均消费 近3月月均流量 不限量资费   是否有合约   宽带号码    宽带资费    宽带成员数   集团名称    入网渠道
--cell_id
drop table workspace.zb_jizhan_20191216;
create table workspace.zb_jizhan_20191216(xiaoqu string,cell_id string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
load data local inpath '/mnt/disk1/user/lj/temp_files/zb_jizhan_1216.txt' into table  workspace.zb_jizhan_20191216;


--小区用户
drop table workspace.zb_xiaoquuser;
create table workspace.zb_xiaoquuser
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  select a.phone_no,a.ci
  from 
  (
    select a.phone_no,a.ci,row_number() over (partition by phone_no order by ci_num desc ) as num 
    from
    (
        select a.phone_no,a.ci,count(a.ci) as ci_num
        from 
        (
            select a.phone_no,a.time_type,a.ci
            from business.dwd_user_location_day a
            where a.dy = '2019' and a.dm = '12' --and a.time_type = 'sf'
            and a.ci in (
                        select cell_id
                        from workspace.zb_jizhan_20191216
                        )
        ) a group by a.phone_no,a.ci
        having ci_num > 4
    ) a
  ) a where num = 1
;

select a.ci,count(distinct a.phone_no)
from workspace.zb_xiaoquuser a
group by a.ci
;
select count(*) from workspace.zb_xiaoquuser;


data_user_unit  --集团名称

drop table workspace.zb_1216jizhan;
create table workspace.zb_1216jizhan
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select distinct b.xiaoqu,a.phone_no,c.prod_prc_name,c.broad_type,c.avg_3mon_arpu,c.avg_3mon_dou,e.prod_prc_name_gr,case when n.phone_no is not null then 1 else 0 end as shifouheyue
        ,g.kd_no1,g.prod_prc_name_kdzf1,g.kuandaichengyuannum,h.unit_name,c.chl_name
        ,z.mobileprovince,z.mobilecity
from workspace.zb_xiaoquuser a
left join workspace.zb_jizhan_20191216 b
on a.ci = b.cell_id
left join 
( 
    select a.*,d.prod_prc_name,m.chl_name
    from 
    (
        select * 
        from datamart.data_dm_uv_info_m 
        where dy = '2019' and dm = '11'
    ) a
    left join datamart.base_prc_info d
    on a.main_prod_prcid = d.prod_prcid
    left join 
    (
        select * 
        from datamart.base_channel_info
        where dy = '2019' and dm = '11'
    ) m
    on a.group_id = m.chl_id
) c
on a.phone_no = c.phone_no
left join datamart.dw_user_prc_bxl_latest e
on a.phone_no = e.phone_no
left join 
(
    select a.phone_no,a.key_phone_no, a.kd_no1,a.prod_prc_name_kdzf1,b.kuandaichengyuannum
    from datamart.dw_user_kd_latest a
    left join 
    (
        select key_phone_no,count(a.phone_no) as kuandaichengyuannum
        from datamart.dw_user_kd_latest  a
        where a.phone_no in (
                            select phone_no
                            from workspace.zb_xiaoquuser
                            )
        group by key_phone_no
    ) b on a.key_phone_no = b.key_phone_no
) g
on a.phone_no = g.phone_no
left join datamart.data_user_unit h
on a.phone_no = h.phone_no
left join 
(
    select * 
    from datamart.data_user_act a
    where innet_date > '2019-12-25' and data_eff_flag = 1
    and a.phone_no in (
                    select phone_no
                    from workspace.zb_xiaoquuser
                    )
) n
on a.phone_no = n.phone_no
left join
(
    --匹配运营商
    select * 
    from business.base_mobile_locale
) z
on substr(a.phone_no,1,7) = z.mobilenumber
;


select * from workspace.zb_1216jizhan  limit 50;
select count(*) from workspace.zb_1216jizhan where prod_prc_name is null and mobilecity <> '德阳';
select * 
from datamart.data_user_act
where innet_date < '2019-12-31'

select * from workspace.zb_1216jizhan  where xiaoqu = '美景嘉园';

select phone_no,count( phone_no) as num
from  workspace.zb_1216jizhan 
group by phone_no
having num > 1 limit 5;

select *
from
(
    select a.phone_no,d.prod_prc_name,m.chl_name
    from 
    (
        select * 
        from datamart.data_dm_uv_info_m 
        where dy = '2019' and dm = '11'
    ) a
    left join datamart.base_prc_info d
    on a.main_prod_prcid = d.prod_prcid
    left join 
    (
        select * 
        from datamart.base_channel_info
        where dy = '2019' and dm = '11'
    ) m
    on a.group_id = m.chl_id
) a where phone_no = '1589288qkjQ';

select *
from datamart.data_dm_uv_info_m
where phone_no = '1589288qkjQ';

select distinct a.phone_no,run_code,open_date,brand_name
from datamart.data_user_info a 
where a.phone_no in(
select phone_no from workspace.zb_1216jizhan where prod_prc_name is null and mobilecity = '德阳'
)
;