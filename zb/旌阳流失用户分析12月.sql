--旌阳流失用户分析12月
drop table workspace.zb_jy_user;
create table workspace.zb_jy_user(phone_no string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
load data local inpath '/mnt/disk1/user/lj/temp_files/liushiUser12.txt' into table  workspace.zb_jy_user;

select * from workspace.zb_jy_user;

--号码 入网区县    入网渠道名称  11月常驻基站 11月常驻基站区县归属 10月常驻基站 10月常驻基站区县归属 9月常驻基站  9月常驻基站区县归属

select * from workspace.th_encrypt_zb_jy_user;

--dwd_obode_kb_201911  所有用户的常驻CI

select * from dwd_obode_kb_201911 limit 20;

--结果
drop table workspace.zb_jy_result;
create table workspace.zb_jy_result
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select distinct a.phone_no,c.county_name,b.chl_name,d.bs_name as 11yueBS,d.county as 11yueCounty
    ,e.bs_name as 10yueBS,e.county as 10yueCounty,f.bs_name as 09yueBS,f.county as 09yueCounty
from 
(
    select distinct a.phone_no,a.county_id,a.group_id
    from datamart.data_dm_uv_info_m a
    where dy = '2019' 
    and a.phone_no in (
                    select phone_no 
                    from workspace.th_encrypt_zb_jy_user
                    )
) a
left join 
(
    select * 
    from datamart.base_channel_info
    where dy = '2019' and dm = '11'
) b
on a.group_id = b.chl_id
left join 
(
    select distinct county_id,county_name
    from datamart.data_dwd_wlan_tv_jishi_ds
    where dy = '2019' and dm = '05'
) c on a.county_id = c.county_id
left join 
(
    select a.phone_no,b.bs_name,b.county
    from 
    (
        select *
        from workspace.dwd_obode_kb_201911 a
        where a.phone_no in (
                    select phone_no 
                    from workspace.th_encrypt_zb_jy_user
                    )
    ) a
    left join 
    (
        select * 
        from business.base_cell_info
        where day = '20191218'
    ) b
    on a.cell_id = b.cell_id
) d
on a.phone_no = d.phone_no
left join 
(
    select a.phone_no,b.bs_name,b.county
    from 
    (
        select *
        from workspace.dwd_obode_kb_201910 a
        where a.phone_no in (
                    select phone_no 
                    from workspace.th_encrypt_zb_jy_user
                    )
    ) a
    left join 
    (
        select * 
        from business.base_cell_info
        where day = '20191218'
    ) b
    on a.cell_id = b.cell_id
) e
on a.phone_no = e.phone_no
left join 
(
    select a.phone_no,b.bs_name,b.county
    from 
    (
        select *
        from workspace.dwd_obode_kb_201909 a
        where a.phone_no in (
                    select phone_no 
                    from workspace.th_encrypt_zb_jy_user
                    )
    ) a
    left join 
    (
        select * 
        from business.base_cell_info
        where day = '20191218'
    ) b
    on a.cell_id = b.cell_id
) f
on a.phone_no = f.phone_no
;

1726591FUWy
1726591FsWZ
1588383FwSt
1726591FnjA
1726591FUSg
1988150TIby
1726591FHct
1726591FoSP
1726591FIbA
1726591FUdZ
1726591FIXt
1726591FsdQ
1726591FUWt
1726591FHdg
1726591FIjZ
1726591Fwvt
1726591FIEt
1726591FnMP
1726591FwbA
1726591FnMg
1726591FIXy
1726591FwjL
1588366RwXu
1828057pIvQ
1726591FsWQ
1726591FUEg
1726591Fnbr
1726591FISA
1726591FNdg

select * from workspace.th_encrypt_zb_jy_user a
where a.phone_no not in (
select phone_no 
from workspace.zb_jy_result
 );

select count(*) from workspace.zb_jy_result limit 10;

select * 
from workspace.dwd_obode_kb_201911 
where phone_no = '1398103qUdy';
select *
from
(
    select a.phone_no,b.bs_name,b.county
    from 
    (
        select *
        from workspace.dwd_obode_kb_201911 a
        where a.phone_no in (
                    select phone_no 
                    from workspace.th_encrypt_zb_jy_user
                    )
    ) a
    left join business.base_cell_info b
    on a.cell_id = b.cell_id
) a where a.phone_no = '1518102zNXt';

select count(phone_no),count(distinct phone_no)
from 
(
    select distinct a.phone_no,a.county_id,a.group_id
    from datamart.data_dm_uv_info_m a
    where dy = '2019' 
    and a.phone_no in (
                    select phone_no 
                    from workspace.th_encrypt_zb_jy_user
                    )
) a;

select phone_no,count(phone_no)
from 
(
    select distinct a.phone_no,a.county_id,a.group_id
    from datamart.data_dm_uv_info_m a
    where dy = '2019' 
    and a.phone_no in (
                    select phone_no 
                    from workspace.th_encrypt_zb_jy_user
                    )
) a group by phone_no ;

select distinct a.phone_no,a.county_id,a.group_id
from datamart.data_dm_uv_info_m a
where phone_no = '1588384Fkby';