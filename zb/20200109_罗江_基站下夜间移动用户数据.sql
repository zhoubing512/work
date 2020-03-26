--基站下夜间移动用户数据

--导出cell_id

drop table workspace.zb_luojiang_20200109;
create table workspace.zb_luojiang_20200109(xiaoqu string,cell_id string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
load data local inpath '/mnt/disk1/user/lj/temp_files/luojiang_jizhan.txt' into table  workspace.zb_luojiang_20200109;


drop table workspace.zb_luojiang_20200109_result;
create table workspace.zb_luojiang_20200109_result
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  select *
  from 
  (
    select a.xiaoqu,a.phone_no
        ,case when c.broad_prod_id is null or c.broad_prod_id = '' or c.broad_prod_id =0 then 0 else 1 end as shifou_kuandai
        ,c.broad_prod_name,c.broad_prod_fee,c.main_prc_fee,c.arpu,c.avg_3mon_arpu
    from 
    (
        select a.*,b.xiaoqu
        from 
        (
            select a.phone_no,a.cell_id
            from workspace.dwd_obode_kb_201912 a
            where a.cell_id in
            (
                select cell_id 
                from workspace.zb_luojiang_20200109
            )
        ) a
        left join workspace.zb_luojiang_20200109 b
        on a.cell_id = b.cell_id
    ) a
    left join 
    (
        select *
        from datamart.data_dm_uv_info_m
        where dy = '2019' and dm = '12'
    ) c
    on a.phone_no = c.phone_no
) a
where a.avg_3mon_arpu is not null
;
