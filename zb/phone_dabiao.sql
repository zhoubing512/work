-----携入号码打标
create table workspace.zb_xr_20191129(phone_no string)
    row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;

load data local inpath '/mnt/disk1/user/lj/temp_files/xr_20191129.txt' into table workspace.zb_xr_20191129;
select count(*) from workspace.zb_xr_20191128;


drop table workspace.zb_xr_20191129_78910;
create table workspace.zb_xr_20191129_78910
    row format delimited fields terminated by '\001' 
    stored as orc tblproperties ('orc.compress'='ZLIB') 
    as 
    select a.phone_no
    ,case when b.opposite_no is not null then '1' 
    else '0' end  as xr_7yue
    ,case when c.opposite_no is not null then '1' 
    else '0' end  as xr_8yue
    ,case when d.opposite_no is not null then '1' 
    else '0' end  as xr_9yue
    ,case when e.opposite_no is not null then '1' 
    else '0' end  as xr_10yue
    from workspace.th_encrypt_zb_xr_20191129 a
    left join 
    (select distinct b1.opposite_no from workspace.th_encrypt_zb_xr_20191129 a1
        left join (
            select phone_no,opposite_no 
            from datamart.data_dwb_cal_user_voc_yx_ds 
            where dy='2019' and dm='07') b1 on a1.phone_no=b1.opposite_no
        ) b on a.phone_no=b.opposite_no
    left join 
    (select distinct c1.opposite_no from workspace.th_encrypt_zb_xr_20191129 a2
        left join (
            select phone_no,opposite_no 
            from datamart.data_dwb_cal_user_voc_yx_ds 
            where dy='2019' and dm='08') c1 on a2.phone_no=c1.opposite_no
        ) c on a.phone_no=c.opposite_no
    left join 
    (select distinct d1.opposite_no from workspace.th_encrypt_zb_xr_20191129 a3
        left join (
            select phone_no,opposite_no 
            from datamart.data_dwb_cal_user_voc_yx_ds 
            where dy='2019' and dm='09') d1 on a3.phone_no=d1.opposite_no
        ) d on a.phone_no=d.opposite_no
    left join 
    (select distinct e1.opposite_no from workspace.th_encrypt_zb_xr_20191129 a4
        left join (
            select phone_no,opposite_no 
            from datamart.data_dwb_cal_user_voc_yx_ds 
            where dy='2019' and dm='10') e1 on a4.phone_no=e1.opposite_no
        ) e on a.phone_no=e.opposite_no
    ;




------所有异网号码6,7,8,9,10月过网情况
drop table workspace.dy_xr_dabiao_678910_tmp1;
create table workspace.dy_xr_dabiao_678910_tmp1
    row format delimited fields terminated by '\001' 
    stored as orc tblproperties ('orc.compress'='ZLIB') 
    as
select  distinct opposite_no
        from
        (
            select distinct opposite_no
            from datamart.data_dwb_cal_user_voc_yx_ds
            where dy='2019' and dm in ('06','07','08','09','10') 
            and opposite_no not like '10%' and opposite_no like '1%' and  length(opposite_no) =11
        ) m
        left join (
            select mobilenumber,mobiletype
            from business.base_mobile_locale
            ) n 
        on substr(m.opposite_no,1,7) = n.mobilenumber 
        where n.mobiletype <> '中国移动'
        ;

drop table workspace.dy_xr_dabiao_6_tmp1;
create table workspace.dy_xr_dabiao_6_tmp1
    row format delimited fields terminated by '\001' 
    stored as orc tblproperties ('orc.compress'='ZLIB') 
    as
select distinct opposite_no 
            from datamart.data_dwb_cal_user_voc_yx_ds 
            where dy='2019' and dm='06'
            and opposite_no not like '10%' and opposite_no like '1%' and  length(opposite_no) =11
            ;

drop table workspace.dy_xr_dabiao_7_tmp1;
create table workspace.dy_xr_dabiao_7_tmp1
    row format delimited fields terminated by '\001' 
    stored as orc tblproperties ('orc.compress'='ZLIB') 
    as
select distinct opposite_no 
            from datamart.data_dwb_cal_user_voc_yx_ds 
            where dy='2019' and dm='07'
            and opposite_no not like '10%' and opposite_no like '1%' and  length(opposite_no) =11
            ;

drop table workspace.dy_xr_dabiao_8_tmp1;
create table workspace.dy_xr_dabiao_8_tmp1
    row format delimited fields terminated by '\001' 
    stored as orc tblproperties ('orc.compress'='ZLIB') 
    as
select distinct opposite_no 
            from datamart.data_dwb_cal_user_voc_yx_ds 
            where dy='2019' and dm='08'
            and opposite_no not like '10%' and opposite_no like '1%' and  length(opposite_no) =11
            ;

drop table workspace.dy_xr_dabiao_9_tmp1;
create table workspace.dy_xr_dabiao_9_tmp1
    row format delimited fields terminated by '\001' 
    stored as orc tblproperties ('orc.compress'='ZLIB') 
    as
select distinct opposite_no 
            from datamart.data_dwb_cal_user_voc_yx_ds 
            where dy='2019' and dm='09'
            and opposite_no not like '10%' and opposite_no like '1%' and  length(opposite_no) =11
            ;

drop table workspace.dy_xr_dabiao_10_tmp1;
create table workspace.dy_xr_dabiao_10_tmp1
    row format delimited fields terminated by '\001' 
    stored as orc tblproperties ('orc.compress'='ZLIB') 
    as
select distinct opposite_no 
            from datamart.data_dwb_cal_user_voc_yx_ds 
            where dy='2019' and dm='10'
            and opposite_no not like '10%' and opposite_no like '1%' and  length(opposite_no) =11
            ;


drop table workspace.dy_xr_dabiao_678910;
create table workspace.dy_xr_dabiao_678910
    row format delimited fields terminated by '\001' 
    stored as orc tblproperties ('orc.compress'='ZLIB') 
    as 
    select distinct a.opposite_no as phone_no
    ,case when b.opposite_no is not null then '1' 
    else '0' end  as xr_6yue
    ,case when c.opposite_no is not null then '1' 
    else '0' end  as xr_7yue
    ,case when d.opposite_no is not null then '1' 
    else '0' end  as xr_8yue
    ,case when e.opposite_no is not null then '1' 
    else '0' end  as xr_9yue
    ,case when f.opposite_no is not null then '1' 
    else '0' end  as xr_10yue
    from dy_xr_dabiao_678910_tmp1 a
    left join dy_xr_dabiao_6_tmp1 b on a.opposite_no=b.opposite_no
    left join dy_xr_dabiao_7_tmp1 c on a.opposite_no=c.opposite_no
    left join dy_xr_dabiao_8_tmp1 d on a.opposite_no=d.opposite_no
    left join dy_xr_dabiao_9_tmp1 e on a.opposite_no=e.opposite_no
    left join dy_xr_dabiao_10_tmp1 f on a.opposite_no=f.opposite_no
    ;

------查看数量
select count(*) from  workspace.th_encrypt_zb_xr_20191129 a 
left join workspace.dy_xr_dabiao_678910 b on a.phone_no = b.phone_no;

select count(*) from workspace.zb_xr_20191129_78910;

    select a.phone_no,a.xr_7yue,a.xr_8yue,a.xr_9yue,a.xr_10yue 
    from workspace.zb_xr_20191126_78910_test a,workspace.zb_xr_20191126_78910 b
    where a.phone_no=b.phone_no and a.xr_7yue=b.xr_7yue and a.xr_8yue=b.xr_8yue and a.xr_9yue=b.xr_9yue and a.xr_10yue=b.xr_10yue


##携入号码 20191126
create table workspace.zb_xr_20191126(phone_no string)
    row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;

    load data local inpath '/mnt/disk1/user/lj/temp_files/xr_20191126.txt' into table workspace.zb_xr_20191126;

    drop table workspace.zb_xr_20191126_7;
    create table workspace.zb_xr_20191126_7 
        row format delimited fields terminated by '\001' 
        stored as orc tblproperties ('orc.compress'='ZLIB') 
        as 
        select opposite_no aa,calltype_id bb,count(distinct phone_no) cc,sum(call_duration_m) dd,count(phone_no) ee,sum (call_duration) ff from (
            select a.phone_no,b.opposite_no,call_duration_m,calltype_id,call_times,call_duration 
            from workspace.th_encrypt_zb_xr_20191126  as a
            left join 
            (select phone_no,opposite_no,call_duration_m,calltype_id,call_times,call_duration 
                from datamart.data_dwb_cal_user_voc_yx_ds 
                where dy='2019' and dm='07' ) as b on a.phone_no=b.opposite_no
        ) as d
        group by  opposite_no,calltype_id
        ;

        drop table workspace.zb_xr_20191126_8;
        create table workspace.zb_xr_20191126_8 
            row format delimited fields terminated by '\001' 
            stored as orc tblproperties ('orc.compress'='ZLIB') 
            as 
            select opposite_no aa,calltype_id bb,count(distinct phone_no) cc,sum(call_duration_m) dd,count(phone_no) ee,sum (call_duration) ff from (
                select a.phone_no,b.opposite_no,call_duration_m,calltype_id,call_times,call_duration 
                from workspace.th_encrypt_zb_xr_20191126  as a
                left join 
                (select phone_no,opposite_no,call_duration_m,calltype_id,call_times,call_duration 
                    from datamart.data_dwb_cal_user_voc_yx_ds 
                    where dy='2019' and dm='08' ) as b on a.phone_no=b.opposite_no
            ) as d
            group by  opposite_no,calltype_id
            ;

            drop table workspace.zb_xr_20191126_9;
            create table workspace.zb_xr_20191126_9 
                row format delimited fields terminated by '\001' 
                stored as orc tblproperties ('orc.compress'='ZLIB') 
                as 
                select opposite_no aa,calltype_id bb,count(distinct phone_no) cc,sum(call_duration_m) dd,count(phone_no) ee,sum (call_duration) ff from (
                    select a.phone_no,b.opposite_no,call_duration_m,calltype_id,call_times,call_duration 
                    from workspace.th_encrypt_zb_xr_20191126  as a
                    left join 
                    (select phone_no,opposite_no,call_duration_m,calltype_id,call_times,call_duration 
                        from datamart.data_dwb_cal_user_voc_yx_ds 
                        where dy='2019' and dm='09' ) as b on a.phone_no=b.opposite_no
                ) as d
                group by  opposite_no,calltype_id
                ;

                drop table workspace.zb_xr_20191126_10;
                create table workspace.zb_xr_20191126_10 
                    row format delimited fields terminated by '\001' 
                    stored as orc tblproperties ('orc.compress'='ZLIB') 
                    as 
                    select opposite_no aa,calltype_id bb,count(distinct phone_no) cc,sum(call_duration_m) dd,count(phone_no) ee,sum (call_duration) ff from (
                        select a.phone_no,b.opposite_no,call_duration_m,calltype_id,call_times,call_duration 
                        from workspace.th_encrypt_zb_xr_20191126  as a
                        left join 
                        (select phone_no,opposite_no,call_duration_m,calltype_id,call_times,call_duration 
                            from datamart.data_dwb_cal_user_voc_yx_ds 
                            where dy='2019' and dm='10' ) as b on a.phone_no=b.opposite_no
                    ) as d
                    group by  opposite_no,calltype_id
                    ;

                    drop table workspace.zb_xr_20191126_78910;
                    create table workspace.zb_xr_20191126_78910 
                        row format delimited fields terminated by '\001' 
                        stored as orc tblproperties ('orc.compress'='ZLIB') 
                        as 
                        select distinct a.phone_no
                        ,case when e.aa is not null then '1' else '0' end as xr_7yue
                        ,case when b.aa is not null then '1' else '0' end as xr_8yue
                        ,case when c.aa is not null then '1' else '0' end as xr_9yue
                        ,case when d.aa is not null then '1' else '0' end as xr_10yue
                        from workspace.th_encrypt_zb_xr_20191126 a
                        left join zb_xr_20191126_7 e on a.phone_no = e.aa
                        left join zb_xr_20191126_8 b on a.phone_no = b.aa
                        left join zb_xr_20191126_9 c on a.phone_no = c.aa
                        left join zb_xr_20191126_10 d on a.phone_no = d.aa
                        ;