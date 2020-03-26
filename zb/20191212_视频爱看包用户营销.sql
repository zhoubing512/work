--视频爱看包用户营销
--ACAZ33547、ACAZ33548、ACAZ33549、ACAZ40371、ACAZ40372、ACAZ40373
--号码、区县、归属学校、集团280代码、开户渠道名称
drop table workspace.zb_quxian;
create table workspace.zb_quxian(college_name string,quxian string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
load data local inpath '/mnt/disk1/user/lj/temp_files/quxian.txt' into table  workspace.zb_quxian;

select * from workspace.zb_quxian;
--中间表

drop table workspace.shipin_zhongjian;
create table workspace.shipin_zhongjian
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select c.phone_no,round(sum(c.flow)/(1024*1024*10),2) as dayAvgShiPinFlow
from
(
    select * 
    from
    (
        select a.phone_no,a.app_id,a.flow
        from datamart.data_dw_xdr_gprs a
        where a.dy='2019' 
        and a.dm='12'
        and a.phone_no in
        (
            --手机号，并集
            select distinct phone_no
            from
            (
                select a.phone_no 
                from datamart.data_user_prc a
                where  a.prod_prcid  in ('ACAZ33547','ACAZ33548','ACAZ33549','ACAZ40371','ACAZ40372','ACAZ40373')
                and phone_no is not null
                union all
                select b.phone_no
                from th_gaoxiao_alluser_detail_2016_to_thisyear_count b
            ) a
        )
    ) a
    where a.app_id in 
        (
            select app_id 
            from datamart.data_dw_xdr_gprs_app
            where app_type='视频' and (app_name like '%爱奇艺%' or app_name like '%腾讯视频%' or 
                app_name like '%优酷%' or app_name like '%芒果TV%' or app_name like '%哔哩哔哩%' or 
                app_name like '%搜狐%' or app_name like '%PPTV%' or app_name like '%乐视%' )
        )
) c 
group by c.phone_no
;

--手机号，并集
select distinct phone_no
from
(
    select a.phone_no 
    from datamart.data_user_prc a
    where  a.prod_prcid  in ('ACAZ33547','ACAZ33548','ACAZ33549','ACAZ40371','ACAZ40372','ACAZ40373')
    and phone_no is not null
    union all
    select b.phone_no
    from th_gaoxiao_alluser_detail_2016_to_thisyear_count b
) a
;


drop table workspace.shipin_20191213;
create table workspace.shipin_20191213
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select distinct a.phone_no
            ,case when d.quxian is not null then d.quxian 
            else (case when e.county_name is not null then e.county_name else f.dongtai_district  end ) end as quxian
            ,d.college_name,c.unit_id,a.chl_name,a.dayAvgShiPinFlow
from 
(
    select a.phone_no,b.chl_name,c.dayAvgShiPinFlow
    from
    (
        select distinct phone_no
        from
        (
            select a.phone_no 
            from datamart.data_user_prc a
            where  a.prod_prcid  in ('ACAZ33547','ACAZ33548','ACAZ33549','ACAZ40371','ACAZ40372','ACAZ40373')
            and phone_no is not null
            union all
            select b.phone_no
            from th_gaoxiao_alluser_detail_2016_to_thisyear_count b
        ) a
    ) a
    left join 
    (
        select * 
        from datamart.data_user_channel
        where dy='2019' and dm='11' and phone_no is not null
    ) b
    on a.phone_no=b.phone_no
    left join workspace.shipin_zhongjian  c 
    on a.phone_no=c.phone_no
) a
left join
(
    select phone_no,unit_id
    from datamart.data_dw_group_meb
    where dy='2019' and dm='11'
) c on a.phone_no=c.phone_no
left join
(
    select a.phone_no,a.college_id,a.college_name,b.quxian
    from workspace.th_gaoxiao_alluser_detail_2016_to_thisyear_count a
    left join workspace.zb_quxian b
    on a.college_name=b.college_name
) d on a.phone_no=d.phone_no
left join
(
    select * 
    from workspace.th_deyang_alluser_cunzu_fenju_detail_night_2019
) e on a.phone_no=e.phone_no
left join
(
    select *
    from datamart.data_dw_user_base_info 
    where dy='2019' 
    and dm='11'
) f on a.phone_no = f.phone_no
;


--检验重复
select count(phone_no),count(distinct phone_no) from workspace.shipin_20191213;

select phone_no,count(phone_no) from workspace.shipin_20191212 group by phone_no having count(phone_no) >1;

select * from workspace.shipin_20191212
where phone_no = '1388101Rndr';

select count(*) from workspace.shipin_20191213 where dayAvgShiPinFlow >0;
select * from workspace.shipin_20191213 where college_name is null and quxian <>'';
select * from workspace.shipin_20191213 where college_name is  null and quxian = '';
select count(*) from workspace.shipin_20191213 where chl_name is not null;

select count(phone_no) 
from workspace.shipin_20191213 d
left join
(
    --匹配运营商
    select * 
    from business.base_mobile_locale
) m 
on substr(d.phone_no,1,7) = m.mobilenumber
where m.mobilecity <>'德阳'
;


select * 
from datamart.data_user_channel
where phone_no in('1588320FJcP','1878198qUXy','1828285YsdZ','1878481TUbt') limit 20;


select phone_no,college_name,unit_id,chl_name,dayAvgShiPinFlow from workspace.shipin_20191212 
where college_name = '德阳安装技师学院（西南工程校）' ;


select count(*) from workspace.th_gaoxiao_alluser_detail_2016_to_thisyear_count;

select * from workspace.shipin_20191212 where phone_no = '1828383aovP' and county_name = '';
select * from data_dw_xdr_gprs_app where app_type='视频' and app_name like '%哔哩哔哩%' limit 100;
--集团280编码
data_user_unit
--筛选视频类app
data_dw_xdr_gprs
data_dw_xdr_gprs_app
select * from data_dw_xdr_gprs_app where app_type='视频' limit 20;

workspace.th_deyang_alluser_cunzu_fenju_detail_night_2019

--理论上3年的未毕业学生(含实习生)若不含实习生则加上（last_year=当年）的限制
select college_id,college_name,count(distinct phone_no)
from th_gaoxiao_alluser_detail_2016_to_thisyear_count 
where student_type='在读'
and id_no <>'0'
--and last_year='2019'
group by college_id,college_name
order by int(college_id);