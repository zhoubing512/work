--广汉2772站点用户
drop table workspace.zb_guanghan_jizhan_20200321;
create table workspace.zb_guanghan_jizhan_20200321(bs_id string,order_num string,cell_id string,bs_name string,cell_name string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
putdata -f bs_info.txt -t workspace.zb_guanghan_jizhan_20200321;

--资费、消费、宽带，成员数

workspace.zb_guanghan_jizhan_20200321

drop table workspace.zb_jizhan_user_guanghan_20200321;
create table workspace.zb_jizhan_user_guanghan_20200321
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select distinct a.phone_no,a.ci,b.jizhan_name
from 
(
    select distinct phone_no,ci
    from 
    (
        select phone_no,time_type,ci,row_number() over(partition by phone_no,time_type order by day_sum desc) as rn
        from 
        (
            select phone_no,count(dd) as day_sum,ci,time_type
            from 
            (
                select *
                from business.dwd_user_location_day
                where dy = '2020' and dm = '03' and (time_type = 'sf' or time_type = 'nsf')
            ) a
            group by phone_no,ci,dd,time_type
            having count(dd) > 2
        ) a
    ) a
    where rn = 1
) a
inner join
(
    select distinct a.bs_id,b.bs_name,b.cell_id,b.cell_name
    from workspace.zb_guanghan_jizhan_20200321 a
    left join business.base_cell_info b
    on a.bs_id = b.bs_id
    
) b
on a.ci = b.cell_id
;

--用5分钟表跑数
dwd_user_location_5minute
drop table workspace.zb_jizhan_user_guanghan_20200321_minute;
create table workspace.zb_jizhan_user_guanghan_20200321_minute
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select distinct a.phone_no,a.cell_id,b.bs_id,b.cell_name,b.bs_name
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
    select  a.bs_id,a.bs_name,a.cell_id,a.cell_name
    from workspace.zb_guanghan_jizhan_20200321 a
    
) b
on a.cell_id = b.cell_id
;

--检查重复

select phone_no,count(phone_no)
from workspace.zb_jizhan_user_guanghan_20200321_minute
group by phone_no
having count(phone_no) > 1
;
+--------------+------+
|   phone_no   | _c1  |
+--------------+------+
| 1351827lHMQ  | 2    |
| 1354707FUdr  | 2    |
| 1356822lnMu  | 2    |
| 1360810TwSA  | 2    |
| 1366830psdg  | 2    |
| 1369619RHvt  | 2    |
| 1379590qNjP  | 2    |
| 1389023zIMA  | 3    |
| 1398010zHMu  | 2    |
| 1398101qJEL  | 2    |
| 1588340GnWL  | 2    |
| 1828056RkbA  | 2    |
| 1828381Rsjy  | 2    |
| 1828381pIEt  | 2    |
| 1345896zHWQ  | 2    |
| 1351827TIbA  | 2    |
| 1362807zwbt  | 2    |
| 1368963Tnvy  | 2    |
| 1389023RocQ  | 2    |
| 1399021RUXQ  | 2    |
| 1399023qUMA  | 2    |
| 1399025TkSu  | 2    |
| 1399025YkdP  | 2    |
| 1399028anvA  | 2    |
| 1399028zkct  | 2    |
| 1518369Fojg  | 2    |
| 1528381zocy  | 2    |
| 1598491zNdZ  | 2    |
| 1820280TIXL  | 2    |
| 1828106YkSL  | 2    |
| 1828386GoSZ  | 2    |
| 1830840zoMy  | 2    |
| 1872801pIjZ  | 2    |
+--------------+------+
select *
from workspace.zb_jizhan_user_guanghan_20200321_minute
where phone_no = '1828386GoSZ'
;
--统计每个基站用户数
select *
from 
(
    select bs_id,bs_name,count(distinct phone_no) num
    from workspace.zb_jizhan_user_guanghan_20200321_minute
    group by bs_id,bs_name
) a
order by num
;
+----------+----------------------------+--------+
| a.bs_id  |         a.bs_name          | a.num  |
+----------+----------------------------+--------+
| 23614    | 德阳广汉小汉南湖馨苑28栋应急站FD18-ZFH   | 0      |
| 540262   | 德阳广汉瞿上园-ZLH                | 0      |
| 795830   | 德阳广汉广信鹭岛应急站-ZLW            | 0      |
| 795835   | 德阳广汉瞿上园别墅区A栋应急站-ZLH        | 0      |
| 795918   | 德阳广汉新鸥鹏教育小镇7号楼应急站FD18-ZFH  | 0      |
| 795831   | 德阳广汉南兴中学应急站FD18-ZFH        | 0      |
| 795917   | 德阳广汉新鸥鹏教育小镇巴川塔应急站FD18-ZFH  | 0      |
| 23613    | 德阳广汉小汉南湖馨苑21栋应急站FD18-ZFH   | 0      |
| 392709   | 德阳广汉雒城印象-ZLW               | 0      |
| 795844   | 德阳广汉益好佳家属区应急站FD18-ZFH      | 0      |
| 795827   | 德阳广汉翡翠郡15栋屋里应急站FD18-ZFH    | 0      |
| 795828   | 德阳广汉翡翠郡增补应急站-ZLW           | 0      |
| 953979   | 德阳广汉帝景国际-ZLW               | 2      |
| 795842   | 德阳广汉万兴一品增补应急站-ZLW          | 9      |
| 795826   | 德阳广汉东嘉苑增补应急站-ZLH           | 9      |
| 23604    | 德阳广汉狮象村应急站FD18-ZFH         | 22     |
| 795832   | 德阳广汉美景嘉园应急站-ZLW            | 64     |
| 23605    | 德阳广汉云盘村委会应急站FD18-ZFH       | 181    |
| 795846   | 德阳广汉紫荆苑增补应急站-ZLW           | 241    |
| 23435    | 德阳广汉南湖馨苑17栋应急-ZLH          | 379    |
| 795843   | 德阳广汉威肯郡应急站-ZLW             | 413    |
| 795841   | 德阳广汉顺德安居小区应急站-ZLW          | 599    |
+----------+----------------------------+--------+


--查看匹配基站
select distinct a.bs_id,a.bs_name,a.cell_id,a.cell_name
from workspace.zb_guanghan_jizhan_20200321 a
left join business.base_cell_info b
on a.bs_id = b.bs_id
order by bs_id,cell_id
;


select cell_id,bs_name from business.base_cell_info where bs_name like '%德阳广汉小汉镇南湖馨苑%';



消费在98以上裸奔超套用户、88以上套餐成员小于3人的存量宽带静态数据

资费、消费、宽带，成员数
--匹配结果
drop table workspace.zb_jizhan_user_guanghan_20200321_result;
create table workspace.zb_jizhan_user_guanghan_20200321_result
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select distinct *
from 
(
select a.bs_name,a.phone_no,b.prod_prc_name,b.arpu,b.avg_3mon_arpu,'0' as shifoukuandai, 0 as chengyuan_num
from workspace.zb_jizhan_user_guanghan_20200321_minute a
inner join 
(
    select a.phone_no,b.prod_prc_name,a.arpu,a.avg_3mon_arpu
    from 
    (
        select phone_no,main_prod_prcid,arpu,avg_3mon_arpu
        from datamart.data_dm_uv_info_m
        where dy = '2020' and dm = '02' and arpu >= 98 and broad_type = 0
    ) a 
    left join datamart.base_prc_info b
    on a.main_prod_prcid = b.prod_prcid
) b
on a.phone_no = b.phone_no
union all
select a.bs_name,a.phone_no,b.prod_prc_name,b.arpu,b.avg_3mon_arpu,b.shifoukuandai,b.chengyuan_num
from workspace.zb_jizhan_user_guanghan_20200321_minute a
inner join 
(
    select a.phone_no,a.prod_prc_name,a.arpu,a.avg_3mon_arpu,'1' as shifoukuandai,chengyuan_num
    from 
    (
        select a.phone_no,b.prod_prc_name,a.arpu,a.avg_3mon_arpu
        from 
        (
            select phone_no,main_prod_prcid,broad_prod_fee,arpu,avg_3mon_arpu
            from datamart.data_dm_uv_info_m
            where dy = '2020' and dm = '02' and avg_3mon_arpu >= 88
        ) a 
        left join datamart.base_prc_info b
        on a.main_prod_prcid = b.prod_prcid
    ) a
    inner join
    (
        select *
        from 
        (
        select phone_no,kd_no1,count(phone_no) over(partition by kd_no1) as chengyuan_num
        from datamart.dw_user_kd 
        where dy = '2020' and dm = '02'
        ) a
        where chengyuan_num < 3
    ) b
    on a.phone_no = b.phone_no
) b
on a.phone_no = b.phone_no
) a
;

--统计每个基站人数
select bs_name,count(phone_no)
from workspace.zb_jizhan_user_guanghan_20200321_result
group by bs_name
;

+-----------------------+------+
|        bs_name        | _c1  |
+-----------------------+------+
| 德阳广汉云盘村委会应急站FD18-ZFH  | 30   |
| 德阳广汉帝景国际-ZLW          | 1    |
| 德阳广汉紫荆苑增补应急站-ZLW      | 61   |
| 德阳广汉美景嘉园应急站-ZLW       | 10   |
| 德阳广汉顺德安居小区应急站-ZLW     | 90   |
| 德阳广汉万兴一品增补应急站-ZLW     | 4    |
| 德阳广汉南湖馨苑17栋应急-ZLH     | 37   |
| 德阳广汉威肯郡应急站-ZLW        | 55   |
| 德阳广汉狮象村应急站FD18-ZFH    | 1    |
+-----------------------+------+

--消费98以上用户
drop table workspace.zb_jizhan_user_guanghan_20200321_result_98;
create table workspace.zb_jizhan_user_guanghan_20200321_result_98
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select a.bs_name,a.phone_no,b.prod_prc_name,b.arpu,b.avg_3mon_arpu,b.broad_prod_name,b.broad_prod_fee,b.chengyuan_num
from workspace.zb_jizhan_user_guanghan_20200321_minute a
inner join 
(
    select a.phone_no,b.prod_prc_name,a.arpu,a.avg_3mon_arpu,a.broad_prod_name,a.broad_prod_fee,c.chengyuan_num
    from 
    (
        select phone_no,main_prod_prcid,arpu,avg_3mon_arpu,broad_prod_name,broad_prod_fee
        from datamart.data_dm_uv_info_m
        where dy = '2020' and dm = '02' and arpu >= 98
    ) a 
    left join datamart.base_prc_info b
    on a.main_prod_prcid = b.prod_prcid
    left join
    (
        select phone_no,kd_no1,count(phone_no) over(partition by kd_no1) as chengyuan_num
        from datamart.dw_user_kd 
        where dy = '2020' and dm = '02'
    ) c
    on a.phone_no = c.phone_no
) b
on a.phone_no = b.phone_no
;

select count(distinct phone_no) num
from workspace.zb_jizhan_user_guanghan_20200321_result_98
;
--统计每个基站人数
select *
from 
(
select bs_name,count(distinct phone_no) num
from workspace.zb_jizhan_user_guanghan_20200321_result_98
group by bs_name
) a
order by num
;
+-----------------------+--------+
|       a.bs_name       | a.num  |
+-----------------------+--------+
| 德阳广汉帝景国际-ZLW          | 1      |
| 德阳广汉东嘉苑增补应急站-ZLH      | 3      |
| 德阳广汉万兴一品增补应急站-ZLW     | 4      |
| 德阳广汉狮象村应急站FD18-ZFH    | 5      |
| 德阳广汉美景嘉园应急站-ZLW       | 20     |
| 德阳广汉云盘村委会应急站FD18-ZFH  | 76     |
| 德阳广汉南湖馨苑17栋应急-ZLH     | 103    |
| 德阳广汉紫荆苑增补应急站-ZLW      | 109    |
| 德阳广汉威肯郡应急站-ZLW        | 128    |
| 德阳广汉顺德安居小区应急站-ZLW     | 209    |
+-----------------------+--------+

select *
from workspace.zb_jizhan_user_guanghan_20200321_result_98
where bs_name = '德阳广汉威肯郡应急站-ZLW'
limit 10
;
--有成员数，但无宽带资费
+--------------+------------------+-----------------+----------------+
|   phone_no   | broad_prod_name  | broad_prod_fee  | chengyuan_num  |
+--------------+------------------+-----------------+----------------+
| 1518386qwdZ  |                  | 0               | 4              |
| 1398010RsEt  |                  | 0               | 3              |
| 1518368Yodr  |                  | 0               | 2              |
| 1588341YJcg  |                  | 0               | 4              |
| 1589289FnbQ  |                  | 0               | 3              |
| 1588366anEu  |                  | 0               | 3              |
| 1399027lkMA  |                  | 0               | 4              |
| 1872807zHMr  |                  | 0               | 4              |
| 1588387zHEQ  |                  | 0               | 3              |
| 1528388zIcA  |                  | 0               | 3              |
| 1390810TNjt  |                  | 0               | 4              |
| 1588340GUWZ  |                  | 0               | 3              |
| 1878100FkWQ  |                  | 0               | 3              |
| 1399028aNSA  |                  | 0               | 3              |
| 1377821ToMP  |                  | 0               | 4              |
| 1588428FIdu  |                  | 0               | 3              |
| 1398101TkSy  |                  | 0               | 4              |
+--------------+------------------+-----------------+----------------+


select phone_no,run_code,broad_type,broad_eff_date,broad_prod_name,broad_prod_fee
from datamart.data_dm_uv_info_m
where phone_no = '1878388aHXt' and dy = '2020' and dm = '02'
;
+--------------+-----------+-------------+-----------------+------------------+-----------------+
|   phone_no   | run_code  | broad_type  | broad_eff_date  | broad_prod_name  | broad_prod_fee  |
+--------------+-----------+-------------+-----------------+------------------+-----------------+
| 1878388aHXt  | A         | 1           | 20190601        | 新爱家168（V2.0）     | 168             |
+--------------+-----------+-------------+-----------------+------------------+-----------------+

select phone_no,member_role_name,key_phone_no,kd_no1,prod_prcid_kdtc
from datamart.dw_user_kd
where phone_no = '1398101TkSy' and dy = '2020' and dm = '02'
;

+--------------+-------------------+---------------+--------------+------------------+
|   phone_no   | member_role_name  | key_phone_no  |    kd_no1    | prod_prcid_kdtc  |
+--------------+-------------------+---------------+--------------+------------------+
| 1878388aHXt  | 全家福关键人            | 1878388aHXt   | 2096253pIjy  | ACBZ14198        |
+--------------+-------------------+---------------+--------------+------------------+
+--------------+-------------------+---------------+--------------+------------------+
|   phone_no   | member_role_name  | key_phone_no  |    kd_no1    | prod_prcid_kdtc  |
+--------------+-------------------+---------------+--------------+------------------+
| 1398101TkSy  | 新商务动力共享手机成员       | 2096269lNdQ   | 2096269lNdQ  | ACAZ23795        |
+--------------+-------------------+---------------+--------------+------------------+

select prod_prcid,prod_prc_name,prod_price
from datamart.base_prc_info
where prod_prcid = 'ACAZ23795'
;


select *
from workspace.zb_jizhan_user_guanghan_20200321_result_98
where phone_no = '1398101TkSy'
;

