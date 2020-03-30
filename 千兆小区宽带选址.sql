--5G智享套餐（家庭版）
--169元档、269元档、369元档、569元档、869元档
--5G智享套餐（个人版）
--128元档、158元档、198元档、238元档、298元档、398元档、598元档


--5G 资费对应千兆宽带
select prod_prcid,prod_prc_name,prod_prc_desc 
from datamart.base_prc_info
where prod_prc_type = 0 and (prod_prc_name like '%5G%' or prod_prc_name like '%5g%')
limit 2;

--查看已经办理的5G套餐业务有哪些
select distinct b.prod_prc_name,a.main_prc_fee
from 
(
    select *
    from datamart.data_dm_uv_info_m
    where dy = '2020' and dm = '02' 
) a 
left join datamart.base_prc_info b
on a.main_prod_prcid = b.prod_prcid
where b.prod_prc_name like '%5G%'
;
+------------------+-----------------+
| b.prod_prc_name  | a.main_prc_fee  |
+------------------+-----------------+
| 5G智享套餐158元       | 158.00          |
| 5G智享套餐128元       | 128.00          |
| 5G智享套餐398元       | 398.00          |
| 智慧爱家5G极速成员资费     | 0.00            |
| 5G智享套餐（家庭版）169档  | 169.00          |
| 5G智享套餐198元       | 198.00          |
| 5G智享套餐598元       | 598.00          |
| 智慧爱家5G优享成员资费     | 0.00            |
| 5G智享套餐198档版      | 198.00          |
| 5G智享套餐128档版      | 128.00          |
| 5G智享套餐238元       | 238.00          |
| 5G智享套餐298元       | 298.00          |
+------------------+-----------------+


select count(*)
from 
(
    select a.phone_no,b.prod_prc_name,a.arpu,a.avg_3mon_arpu
    from 
    (
        select *
        from datamart.data_dm_uv_info_m
        where dy = '2020' and dm = '02' 
    ) a 
    left join datamart.base_prc_info b
    on a.main_prod_prcid = b.prod_prcid
    where b.prod_prc_name like '%5G%'
) a
limit 20
;
+--------------+------------------+---------+------------------+
|  a.phone_no  | b.prod_prc_name  | a.arpu  | a.avg_3mon_arpu  |
+--------------+------------------+---------+------------------+
| 1828054zkcu  | 智慧爱家5G极速成员资费     | 6.00    | 18.48            |
| 1399027RIWL  | 5G智享套餐158元       | 172.35  | 433.43           |
| 1518105pnMy  | 智慧爱家5G极速成员资费     | 0.00    | 0.00             |
| 1354170GsML  | 5G智享套餐128元       | 131.20  | 121.73           |
| 1588426lkbu  | 智慧爱家5G极速成员资费     | 13.10   | 48.22            |
| 1388108zUdt  | 智慧爱家5G极速成员资费     | 190.60  | 255.30           |
| 1398100YIjA  | 智慧爱家5G优享成员资费     | 146.00  | 174.59           |
| 1828050TJMt  | 智慧爱家5G极速成员资费     | 0.00    | 18.53            |
| 1361810GkXL  | 5G智享套餐128元       | 142.20  | 139.46           |
| 1828108aNvr  | 智慧爱家5G极速成员资费     | 241.00  | 301.20           |
| 1368964TJdP  | 智慧爱家5G优享成员资费     | 148.00  | 181.13           |
| 1390810TJWt  | 5G智享套餐298元       | 198.01  | 162.39           |
| 1878104TISg  | 智慧爱家5G优享成员资费     | 0.00    | 13.36            |
| 1878380lsSy  | 智慧爱家5G极速成员资费     | 238.91  | 183.08           |
| 1356840YUdt  | 5G智享套餐198元       | 270.89  | 247.65           |
| 1828380FsbZ  | 智慧爱家5G优享成员资费     | 8.10    | 16.03            |
| 1350802lHdy  | 智慧爱家5G优享成员资费     | 2.40    | 116.05           |
| 1366830aUSQ  | 5G智享套餐128元       | 139.40  | 158.70           |
| 1354703TJbQ  | 5G智享套餐128元       | 132.30  | 113.58           |
| 1399028RHbt  | 5G智享套餐128元       | 136.00  | 116.67           |
+--------------+------------------+---------+------------------+
select phone_no,id_no,group_id,member_id,member_type,member_role_id,member_role_name,member_role_desc,short_num,state,group_eff_time,group_exp_time
from datamart.data_user_group
where dy = '2020' and dm = '03' and phone_no = '1350802lHdy'
;