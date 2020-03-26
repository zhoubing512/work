--各个区县携入携出人数
--各个区县、每月携入携出的人数

+--------------------------+------------+-------------+
|         col_name         | data_type  |   comment   |
+--------------------------+------------+-------------+
| work_type                | string     | 3为携入, 8为携出  |
| trade_id                 | string     | 加密3         |
| accept_month             | string     |             |
| service_type             | string     |             |
| id_no                    | string     | 德阳用户有此字段    |
| phone_no                 | string     |             |
| flow_id                  | string     | 加密3         |
| lsms_accept              | string     | 加密3         |
| message_id               | string     | 加密3         |
| port_out_netid           | string     |             |
| port_in_netid            | string     |             |
| home_netid               | string     |             |
| card_type_after          | string     |             |
| card_type_before         | string     |             |
| req_time                 | string     |             |
| result_code              | string     |             |
| result_message           | string     |             |
| error_message            | string     |             |
| credtype                 | string     |             |
| crednumber               | string     |             |
| authcode                 | string     |             |
| expired                  | string     |             |
| custid                   | string     |             |
| cust_name                | string     |             |
| deal_flag                | string     |             |
| chgtime                  | string     | 携号时间        |
| remark                   | string     |             |
| rsrv_str1                | string     |             |
| rsrv_str2                | string     |             |
| rsrv_str3                | string     |             |
| rsrv_str4                | string     |             |
| rsrv_str5                | string     |             |
| city_id                  | string     | 德阳用户有此字段    |
| carry_operator           | string     | 德阳用户有此字段    |
| carry_type               | string     | 德阳用户有此字段    |
| data_date                | string     | 数据日期        |
| dy                       | string     |             |
| dm                       | string     |             |
|                          | NULL       | NULL        |
| # Partition Information  | NULL       | NULL        |
| # col_name               | data_type  | comment     |
| dy                       | string     |             |
| dm                       | string     |             |
+--------------------------+------------+-------------+

携入
德阳用户
1898105pwdt
1809002GwSg
非德阳用户
+--------------+
|   phone_no   |
+--------------+
| 1738068asvr  |
| 1568086zsjP  |
| 1808037lndL  |
| 1731122pJjQ  |
| 1552035zNEQ  |
| 1330800RNvP  |
| 1319807lJWP  |
| 1862836zoMA  |
| 1850839YNWZ  |
| 1779022lwWP  |
| 1568173YNEQ  |
| 1804800lnWA  |
| 1898053TNvQ  |
| 1801110TISL  |
| 1850813lJbg  |
| 1333075qHjP  |
| 1994079GHSt  |
| 1333075qHjP  |
| 1860800Gkdr  |
| 1568185akvP  |
+--------------+

select phone_no,open_date
from datamart.data_user_info
where dy = '2020' and dm in('02','03') and phone_no = '1739886GISP';

select distinct phone_no,create_date
from datamart.data_user_channel
where phone_no = '1330829RJMQ' and
(dy = '2020' or (dy = '2019' and dm > 10))
limit 5
;

select work_type,phone_no,accept_month,carry_operator,carry_type,data_date,dy,dm
from datamart.data_ods_ur_carryingwork_info
where work_type = 3 and accept_month = '202003'
limit 2;
1330829RJMQ
1739886GISP


select date_time_xr,county_name,count(phone_no) as xr_num
from 
(
    
    select a.*
    from 
    (
        select distinct phone_no,substr(open_date,1,7) as date_time_xr,county_name
        from datamart.data_user_channel a
        where a.dy = '2020' and a.dm = '02' 
    ) a where 
    a.phone_no in
    (
     select phone_no from datamart.data_ods_ur_carryingwork_info where work_type = 3 
    ) 
) a
group by county_name,date_time_xr
order by county_name,date_time_xr
;

+---------------+--------------+---------+
| date_time_xr  | county_name  | xr_num  |
+---------------+--------------+---------+
| 2019-11       | 中江分公司        | 235     |
| 2019-12       | 中江分公司        | 358     |
| 2020-01       | 中江分公司        | 155     |
| 2020-02       | 中江分公司        | 71      |
| 2019-11       | 什邡分公司        | 197     |
| 2019-12       | 什邡分公司        | 365     |
| 2020-01       | 什邡分公司        | 177     |
| 2020-02       | 什邡分公司        | 62      |
| 2019-11       | 广汉分公司        | 195     |
| 2019-12       | 广汉分公司        | 372     |
| 2020-01       | 广汉分公司        | 235     |
| 2020-02       | 广汉分公司        | 46      |
| 2019-11       | 旌阳分公司        | 286     |
| 2019-12       | 旌阳分公司        | 615     |
| 2020-01       | 旌阳分公司        | 194     |
| 2020-02       | 旌阳分公司        | 58      |
| 2019-11       | 绵竹分公司        | 120     |
| 2019-12       | 绵竹分公司        | 347     |
| 2020-01       | 绵竹分公司        | 141     |
| 2020-02       | 绵竹分公司        | 39      |
| 2019-11       | 罗江分公司        | 114     |
| 2019-12       | 罗江分公司        | 207     |
| 2020-01       | 罗江分公司        | 139     |
| 2020-02       | 罗江分公司        | 27      |
+---------------+--------------+---------+

select date_time_xr,county_name,count(phone_no) as xr_num
from 
(
    
    select a.*
    from 
    (
        select distinct phone_no,substr(create_date,1,7) as date_time_xr,county_name
        from datamart.data_user_channel a
        where a.dy = '2020' and a.dm = '02' 
    ) a
    inner join
    (
     select phone_no from datamart.data_ods_ur_carryingwork_info where work_type = 3 
    ) b
    on a.phone_no = b.phone_no
) a
group by county_name,date_time_xr
order by county_name,date_time_xr
;
+---------------+--------------+---------+
| date_time_xr  | county_name  | xr_num  |
+---------------+--------------+---------+
| 2019-11       | 中江分公司        | 237     |
| 2019-12       | 中江分公司        | 362     |
| 2020-01       | 中江分公司        | 156     |
| 2020-02       | 中江分公司        | 71      |
| 2019-11       | 什邡分公司        | 201     |
| 2019-12       | 什邡分公司        | 368     |
| 2020-01       | 什邡分公司        | 177     |
| 2020-02       | 什邡分公司        | 62      |
| 2019-11       | 广汉分公司        | 198     |
| 2019-12       | 广汉分公司        | 372     |
| 2020-01       | 广汉分公司        | 237     |
| 2020-02       | 广汉分公司        | 46      |
| 2019-11       | 旌阳分公司        | 289     |
| 2019-12       | 旌阳分公司        | 617     |
| 2020-01       | 旌阳分公司        | 198     |
| 2020-02       | 旌阳分公司        | 58      |
| 2019-11       | 绵竹分公司        | 122     |
| 2019-12       | 绵竹分公司        | 350     |
| 2020-01       | 绵竹分公司        | 141     |
| 2020-02       | 绵竹分公司        | 39      |
| 2019-11       | 罗江分公司        | 114     |
| 2019-12       | 罗江分公司        | 208     |
| 2020-01       | 罗江分公司        | 140     |
| 2020-02       | 罗江分公司        | 27      |
+---------------+--------------+---------+


select *
from datamart.data_user_channel
where phone_no ='1898105pwdt';

select county_name,accept_month,count(phone_no) as xc_num
from 
(
    select distinct a.phone_no,a.accept_month,b.county_name
    from 
    (
        select distinct phone_no,accept_month,dy,dm,city_id,carry_operator,carry_type,data_date
        from datamart.data_ods_ur_carryingwork_info 
        where work_type = 8 and id_no <> ''
    ) a
    left join 
    (
        select distinct phone_no,county_id,county_name
        from datamart.data_user_channel
        where dy = '2020' or (dy = '2019' and dm > 10)
    ) b
    on a.phone_no = b.phone_no
) a
group by county_name,accept_month
order by county_name,accept_month
;
+--------------+---------------+---------+
| county_name  | accept_month  | xc_num  |
+--------------+---------------+---------+
| 中江分公司        | 201911        | 97      |
| 中江分公司        | 201912        | 289     |
| 中江分公司        | 202001        | 472     |
| 中江分公司        | 202002        | 263     |
| 中江分公司        | 202003        | 58      |
| 什邡分公司        | 201911        | 260     |
| 什邡分公司        | 201912        | 564     |
| 什邡分公司        | 202001        | 933     |
| 什邡分公司        | 202002        | 379     |
| 什邡分公司        | 202003        | 56      |
| 广汉分公司        | 201911        | 193     |
| 广汉分公司        | 201912        | 550     |
| 广汉分公司        | 202001        | 894     |
| 广汉分公司        | 202002        | 209     |
| 广汉分公司        | 202003        | 59      |
| 旌阳分公司        | 201911        | 450     |
| 旌阳分公司        | 201912        | 1157    |
| 旌阳分公司        | 202001        | 1375    |
| 旌阳分公司        | 202002        | 582     |
| 旌阳分公司        | 202003        | 116     |
| 绵竹分公司        | 201911        | 151     |
| 绵竹分公司        | 201912        | 458     |
| 绵竹分公司        | 202001        | 750     |
| 绵竹分公司        | 202002        | 355     |
| 绵竹分公司        | 202003        | 60      |
| 罗江分公司        | 201911        | 114     |
| 罗江分公司        | 201912        | 239     |
| 罗江分公司        | 202001        | 382     |
| 罗江分公司        | 202002        | 173     |
| 罗江分公司        | 202003        | 28      |
+--------------+---------------+---------+









