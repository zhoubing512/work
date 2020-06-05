--携入、携出人数  网格 在数据库中存储网格(网格为最小单位)，区县、分局可根据网格计算得到(需要网格经纬度，用户热力图显示)(按月进行统计)
--包含预测用户数据(预测数据按最新)

--TGI指数，即Target Group Index（目标群体指数），可反映目标群体在特定研究范围(如地理区域、人口统计领域、媒体受众、产品消费者)内的强势或弱势。
--TGI指数= [目标群体中具有某一特征的群体所占比例/总体中具有相同特征的群体所占比例]*标准数100。
--TGI指数表明不同特征用户关注问题的差异情况，其中TGI指数等于100表示平均水平，高于100，代表该类用户对某类问题的关注程度高于整体水平

--携出客户中该网格的占比=该网格携出客户数/该地市总携出客户数
--该网格在所有网格中的占比=该网格客户数/该地市总客户数
--网格的TGI指数=携出客户中该网格的占比/该网格在所有网格中的占比×100（标准数）
----TGI指数等于100表示平均水平，高于100，代表携出客户中该网格的突出程度高于整体水平。最终根据TGI指数，输出TOP10高危携转网格。


--基本信息(针对携出人群)
--年龄分布、网龄分布、套餐分布、集团属性、消费情况、通话及流量情况(需要进行分段的类型，均可以存在这个表当中)、性别


--特征重要性(建模时的一些重要特征)
--异网通话比例、套外费用、通话时长、网龄、流量、消费
--年龄、cqi良好率（网络指标）、切换成功率（网络指标）、volte丢包率（网络指标）、无线掉线率（网络指标）、无线接通率（网络指标）、附加产品费用、附加产品订购次数
--资费价格、资费年龄、呼叫10086次数、sp附加业务订购次数、被10085外呼次数、套餐外费用产生次数
--升档次数、集团等级、附加产品费用产生次数、降档次数、被10086呼叫次数



--MySQL表
----携转：
--表1：dv_carrying_district_info
--列名：主键(自增)、区县、分局、网格、携出人数、携入人数、TGI指数、经纬度(一个网格对应一个点的经纬度)、携出预测、日期(年、月：例 2020-04)、备用字段1、备用字段2、备用字段3、创建时间、更新时间

--表2：(需要分段统计的数据 年龄、网龄、通话时长等) dv_carrying_statistic_segment_info
--列名：主键(自增)、区县、分局、网格、类型(年龄、网龄、通话时长、流量、消费)、分段(例：年龄段，网龄段)、人数、日期(年、月：例 2020-04)、备用字段1、备用字段2、备用字段3、创建时间、更新时间

--表3：(不分段统计的数据 异网通话比例等) dv_carrying_statistic_target_info
--列名：主键(自增)、区县、分局、网格、类型、数据(异网通话比例、套外平均费用、平均通话时长、平均流量、平均消费、性别)、日期(年、月：例 2020-04)、备用字段1、备用字段2、备用字段3、创建时间、更新时间


--每个表的数据：
--表1：携入携出人数、TGI指数、经纬度、携出预测
--表2：年龄、网龄、通话时长、流量、消费(以及其他关注的可分段的数据)
--表3：异网通话比例、套外平均费用、平均通话时长、平均流量、平均消费、性别(以及其他不分段的数据)

----满意度：
--字段分析：
--满意客户：客户沉默离网、转为沉默、客户调研、携转查询、滤网投诉、正常客户
--基础信息：年龄、性别、网龄、流量、通话





----5G 字段分析:
--5G现状：
--5G终端客户(总量人数：分区县、分局、网格) 5G基站(经纬度展示、色块图展示点位) 终端价格、换机周期、换机品牌
--5G资费:资费、流量、超套(总量人数：分区县、分局、网格)

--5G发展 
--5G目标客户(总量人数：分区县、分局、网格)
--5G目标客户标准：一、消费达到5G资费标准；二、流量使用达到一定标准；三、权益类app使用频次高；四、5G手机终端在用客户

------5G MySQL表：
--表1：dv_5g_current_develop_info
--列名：主键(自增)、区县、分局、网格、5G终端客户数量、5G目标客户数量、换机周期、终端价格、换机规模、日期(年、月：例 2020-04)、备用字段1、备用字段2、备用字段3、创建时间、更新时间

--表2：dv_5g_statistic_segment_info
--列名：主键(自增)、区县、分局、网格、客户类型(5g终端客户、5g目标客户)、类型(终端价格、权益类app使用频次、年龄、网龄、通话时长、流量、消费)、分段(例：年龄段，网龄段)、人数、日期(年、月：例 2020-04)、备用字段1、备用字段2、备用字段3、创建时间、更新时间

--表3：dv_5g_statistic_target_info
--列名：主键(自增)、区县、分局、网格、客户类型(5g终端客户、5g目标客户)、类型、数据(终端均价、权益类app使用频次、套外平均费用、平均通话时长、平均流量、平均消费、性别)、日期(年、月：例 2020-04)、备用字段1、备用字段2、备用字段3、创建时间、更新时间

--表4：dv_5g_bs_info
--列名：主键(自增)、区县、分局、网格、基站名称、经纬度(用字符串存储)、备用字段1、备用字段2、备用字段3、创建时间、更新时间


--表5：dv_base_region_info
--列名：主键(自增)、区县id、区县、分局id、分局、网格id、网格、备用字段1、备用字段2、备用字段3、创建时间、更新时间


--提取基站经纬度信息(5G)
SELECT bs_id,bs_name,bs_type,lon,lat
FROM business.base_cell_info 
WHERE day = '20200526'
Limit 5
;


--区县、网格、分局(准备数据，基础数据) 
--网格对应的分局、区县 都必须从网格名称中获取，否则不准 有可能一个网格对应2个或者3个分局、区县
drop table workspace.zb_dv_base_region_info_temp;
create table workspace.zb_dv_base_region_info_temp 
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as 
SELECT county_no,county_name,area_no,split(grid_name,'网格')[0] as area_name,grid_id,grid_name,data_date
FROM business.base_deyang_country_geocode
LIMIT 5
;

drop table workspace.zb_dv_base_region_info;
create table workspace.zb_dv_base_region_info 
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as 
SELECT *
FROM
(
SELECT DISTINCT CASE WHEN substr(area_name,1,2) = '旌阳' THEN 1 
             WHEN substr(area_name,1,2) = '广汉' THEN 2 
             WHEN substr(area_name,1,2) = '什邡' THEN 3 
             WHEN substr(area_name,1,2) = '绵竹' THEN 4 
             WHEN substr(area_name,1,2) = '中江' THEN 5 
             WHEN substr(area_name,1,2) = '罗江' THEN 6 
             ELSE NULL END AS county_no,
            substr(area_name,1,2) as county_name,area_no,substr(area_name,3) as area_name,grid_id,grid_name,'' as reserve1,'' as reserve2,'' as reserve3,
        from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as create_time,
        from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as update_time
FROM 
(
    SELECT *
    FROM workspace.zb_dv_base_region_info_temp
    WHERE data_date = '2020-04-07'
) a
ORDER BY county_name,area_name,grid_name
) a WHERE county_no IS NOT NULL
;

--导出数据
--getdata -t workspace.zb_dv_base_region_info

--检验 
--最新时间为2020-04-07
SELECT county_name,area_name,data_date FROM workspace.zb_dv_base_region_info_temp ORDER BY data_date desc limit 15; 

SELECT grid_name,COUNT(grid_name)
FROM workspace.zb_dv_base_region_info
GROUP BY grid_name
HAVING COUNT(grid_name) > 1
;
--一个网格，多条记录
| 广汉小汉分局网格30  | 2    |
| 广汉小汉分局网格32  | 2    |
| 广汉小汉分局网格33  | 2    |
| 广汉连山分局网格21  | 2    |
| 广汉连山分局网格22  | 2    |
| 广汉连山分局网格24  | 3    |
| 旌阳东湖分局网格2   | 2    |
| 旌阳东湖分局网格5   | 3    |
| 旌阳孝泉分局网格1   | 3    |
| 旌阳孝泉分局网格3   | 2    |
| 旌阳工农村分局网格1  | 2    |
| 旌阳工农村分局网格2  | 2    |
| 旌阳黄许分局网格4   | 2    |
| 绵竹土门分局网格6   | 2    |
| 绵竹富新分局网格5   | 3    |
| 绵竹汉旺分局网格6   | 2    |
| 罗江略坪分局网格2   | 2    |
| 罗江略坪分局网格4   | 2    |
| 罗江略坪分局网格6   | 2    |
SELECT * FROM workspace.zb_dv_base_region_info WHERE grid_name = '旌阳孝泉分局网格1';
SELECT count(DISTINCT area_name) FROM workspace.zb_dv_base_region_info;
SELECT county_no,county_name,area_name,grid_name,grid_id FROM workspace.zb_dv_base_region_info WHERE area_name = '城北分局';
SELECT area_name,count(area_name) FROM 
(
SELECT DISTINCT county_name,area_name FROM workspace.zb_dv_base_region_info
) a GROUP BY area_name HAVING count(area_name) > 1;


--携转表
SELECT work_type,trade_id,accept_month,service_type,phone_no,chgtime,remark,city_id,carry_operator,carry_type,data_date
FROM datamart.data_ods_ur_carryingwork_info
WHERE dy = '2020' AND dm = '05' AND city_id IS NOT NULL AND city_id <> '' AND work_type = 8
ORDER BY work_type
LIMIT 10;


--检查小区数量
SELECT COUNT(DISTINCT ci )
FROM business.dwd_user_location_hour
WHERE dy = '2020' AND dm = '04' 
;
32055
SELECT COUNT(DISTINCT cell_id )
FROM business.base_cell_info
;
52769
