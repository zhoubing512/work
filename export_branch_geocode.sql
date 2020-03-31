select distinct county_name,area_name
from business.base_deyang_country_geocode
;
--最新的分局
/*
+--------------+------------+
| county_name  | area_name  |
+--------------+------------+
| 中江           | 万福分局       |
| 中江           | 仓山分局       |
| 中江           | 冯店分局       |
| 中江           | 回龙分局       |
| 中江           | 城北分局       |
| 中江           | 城南分局       |
| 什邡           | 师古分局       |
| 什邡           | 洛水分局       |
| 广汉           | 佛山路分局      |
| 广汉           | 大北街分局      |
| 广汉           | 小汉分局       |
| 广汉           | 新丰分局       |
| 广汉           | 肇庆路分局      |
| 旌阳           | 城北分局       |
| 旌阳           | 城南/高新      |
| 旌阳           | 城南分局       |
| 旌阳           | 开发区分局      |
| 旌阳           | 长江路分局      |
| 旌阳           | 高新分局       |
| 旌阳           | 黄许分局       |
| 绵竹           | 城北分局       |
| 绵竹           | 城南分局       |
| 绵竹           | 无归属分局      |
| 绵竹           | 汉旺分局       |
| 罗江           | 略坪分局       |
| 罗江           | 金山分局       |
| 中江           | 冯兴分局       |
| 中江           | 南华分局       |
| 中江           | 广福分局       |
| 中江           | 永太分局       |
| 中江           | 辑庆分局       |
| 中江           | 集凤分局       |
| 中江           | 龙台分局       |
| 什邡           | 双盛分局       |
| 什邡           | 城北分局       |
| 什邡           | 城南分局       |
| 广汉           | 南兴分局       |
| 广汉           | 城南分局       |
| 广汉           | 无归属分局      |
| 广汉           | 连山分局       |
| 旌阳           | 东湖分局       |
| 旌阳           | 孝泉分局       |
| 旌阳           | 旌东/开发区     |
| 旌阳           | 旌东分局       |
| 绵竹           | 土门分局       |
| 绵竹           | 孝德分局       |
| 绵竹           | 富新分局       |
| 罗江           | 中心分局       |
| 罗江           | 城郊分局       |
| 罗江           | 新盛分局       |
| 罗江           | 景区分局       |
| 罗江           | 鄢家分局       |
+--------------+------------+
*/

--导出分局的geocode
--旌阳、罗江、广汉、什邡、绵竹、中江
drop table workspace.zb_geocode_zhongjiang;
create table workspace.zb_geocode_zhongjiang
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select distinct county_name,area_name,geo_code_baidu,lon_baidu,lat_baidu
from business.base_deyang_country_geocode
where county_name = '中江'
;

getdata -t workspace.zb_geocode_zhongjiang  -f zhongjiang.txt;
