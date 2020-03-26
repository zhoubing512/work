--2772 效果追踪
zb_wyd_geo_20191202
zb_wyd_geo_20191218 

宽带新增、目标客户拓展量（协入）、合约

天籁福
水岸花都3期
乐福西苑
蓝兴园
建馨家园
天悦湾
玫瑰湾
保利海德花园
云盘村委会
小汉镇南湖馨苑 ----之前就未找到用户
欧洲半岛
水岸上城


德阳旌阳天籁湖
德阳旌阳水岸花都3期
德阳旌阳乐福西苑
德阳旌阳蓝兴园小区
德阳旌阳建馨家园1区
德阳旌阳天悦湾
德阳旌阳希望城玫瑰湾
德阳旌阳区保利国际海德花园
德阳广汉云盘村委会
欧洲半岛
中江水岸上城

--合并表
drop table workspace.zb_2772_temp;
create table workspace.zb_2772_temp
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
    select *
    from workspace.zb_wyd_geo_20191202
    union all
    select *
    from workspace.zb_wyd_geo_20191218
;

select distinct cell_name
from workspace.zb_2772_temp
where cell_name like '%水岸上城%'
;


德阳旌阳天籁湖
德阳旌阳水岸花都3期
德阳旌阳乐福西苑
德阳旌阳蓝兴园小区
德阳旌阳建馨家园1区
德阳旌阳天悦湾
德阳旌阳希望城玫瑰湾
德阳旌阳区保利国际海德花园
德阳广汉云盘村委会
欧洲半岛
中江水岸上城

select *
from datamart.data_kd_user_t 
where dy = '2019'  and kd_area_name like '%建馨家园一%'
order by dm desc
limit 20;
select * 
from datamart.data_kd_user_t 
where phone_no = '1356841RIcu'
order by dm;

--宽带新增（无宽带的变有宽带，算新增）
drop table workspace.zb_2772_kuandai;
create table workspace.zb_2772_kuandai
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select distinct a.cell_name,b.key_phone_no
from 
(
    select *
    from workspace.zb_2772_temp
    where kd_main_phone is null or kd_main_phone = ''
) a
inner join datamart.dw_user_kd_latest b
on a.phone_no = b.phone_no
inner join datamart.data_user_info c
on b.kd_no1 = c.phone_no
where c.open_date >= '2019-11-01'
;

select cell_name,count(key_phone_no)
from workspace.zb_2772_kuandai
group by cell_name;

select cell_name,count(phone_no)
from
(
    select distinct a.cell_name,a.phone_no
    from zb_wyd_geo_20200110 a
    inner join 
    (
        select * 
        from datamart.data_dm_uv_info_m a
        where dy = '2019' and dm = '12' and is_main_pay = '2'
        and broad_eff_date <> '' and broad_eff_date is not null and broad_eff_date >='20191201'
    ) b
    on a.phone_no = b.phone_no
) a
group by cell_name
;
12月 新跑数据
仟坤伊顿庄园  8
德阳广汉威肯郡 16
安顺巷安国公寓 3
德阳旌阳温莎湖畔小区  1
德阳广汉万兴一品    6
中江蓝湾半岛  11
德阳广汉云盘村委会   1
德阳旌阳乐福西苑    11
1788小区  10
德阳广汉格兰维亚    16
德阳什邡市雍翠家苑   1
蔚泉新村小区  2
德阳广汉广信鹭岛    1
安澜苑 2
德阳旌阳东河佳苑小区  4
德阳什邡宏达世纪新城  1
天悦国际    2
德阳旌阳天籁湖 8
德阳广汉顺德安居小区  12
德阳广汉市雒城印象   4
德阳旌阳爱德华庭二期  8
仟坤国际    1
德阳旌阳春天印象    12
德阳什邡市物华北苑小区 8
剑桥城 3
中江20号安置房    13
德阳广汉欧城联邦三期  2
德阳旌阳建馨家园1区  2
德阳旌阳东湖山美庐别墅 3
中江公园一号  2
人民医院家属区 13
滨江华府    7
圣芭芭拉别墅区 1
德阳香港花园三期    2
德阳旌阳天悦湾 3
上源名城    11
中江祥瑞国际  3
德阳旌阳蔬菜水产公司宿舍    11
德阳什邡市灵杰工业园宿舍    9
德阳绵竹金陵雅居小区  5
德阳旌阳希望城玫瑰湾  10
德阳广汉帝景国际    25
中江中央公园小区    5
德阳什邡市凤凰城    6
玉京名城    3
阳晨秀水湾   1
德阳广汉帝景湾 4
德阳广汉翡翠郡 8
新鸥鹏教育小镇哈佛公寓 1
德阳旌阳区香江华府   1
德阳旌阳帝景峰 3

--直接跑宽带安装小区名字用户
select count(kd_no)
from 
(
    select distinct a.kd_no,a.cell_name,a.add_date
    from workspace.th_encrypt_user_kd_address a
    where a.cell_name like '%云盘%'
    and a.kd_no in
    (
        select a.phone_no 
        from datamart.data_dm_uv_info_m a
        where a.dy = '2019' and a.dm = '12' and a.is_main_pay = '2'
        and a.broad_eff_date <> '' and a.broad_eff_date is not null and a.broad_eff_date >='20191201'
    )
) a
;



8888888888888888888888888888888888888888888888888888888888888888888888888888888888
drop table workspace.zb_kd_cell_id_20200110;
create table workspace.zb_kd_cell_id_20200110(cell_name string,kd_cell_id string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
load data local inpath '/mnt/disk1/user/lj/temp_files/2772_kd_cell_id.txt' into table  workspace.zb_kd_cell_id_20200110;



select distinct a.kd_no,a.cell_name,a.add_date
from workspace.th_encrypt_user_kd_address a
where a.cell_name like '%云盘%'

--查小区名字和id
select distinct kd_cell_id,cell_name
from workspace.th_encrypt_user_kd_address
where cell_name like '%姑苏西区%'
;
--查看该小区新增宽带数量
select cell_name,count(phone_no)
from 
(
    select distinct a.phone_no,b.cell_name
    from 
    (
        select distinct a.phone_no 
        from datamart.data_dm_uv_info_m a
        where a.dy = '2019' and a.dm = '12' and a.is_main_pay = '2'
        and a.broad_eff_date <> '' and a.broad_eff_date is not null and a.broad_eff_date >='20191201'
    ) a
    inner join 
    (
        select *
        from 
        (
            select a.phone_no,b.cell_name
            from datamart.data_kd_user_t a
            inner join workspace.zb_kd_cell_id_20200110 b
            on a.kd_area = b.kd_cell_id
        ) m
    ) b
    on a.phone_no = b.phone_no 
) n
group by cell_name
;
select a.*
from workspace.zb_kd_cell_id_20200110 a
where a.cell_name not in 
(
    select cell_name
    from 
    (
        select a.phone_no,b.cell_name
        from datamart.data_kd_user_t a
        inner join workspace.zb_kd_cell_id_20200110 b
        on a.kd_area = b.kd_cell_id
    ) m
    group by cell_name
) 
;



中江祥瑞国际  5
安国新村安置房 1
德阳旌阳区香江华府   6
龙岭华府    2
德阳广汉市雒城印象   5
德阳广汉那维亚半岛小区 6
德阳什邡市凤凰城    10
德阳什邡市灵杰工业园宿舍    1
德阳旌阳区保利国际海德花园   2
中江水岸上城  4
东汽竹苑小区  1
德阳旌阳乐福西苑    2
德阳绵竹金陵雅居小区  3
德阳旌阳春天印象    7
仟坤伊顿庄园  8
德阳旌阳万嘉国际3期  9
德阳广汉万兴一品    10
德阳旌阳中央绿洲一期  7
迎祥广场小区  1
水岸皇城    2
德阳什邡宏达世纪新城  3
中江铜山名都  1
中江杉春家园  6
德阳广汉顺德安居小区  7
中江中凯一号  4
德阳旌阳迅果电梯公司宿舍    3
德阳旌阳碧桂园1期电梯公寓   5
德阳广汉帝景国际    9
玉京名城    1
金岸湖畔    2
德阳广汉格兰维亚    5
金雁社区    4
德阳广汉威肯郡 6
德阳广汉欧城联邦三期  6
德阳旌阳天悦湾 4
德阳广汉翡翠郡 3
上源名城    8
德阳旌阳天籁湖 6
德阳广汉益好佳家属区  1
安澜苑 5
安顺巷安国公寓 1
仟坤国际    4
德阳旌阳东湖山美庐别墅 1
德阳旌阳希望城玫瑰湾  5
德阳什邡市盛世豪庭   9
德阳旌阳爱德华庭二期  8
中江公园一号  6
滨江华府    4
中江蓝湾半岛  4
德阳什邡市物华北苑小区 6
德阳旌阳城市花园    2
德阳广汉广信鹭岛    7
德阳旌阳九维蓝谷生活区 3
德阳广汉帝景湾 1
中江滨江印象  2
阳晨秀水湾   4
欧洲半岛    2
中江中央公园小区    2
德阳广汉东嘉苑增补   1
德阳广汉紫荆苑 1
德阳广汉市小汉镇南湖馨苑    7
圣芭芭拉别墅区 2
德阳旌阳建馨家园1区  2
1788小区  1
南轩雅居（北区）    3
德阳旌阳帝景峰 5
德阳旌阳东河佳苑小区  9
蔚泉新村小区  3


dw_user_kd_latest
dw_user_kd
1364810GwWQ
2098126aNdP
select * from workspace.zb_2772_temp where phone_no = '1364810GwWQ';
select * from data_user_info where phone_no = '2098126aNdP';
1354826pobg

--携入
zb_2772_xieru 原表
12月新跑数据
drop table workspace.zb_2772_xieru_20200110;
create table workspace.zb_2772_xieru_20200110
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
    select cell_name,count(cy_phone)
    from
    (
        select a.cell_name,a.cy_phone
        from
        (
            select a.phone_no,a.cell_name,a.cy_phone
            from 
            (
                select cell_name,phone_no,cy_phone1 as cy_phone
                from workspace.zb_wyd_geo_20200110
                union all
                select cell_name,phone_no,cy_phone2 as cy_phone
                from workspace.zb_wyd_geo_20200110
                union all
                select cell_name,phone_no,cy_phone3 as cy_phone
                from workspace.zb_wyd_geo_20200110
            ) a
            where a.cy_phone is not null
        ) a
        inner join
        (
            select *
            from datamart.data_user_info 
            --where dy = '2019' and dm in ('12','11')
        ) b
        on a.cy_phone = b.phone_no
    ) a group by cell_name
;

中江20号安置房    5
阳晨秀水湾   3
德阳广汉帝景国际    15
德阳旌阳区北纬31度  6
德阳广汉万兴一品    3
德阳旌阳天籁湖 19
德阳什邡市物华北苑小区 13
茂泉小区3栋  2
德阳广汉欧城联邦三期  7
德阳旌阳帝景峰 6
安澜苑 6
德阳旌阳东湖山美庐别墅 4
德阳广汉云盘村委会   2
中江祥瑞国际  2
德阳广汉翡翠郡 8
上源名城    27
德阳旌阳区保利国际海德花园   2
德阳什邡市凤凰城    15
德阳广汉顺德安居小区  18
德阳旌阳乐福西苑    6
仟坤伊顿庄园  8
德阳旌阳希望城玫瑰湾  17
玉京名城    18
德阳广汉威肯郡 6
德阳什邡宏达世纪新城  2
德阳旌阳天悦湾 8
德阳广汉帝景湾 5
德阳香港花园三期    6
1788小区  23
安顺巷安国公寓 2
新鸥鹏教育小镇哈佛公寓 2
德阳旌阳蔬菜水产公司宿舍    21
中江铜山名都  2
德阳旌阳温莎湖畔小区  9
德阳旌阳春天印象    35
人民医院家属区 23
德阳广汉广信鹭岛    6
德阳什邡市雍翠家苑   8
中江中央公园小区    2
滨江华府    27
德阳旌阳东河佳苑小区  9
剑桥城 12
德阳旌阳爱德华庭二期  4
德阳广汉市雒城印象   11
德阳广汉格兰维亚    43
德阳什邡市灵杰工业园宿舍    10
中江蓝湾半岛  2


--合约
zb_2772_heyue 原表
20200110 新跑数据 zb_2772_heyue_20200110
select cell_name,count(phone_no)
from
(
    select distinct a.cell_name,a.phone_no 
    from workspace.zb_wyd_geo_20200110 a
    inner join 
    (
        select * 
        from datamart.data_user_act
        where substr(oper_date,1,7) >= '2019-12'
    ) b
    on a.phone_no = b.phone_no
) a
group by cell_name
;
中江中凯一号  9
仟坤伊顿庄园  332
中江20号安置房    451
德阳什邡市雍翠家苑   155
金雁社区    6
中江蓝湾半岛  499
阳晨秀水湾   69
中江中央公园小区    131
德阳广汉帝景国际    667
德阳广汉市雒城印象   221
德阳旌阳区北纬31度  61
1788小区  269
安顺巷安国公寓 325
南轩雅居（北区）    55
上源名城    269
新鸥鹏教育小镇哈佛公寓 37
德阳广汉格兰维亚    718
德阳广汉万兴一品    92
德阳旌阳天籁湖 285
德阳旌阳希望城玫瑰湾  229
水岸皇城    2
滨江华府    258
德阳广汉瞿上园别墅区  8
金岸湖畔    3
玉京名城    133
德阳广汉威肯郡 421
德阳旌阳蔬菜水产公司宿舍    337
德阳什邡市物华北苑小区 179
蔚泉新村小区  113
中江铜山名都  35
德阳广汉帝景湾 75
茂泉小区3栋  97
德阳旌阳东河佳苑小区  47
德阳旌阳蓝兴园小区   4
德阳旌阳中央绿洲一期  8
德阳什邡宏达世纪新城  123
德阳广汉欧城联邦三期  202
德阳旌阳帝景峰 173
德阳广汉东嘉苑增补   6
圣芭芭拉别墅区 1
德阳旌阳建馨家园1区  95
德阳旌阳天悦湾 115
东汽竹苑小区  127
德阳旌阳区保利国际海德花园   23
安澜苑 56
德阳什邡市凤凰城    338
中江滨江印象  8
德阳旌阳温莎湖畔小区  175
德阳什邡市灵杰工业园宿舍    430
龙岭华府    7
天悦国际    46
仟坤国际    13
德阳旌阳春天印象    618
德阳广汉顺德安居小区  376
德阳旌阳东湖山美庐别墅 24
人民医院家属区 479
德阳广汉云盘村委会   104
剑桥城 195
德阳旌阳区香江华府   82
德阳旌阳乐福西苑    312
中江公园一号  19
中江祥瑞国际  96
德阳香港花园三期    87
德阳广汉广信鹭岛    81
德阳广汉翡翠郡 266
德阳旌阳爱德华庭二期  214
德阳绵竹金陵雅居小区  269




drop table workspace.zb_2772_heyue_20200110;
create table workspace.zb_2772_heyue_20200110
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
    select cell_name,count(phone_no)
    from 
    (
        select distinct cell_name,a.phone_no
        from 
        (
            select * 
            from workspace.zb_2772_temp
            where shifouheyue = 0
        ) a
        inner join 
        (
            select distinct phone_no 
            from 
            (
                select phone_no 
                from workspace.zb_act_20191128 a 
                left join datamart.dw_user_act b 
                on a.daima = b.means_id_a
            ) a
        ) b
        on a.phone_no = b.phone_no
    ) a group by cell_name
;

    select count(a.phone_no)
    from 
    (
        select * 
        from workspace.zb_2772_temp
        where shifouheyue = 0
    ) a;


zb_2772_xieru
zb_2772_heyue
select * from workspace.zb_2772_xieru
where cell_name like '%天籁%'
or cell_name like '%水岸%'
or cell_name like '%乐福西苑%'
or cell_name like '%蓝兴%'
or cell_name like '%建馨家园%'
or cell_name like '%天悦湾%'
or cell_name like '%玫瑰湾%'
or cell_name like '%海德%'
or cell_name like '%云盘%'
or cell_name like '%南湖馨苑%'
or cell_name like '%欧洲半岛%'
;
--客户信息表，宽带  
drop table workspace.user_kd_address;
create table workspace.user_kd_address(work_order string,city string,county string
    ,shifouhege string,error_msg string,no_monitor_reason string,uder_id string,kd_no string,sline_state string
    ,net_in_date string,fugaifanwei_id string,kd_cell_id string,cell_name string,jierushebei_name string,jieru_port_name string,OLT_name string,OLT_port string,new_ONU string
    ,shebei_version string,fugaifangshi string,fugaidiyu string,huxianfangshi string
    ,LOIDhao string,PON string,shouli_hall string,shouli_hall_worker string,shouli_hall_phone string,shouli_hall_worker_jobnumber string,weihurenyuan string,biaozhun_address string
    ,CVLAN string,SVLAN string, ODNshebei string,ODNduankou string,prc_norm string,jilianjiaohuanji string,jiaohuanji_port string,use_fugaifanwei string,add_person string,add_date string,tuiwang_date string,kd_tv_zhanghao string,IMS_user string,IMS_number string,IMS_default_user string,update_mark string,Flowid string,alter_person string,alter_date string,time_stamp string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
load data local inpath '/mnt/disk1/user/lj/temp_files/kehuxinxi_Copy123.txt' into table  workspace.user_kd_address;

select * from workspace.user_kd_address where city = '德阳' limit 1;

select kd_no,cell_name,add_date
from workspace.th_encrypt_user_kd_address
where cell_name like '%欧洲半岛%' 
and substr(add_date,1,4) = '2019'
limit 5;
select from_unixtime(unix_timestamp('2013/2/23  15:35:05','yyyy-mm-dd'),'yyyymmdd');
slelect unix_timestamp('2013/2/23  15:35:05');
select datediff('2013-2-23','2019-12-23');


    where cell_name like '%天籁%'
    or cell_name like '%水岸%'
    or cell_name like '%乐福西苑%'
    or cell_name like '%蓝兴%'
    or cell_name like '%建馨家园%'
    or cell_name like '%天悦湾%'
    or cell_name like '%玫瑰湾%'
    or cell_name like '%海德%'
    or cell_name like '%云盘%'
    or cell_name like '%南湖馨苑%'
    or cell_name like '%欧洲半岛%'

select a.cell_name ,b.open_date 
from 
(
    select *
    from workspace.th_encrypt_user_kd_address
where work_order in ('20981292918','20981292927','20981292921','20981292791','20981292911','20981292939')

) a
inner join 
(
    select a.key_phone_no,b.open_date
    from 
    (
        select *
        from datamart.dw_user_kd_latest
    ) a
    inner join
    (
    select *
    from datamart.data_user_info
    where dy = '2019' and dm = '12'
    ) b
    on a.kd_no1 = b.phone_no
) b
on a.kd_no = b.key_phone_no
where b.open_date > '2019-12-01'
limit 20;
--用宽带号码
select *
from workspace.th_encrypt_user_kd_address a
order by a.work_order desc
limit 1;

select kd_no
from workspace.th_encrypt_user_kd_address
where work_order = '20981290138'
--in ('20981292882','20981292884','20981292885','20981292888','20981292890','20981292892','20981292895','20981292900','20981292901','20981292904','20981292908','20981292910','20981292911','20981292914','20981292915')
20981292882
20981292884
20981292885
20981292888
20981292890
20981292892
20981292895
20981292900
20981292901
20981292904
20981292908
20981292910
20981292911
20981292914
20981292915


20981292918
20981292919
20981292920
20981292921
20981292927


20981292929
20981292939
20981292920


select *
from datamart.dw_user_kd_latest
where phone_no = '1828055zHdA'
--in  ('1389020RNby','2098129qJbu','1522884qJdA','2098129qwWZ','1822716FwXg','1588367pwbt','2098129qwWP','1370810GkXy','1592830YkvL','1518101RnMy','1518381RJXy','2098129qwcA','1518101TJct','1518104YUdA','1882851aIvt')
;
1389020RNby
2098129qJbu
1522884qJdA
2098129qwWZ
1822716FwXg
1588367pwbt
2098129qwWP
1370810GkXy
1592830YkvL
1518101RnMy
1518381RJXy
2098129qwcA
1518101TJct
1518104YUdA
1882851aIvt


1355062zoEL
2098129qJXg
1588363FUbL
1389021aUSr
2098129qJXt

1598384Rwvr
1888089Todu
1389021aUSr
select *
from datamart.data_user_info
where phone_no = '1389021aUSr'
and dy = '2019' and dm = '12'
;


select kd_no,c.open_date
from workspace.th_encrypt_user_kd_address a
inner join datamart.dw_user_kd_latest b
on a.kd_no = b.phone_no
inner join 
(
    select *
    from datamart.data_user_info
    where dy = '2019' and dm = '12'
    --and open_date >= '2019-12-01'
) c
on b.kd_no1 = c.phone_no
order by c.open_date desc limit 20;
where a.work_order = '20963883638'
;

select count(kd_no)
from workspace.th_encrypt_user_kd_address a
inner join datamart.dw_user_kd_latest b
on a.kd_no = b.phone_no
667686

select phone_no,key_phone_no,kd_no1 from datamart.dw_user_kd_latest where phone_no like '209%' limit 10;
--data_user_info 12月 597个
select count(*) 
from datamart.dw_user_kd_latest b
inner join 
(
    select *
    from datamart.data_user_info
    where dy = '2019' and dm = '12'
    and open_date >= '2019-12-01'
) c 
on b.kd_no1 = c.phone_no;



