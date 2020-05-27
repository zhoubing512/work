select college_id,college_name,count(distinct phone_no)
from workspace.th_gaoxiao_alluser_detail_2015_to_2019_03_count_result
where student_type='在读'
and run_name in('正常','无')
and id_no <>'0'
group by college_id,college_name
order by int(college_id);


--th_gaoxiao_alluser_detail_2015_to_2018_12_count_result

--5月  th_gaoxiao_alluser_detail_2015_to_2019_05_count_result 
--4月  th_gaoxiao_alluser_detail_2015_to_2019_04_count_result 
--3月  th_gaoxiao_alluser_detail_2015_to_2019_03_count_result 

select a.college_name,sum(consume) as college_consume,sum(ss_fee) as college_ss_fee
from 
(
    select distinct a.college_id,a.college_name,a.phone_no,b.consume,b.ss_fee
    from workspace.th_gaoxiao_alluser_detail_2015_to_2018_12_count_result a
    left join 
    ( 
        select * 
        from datamart.data_user_fee_detail_hy
        where dy = '2018' and dm = '12'
    ) b on a.phone_no = b.phone_no
) a group by college_name
 ;


select a.college_name,sum(total_fee) as college_ss_fee,count(distinct phone_no)
from 
(
    select distinct a.college_id,a.college_name,a.phone_no,b.total_fee
    from 
    (
        select *
        from workspace.th_gaoxiao_alluser_detail_2015_to_2018_12_count_result
        where student_type='在读' and run_name in('正常','无') and id_no <>'0'
     ) a
    left join 
    ( 
        select * 
        from datamart.dw_user_fee_real_v2
        where dy = '2019' and dm = '03'
    ) b on a.phone_no = b.phone_no
) a group by college_name
order by a.college_name
 ;


-- +-----------+-----------+
-- | run_code  | run_name  |
-- +-----------+-----------+
-- | C         | 单停        |
-- | E         | 预接入       |
-- | L         | 强关        |
-- | M         | 保号        |
-- | U         | 未实名单停     |
-- | W         | 物联网沉默期    |
-- | b         | 局拆        |
-- | e         | 携出预销      |
-- | 0         | 在途        |
-- | A         | 正常        |
-- | B         | 冒高        |
-- | D         | 欠停        |
-- | G         | 报停        |
-- | H         | 挂失        |
-- | I         | 预销        |
-- | J         | 预拆        |
-- | K         | 强开        |
-- | Q         | 未实名停机     |
-- | X         | 携号        |
-- | Y         | 未实名预拆     |
-- | q         | 疑似骚扰诈骗停机  |
-- | u         | 疑似骚扰诈骗单停  |
-- +-----------+-----------+

 --看18年12月这批人在 19年 1，2，3，4，5月离网了多少人

 select data_year,run_name,phone_no,college_name,first_year,last_year,all_year,should_years,count_years,graduate_year,student_type
 from workspace.th_gaoxiao_alluser_detail_2015_to_2018_12_count_result a
 limit 2;

SELECT DISTINCT run_code,run_name
FROM datamart.data_user_info 
WHERE dy = '2020' and dm = '05'
;


-- 流失定义：最终离网的用户, 包含: 'I', 'J', 'G' 分别为预销、预拆、停机保号, 预销为用户主动行为, 预拆为被动行为, 停机保号说明用户当前不使用该号码
SELECT dm,COUNT(DISTINCT phone_no ) AS lw_dm_num
FROM
(
    SELECT DISTINCT phone_no,dm,run_code,run_name
    FROM
    (
        SELECT DISTINCT a.phone_no,b.run_code,b.run_name,b.dm
        FROM 
        (
            SELECT *
            FROM workspace.th_gaoxiao_alluser_detail_2015_to_2018_12_count_result
            WHERE student_type='在读' AND run_name IN ('正常','无') AND id_no <>'0'
        ) a
        LEFT JOIN 
        (
            SELECT *
            FROM
            (
                SELECT phone_no,run_code,run_name,run_date,dm,
                        ROW_NUMBER() OVER(PARTITION BY phone_no ORDER BY run_date desc) rn
                FROM
                (
                    SELECT *
                    FROM datamart.data_user_info
                    WHERE dy = '2019' AND dm IN ('01','02','03','04','05','06')
                ) a 
            ) a WHERE rn = 1
        ) b ON a.phone_no = b.phone_no
    ) a WHERE a.run_code IN('I', 'J', 'G')
) a
GROUP BY a.dm
ORDER BY a.dm
;
/*
+-----+------------+
| 月份  | 人数  |
+-----+------------+
| 01  | 369        |
| 02  | 275        |
| 03  | 713        |
| 04  | 740        |
| 05  | 979        |
| 06  | 875        |
+-----+------------+

*/

--用18年12月的学生数据跑19年1 2 3 4 5月的收入，跑数同时需要剔除这些月份销户用户
select a.college_name,sum(total_fee) as college_ss_fee,count(distinct phone_no)
from 
(
    select distinct a.college_id,a.college_name,a.phone_no,b.total_fee
    from 
    (--剔除了销户后的用户
        SELECT DISTINCT phone_no,dm,run_code,run_name,college_name,college_id
        FROM
        (
            SELECT DISTINCT a.phone_no,b.run_code,b.run_name,b.dm,a.college_name,a.college_id
            FROM 
            (
                SELECT *
                FROM workspace.th_gaoxiao_alluser_detail_2015_to_2018_12_count_result
                WHERE student_type='在读' AND run_name IN ('正常','无') AND id_no <>'0'
            ) a
            LEFT JOIN 
            (
                SELECT *
                FROM
                (
                    SELECT phone_no,run_code,run_name,run_date,dm,
                            ROW_NUMBER() OVER(PARTITION BY phone_no ORDER BY run_date desc) rn
                    FROM
                    (
                        SELECT *
                        FROM datamart.data_user_info
                        WHERE dy = '2019' AND dm IN ('01','02','03','04','05')
                    ) a 
                ) a WHERE rn = 1
            ) b ON a.phone_no = b.phone_no
        ) a WHERE a.run_code NOT IN('I', 'J', 'G')
     ) a
    left join 
    ( 
        select * 
        from datamart.dw_user_fee_real_v2
        where dy = '2019' and dm = '05'
    ) b on a.phone_no = b.phone_no
) a group by college_name
order by a.college_name
 ;
