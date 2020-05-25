select college_id,college_name,count(distinct phone_no)
from workspace.th_gaoxiao_alluser_detail_2015_to_2019_03_count_result
where student_type='在读'
and run_name in('正常','无')
and id_no <>'0'
group by college_id,college_name
order by int(college_id);


th_gaoxiao_alluser_detail_2015_to_2018_12_count_result

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
        from workspace.th_gaoxiao_alluser_detail_2015_to_2019_11_count_result
        where student_type='在读' and run_name in('正常','无') and id_no <>'0'
     ) a
    left join 
    ( 
        select * 
        from datamart.dw_user_fee_real_v2
        where dy = '2019' and dm = '11'
    ) b on a.phone_no = b.phone_no
) a group by college_name
order by a.college_name
 ;