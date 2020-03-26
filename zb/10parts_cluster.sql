--10类用户，每一类均有特征，根据不同的类别进行打标，将所有的用户进行聚类
tzq_20191220_education_phone
2773908
>= 100  142871  家长用户识别模型
lj_app_data_20191108_tmp1_type_2
1495894 金融支付用户识别模型
th_social_contact_phone_call_num_step5
th_fastmail_phone_call_num_step8
623703 网购用户识别模型
th_fastmail_phone_position_change_num_result_step6
1465 快递用户识别模型
hp_video_app_bxl_target_user_20190630
33178 重度视频用户识别模型
hp_wzry_user_os_flag_20190621_d
24069 高配游戏用户识别模型
th_stay_work_yidi_2019_detail
36587 候鸟用户识别模型
lj_tvb_twoprc_info_20190612
74047 电视包用户画像模型
th_child_to_family_20190231
38414 腕表家长用户圈识别模型
lj_model_student_wb_user_20190903
30968 腕表用户识别模型
th_chunjie_2018_back
210920 返乡用户识别模型

--导出表到文件
insert overwrite local directory '/mnt/disk1/user/zhoubing/results/zb_10part_cluster_data' row format delimited fields 
terminated by ',' 
select * from workspace.zb_10part_cluster_data where 1=1 
;
--退出hive压缩文件
sh merge_reduce.sh /mnt/disk1/user/zhoubing/results/zb_10part_cluster_data;


--用户打标集合表
drop table workspace.zb_10part_cluster_data;
create table workspace.zb_10part_cluster_data
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
select distinct a.phone_no
        ,case when b.phone_no is not null then 1 else 0 end as education
        ,case when c.phone_no is not null then 1 else 0 end as finance_pay
        ,case when d.phone_no is not null then 1 else 0 end as online_shopping
        ,case when e.phone_no is not null then 1 else 0 end as fast_mail
        ,case when f.phone_no is not null then 1 else 0 end as vedio_target
        ,case when g.phone_no is not null then 1 else 0 end as wzry_user
        ,case when h.phone_no is not null then 1 else 0 end as stay_work_yidi
        ,case when i.phone_no is not null then 1 else 0 end as tvb
        ,case when j.phone_no is not null then 1 else 0 end as child_to_family
        ,case when k.phone_no is not null then 1 else 0 end as student_wb_use
        ,case when m.phone_no is not null then 1 else 0 end as back_deyang
        ,case when n.phone_no is not null then 1 else 0 end as dm_user
from 
(
    select phone_no
    from workspace.tzq_20191220_education_phone
    where score >= 100
    union all
    select phone_no
    from workspace.lj_app_data_20191108_tmp1_type_2
    union all
    select phone_no
    from workspace.th_fastmail_phone_call_num_step8
    union all
    select phone_no
    from workspace.th_fastmail_phone_position_change_num_result_step6
    union all
    select phone_no
    from workspace.hp_video_app_bxl_target_user_20190630
    union all
    select phone_no
    from workspace.hp_wzry_user_os_flag_20190621_d
    union all
    select phone_no
    from workspace.th_stay_work_yidi_2019_detail
    union all
    select phone_no
    from workspace.lj_tvb_twoprc_info_20190612
    union all
    select phone_no
    from workspace.th_child_to_family_20190231
    union all
    select phone_no
    from workspace.lj_model_student_wb_user_20190903
    union all
    select phone_no
    from workspace.th_chunjie_2018_back
    union all
    select phone_no
    from workspace.zb_dm_all_user_result
) a
left join 
(
    select phone_no 
    from workspace.tzq_20191220_education_phone
    where score >= 100
) b
on a.phone_no = b.phone_no
left join workspace.lj_app_data_20191108_tmp1_type_2 c
on a.phone_no = c.phone_no
left join workspace.th_fastmail_phone_call_num_step8 d
on a.phone_no = d.phone_no
left join workspace.th_fastmail_phone_position_change_num_result_step6 e
on a.phone_no = e.phone_no
left join workspace.hp_video_app_bxl_target_user_20190630 f
on a.phone_no = f.phone_no
left join workspace.hp_wzry_user_os_flag_20190621_d g
on a.phone_no = g.phone_no
left join workspace.th_stay_work_yidi_2019_detail h
on a.phone_no = h.phone_no
left join workspace.lj_tvb_twoprc_info_20190612 i
on a.phone_no = i.phone_no
left join workspace.th_child_to_family_20190231 j
on a.phone_no = j.phone_no
left join workspace.lj_model_student_wb_user_20190903 k
on a.phone_no = k.phone_no
left join workspace.th_chunjie_2018_back m
on a.phone_no = m.phone_no
left join workspace.zb_dm_all_user_result n
on a.phone_no = n.phone_no
;