drop table workspace.zb_20200410_yiwang_linshi;
create table workspace.zb_20200410_yiwang_linshi
  row format delimited fields terminated by '\001' 
  stored as orc tblproperties ('orc.compress'='ZLIB') 
  as
  select distinct *
  from
  (
        select distinct a.opp_phone_no,a.opp_type,b.county_name,b.area_name,b.grid_name
        from workspace.th_en_zb_dx_lt_user_202003 a
        left join
        (
            select distinct county_name,area_name,grid_id,grid_name
            from business.base_deyang_country_geocode
        )  b on  a.grid_id = b.grid_id
  ) a where county_name like '%旌阳%'
;

