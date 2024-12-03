create or replace table crime.files 
partition by date_trunc(date,month) 
as 
select unique_key,date,primary_type,arrest,domestic,
latitude,longitude,community_area
from bigquery-public-data.chicago_crime.crime;

create or replace table crime.universe as 
select 
* except(date,primary_type),
case when primary_type 
in ('THEFT',
'BATTERY',
'CRIMINAL DAMAGE',
'ASSAULT',
'MOTOR VEHICLE THEFT',
'DECEPTIVE PRACTICE') then primary_type else 'OTHER' end as crime_type,
date_trunc(date,week) as fh_week
from crime.files 
where cast(date as date) between '2021-01-01' and CURRENT_DATE() and 
not domestic;

create or replace table crime.aggregated as 
with agg as (
select cast (fh_week as date) fh_week,community_area,crime_type,count(*) as num_crimes
from `anahuac-bi.crime.universe`
group by all)
,
cat as (
  select * from 
  (select distinct cast(fh_week as date) fh_week from `anahuac-bi.crime.universe`),
  (select distinct crime_type from `anahuac-bi.crime.universe`),
  (select distinct community_area from `anahuac-bi.crime.universe`)

)
select A.fh_week,crime_type,community_area,sum(coalesce(B.num_crimes,0)) as num_crimes 
from cat A full outer join agg B using(fh_week,crime_type,community_area)
where community_area is not null 
group by all

;

create or replace table  crime.tad as 
select * from (
select 
*,
avg(num_crimes) over (partition by crime_type,community_area order by fh_week rows between 11 preceding and 0 following) as x_avg_crimes ,
min(num_crimes) over (partition by crime_type,community_area order by fh_week rows between 11 preceding and 0 following) as x_min_crimes ,
max(num_crimes) over (partition by crime_type,community_area order by fh_week rows between 11 preceding and 0 following) as x_max_crimes ,
extract(month from fh_week) as x_mes,
sum(num_crimes) over (partition by crime_type,community_area order by fh_week rows between 1 following and 2 following) as tgt,
row_number() over (partition by crime_type,community_area order by fh_week) as rn
from `anahuac-bi.crime.aggregated`
order by fh_week, crime_type,community_area)
where fh_week between '2021-03-14' and '2024-11-10';

CREATE OR REPLACE MODEL crime.primer_modelo
OPTIONS(
  model_type = 'BOOSTED_TREE_REGRESSOR',
  input_label_cols = ['tgt']
) AS
SELECT
  x_avg_crimes,x_max_crimes,x_mes,x_min_crimes,tgt
FROM
  crime.tad
;

