
  
    

  create  table "warehouse"."public"."fact_listings_weather_hourly__dbt_tmp"
  
  
    as
  
  (
    with listings as (
  select
    location,
    ts_hour,
    count(*) as listings_cnt,
    avg(price_sek) as avg_price_sek
  from "warehouse"."public"."stg_vehicle_listings"
  group by 1,2
),
weather as (
  select * from "warehouse"."public"."stg_weather_hourly"
)
select
  l.location,
  l.ts_hour,
  l.listings_cnt,
  l.avg_price_sek,
  w.temperature_2m,
  w.precipitation,
  w.wind_speed_10m,
  w.weather_code
from listings l
left join weather w
  on l.location = w.location
 and l.ts_hour = w.ts_hour
  );
  