
  create view "warehouse"."public"."stg_vehicle_listings__dbt_tmp"
    
    
  as (
    select
  listing_id,
  date_trunc('hour', listing_ts) as ts_hour,
  location,
  make,
  model,
  vehicle_year,
  price_sek::numeric as price_sek,
  mileage_km::numeric as mileage_km
from "warehouse"."raw"."vehicle_listings"
  );