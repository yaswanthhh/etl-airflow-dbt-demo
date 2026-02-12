select
  location,
  ts_hour,
  temperature_2m::numeric as temperature_2m,
  precipitation::numeric as precipitation,
  wind_speed_10m::numeric as wind_speed_10m,
  weather_code::int as weather_code
from {{ source('raw','weather_hourly') }}
