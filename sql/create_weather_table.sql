create schema if not exists raw;

create table if not exists raw.weather_hourly (
  location text not null,
  ts_hour timestamptz not null,
  temperature_2m numeric,
  precipitation numeric,
  wind_speed_10m numeric,
  weather_code int,
  ingested_at timestamptz not null default now(),
  primary key (location, ts_hour)
);
