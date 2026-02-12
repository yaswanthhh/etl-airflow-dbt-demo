create schema if not exists raw;

create table if not exists raw.vehicle_listings (
  listing_id text primary key,
  listing_ts timestamptz not null,
  location text not null,
  make text not null,
  model text not null,
  vehicle_year int not null,
  price_sek numeric not null,
  mileage_km numeric not null,
  source_file text not null,
  ingested_at timestamptz not null default now()
);
