create table if not exists public.apartment_enrichment (
  id text primary key,
  region_code text not null,
  apt text not null,
  dong text not null,
  lat double precision,
  lng double precision,
  zoning text,
  nearby_station text,
  nearby_station_distance_km double precision,
  nearby_elementary_school text,
  nearby_elementary_school_distance_km double precision,
  nearby_elementary_school_status text,
  nearby_park text,
  nearby_park_distance_km double precision,
  nearby_park_status text,
  flat_land_status text,
  flat_land_elevation_range_m double precision,
  updated_at timestamptz not null default now()
);

alter table public.apartment_enrichment
  add column if not exists lat double precision,
  add column if not exists lng double precision,
  add column if not exists zoning text,
  add column if not exists nearby_station text,
  add column if not exists nearby_station_distance_km double precision,
  add column if not exists nearby_elementary_school text,
  add column if not exists nearby_elementary_school_distance_km double precision,
  add column if not exists nearby_elementary_school_status text,
  add column if not exists nearby_park text,
  add column if not exists nearby_park_distance_km double precision,
  add column if not exists nearby_park_status text,
  add column if not exists flat_land_status text,
  add column if not exists flat_land_elevation_range_m double precision,
  add column if not exists updated_at timestamptz not null default now();

create index if not exists apartment_enrichment_region_dong_idx
  on public.apartment_enrichment (region_code, dong);

create index if not exists apartment_enrichment_updated_at_idx
  on public.apartment_enrichment (updated_at desc);
