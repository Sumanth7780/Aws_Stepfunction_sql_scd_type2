/*
governance:
  script_id: 100_zone_dim
  author: "Sumanth Dadi"
  owner: "Mobility Analytics"
  steward: "Data Steward - Zones"
  domain: "mdm_location"
  classification: "internal"
  purpose: "Create zone_dim from SCD2 master for analytics joins"
  dependencies:
    - mdm.taxi_zone_master
  quality_expectations:
    - "no_null_location_id"
    - "exactly_one_current_version_per_record"
  change_ticket: "CHG-0001"
  version: "1.0.0"
*/
/* ------------------------------------------------------------
Author: Sumanth
Owner: Data Engineering
Purpose: Create master data + SCD2 dimension tables (Zone)
Dependencies: Postgres RDS
Quality: enforced by constraints + DQ scripts
------------------------------------------------------------ */

create schema if not exists staging;
create schema if not exists mdm;
create schema if not exists governance;

-- Staging incoming feed from S3
create table if not exists staging.stg_zone_master_incoming (
  location_id   int,
  borough       text,
  zone_name     text,
  service_zone  text,
  ingested_at   timestamp,
  run_id        text
);

-- SCD2 dimension (current + history in same table)
create table if not exists mdm.zone_dim_scd2 (
  zone_sk        bigserial primary key,
  location_id    int not null,                 -- business key
  borough        text not null,
  zone_name      text not null,
  service_zone   text not null,

  hashdiff       text not null,                -- change detection hash
  effective_from timestamp not null,
  effective_to   timestamp,
  is_current     boolean not null default true,
  version_number int not null default 1,

  created_at     timestamp not null default now(),
  created_by     text not null default current_user,

  approved_at    timestamp,
  approved_by    text,
  approval_status text not null default 'PROPOSED',  -- PROPOSED|APPROVED|REJECTED
  change_reason  text,
  source_run_id  text
);

create index if not exists ix_zone_dim_scd2_bk_current
  on mdm.zone_dim_scd2(location_id)
  where is_current = true;

create index if not exists ix_zone_dim_scd2_effective
  on mdm.zone_dim_scd2(location_id, effective_from, effective_to);

-- Version control header table (optional but useful)
create table if not exists governance.mdm_version_control (
  entity_name     text not null,
  run_id          text not null,
  proposed_at     timestamp not null default now(),
  approved_at     timestamp,
  approved_by     text,
  status          text not null default 'PROPOSED', -- PROPOSED|APPROVED|ROLLED_BACK
  reason          text,
  primary key(entity_name, run_id)
);
