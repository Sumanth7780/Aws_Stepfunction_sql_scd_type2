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
  script_id: 020_create_audit_tables
  owner: "Mobility Analytics"
  purpose: "Append-only audit trail for MDM + pipeline runs"
  version: "1.0.0"
*/

/* ------------------------------------------------------------
Author: Sumanth
Owner: Governance
Purpose: Create pipeline audit + SCD change audit + DQ result tables
------------------------------------------------------------ */

create table if not exists governance.pipeline_run_audit (
  run_id        text primary key,
  job_name      text,
  trigger_type  text,
  data_owner    text,
  status        text,
  started_at    timestamp,
  ended_at      timestamp,
  error_message text
);

create table if not exists governance.zone_change_audit (
  audit_id      bigserial primary key,
  run_id        text not null,
  action_type   text not null, -- INSERT|EXPIRE|NOCHANGE|ROLLBACK
  location_id   int,
  old_zone_sk   bigint,
  new_zone_sk   bigint,
  changed_at    timestamp not null default now(),
  changed_by    text not null default current_user,
  details       jsonb
);

create table if not exists governance.dq_results (
  dq_id         bigserial primary key,
  run_id        text not null,
  rule_name     text not null,
  passed        boolean not null,
  fail_count    bigint not null,
  sample        jsonb,
  checked_at    timestamp not null default now()
);
