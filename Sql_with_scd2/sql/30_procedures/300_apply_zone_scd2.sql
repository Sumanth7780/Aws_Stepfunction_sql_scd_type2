/* ------------------------------------------------------------
Author: Sumanth
Owner: MDM
Purpose: Apply SCD Type 2 for zone_dim based on candidate data
Logic:
 - If BK not found -> insert new current row
 - If BK found and hashdiff same -> no change
 - If BK found and hashdiff changed -> expire old + insert new
Audit: writes to governance.zone_change_audit
------------------------------------------------------------ */

-- Ensure mdm_version_control row exists
insert into governance.mdm_version_control(entity_name, run_id, status)
values ('ZONE', current_setting('app.run_id', true), 'PROPOSED')
on conflict(entity_name, run_id) do nothing;

-- 1) Insert brand-new BKs
with cand as (
  select * from staging.v_zone_candidate
),
new_bk as (
  select c.*
  from cand c
  left join mdm.zone_dim_scd2 d
    on d.location_id = c.location_id
   and d.is_current = true
  where d.location_id is null
),
ins as (
  insert into mdm.zone_dim_scd2(
    location_id, borough, zone_name, service_zone,
    hashdiff, effective_from, effective_to, is_current, version_number,
    approval_status, source_run_id, change_reason
  )
  select
    location_id, borough, zone_name, service_zone,
    hashdiff, now(), null, true, 1,
    'PROPOSED', current_setting('app.run_id', true),
    'Initial load'
  from new_bk
  returning zone_sk, location_id
)
insert into governance.zone_change_audit(run_id, action_type, location_id, new_zone_sk, details)
select
  current_setting('app.run_id', true),
  'INSERT',
  location_id,
  zone_sk,
  jsonb_build_object('type','new_bk')
from ins;

-- 2) Detect changes for existing BKs (hashdiff changed)
with cand as (
  select * from staging.v_zone_candidate
),
curr as (
  select d.*
  from mdm.zone_dim_scd2 d
  where d.is_current = true
),
chg as (
  select
    c.location_id,
    c.borough, c.zone_name, c.service_zone, c.hashdiff,
    curr.zone_sk as old_zone_sk,
    curr.version_number as old_version
  from cand c
  join curr on curr.location_id = c.location_id
  where curr.hashdiff <> c.hashdiff
),
expired as (
  update mdm.zone_dim_scd2 d
     set is_current = false,
         effective_to = now()
  from chg
  where d.zone_sk = chg.old_zone_sk
  returning d.zone_sk, d.location_id
),
ins2 as (
  insert into mdm.zone_dim_scd2(
    location_id, borough, zone_name, service_zone,
    hashdiff, effective_from, effective_to, is_current, version_number,
    approval_status, source_run_id, change_reason
  )
  select
    location_id, borough, zone_name, service_zone,
    hashdiff, now(), null, true, old_version + 1,
    'PROPOSED', current_setting('app.run_id', true),
    'Attribute change detected'
  from chg
  returning zone_sk, location_id
)
insert into governance.zone_change_audit(run_id, action_type, location_id, old_zone_sk, new_zone_sk, details)
select
  current_setting('app.run_id', true),
  'EXPIRE+INSERT',
  chg.location_id,
  chg.old_zone_sk,
  ins2.zone_sk,
  jsonb_build_object(
    'old_version', chg.old_version,
    'new_version', chg.old_version + 1
  )
from chg
join ins2 on ins2.location_id = chg.location_id;

-- 3) Optional: log no-change rows (useful for transparency)
insert into governance.zone_change_audit(run_id, action_type, location_id, details)
select
  current_setting('app.run_id', true),
  'NOCHANGE',
  c.location_id,
  jsonb_build_object('hashdiff','same')
from staging.v_zone_candidate c
join mdm.zone_dim_scd2 d
  on d.location_id = c.location_id
 and d.is_current = true
where d.hashdiff = c.hashdiff;
