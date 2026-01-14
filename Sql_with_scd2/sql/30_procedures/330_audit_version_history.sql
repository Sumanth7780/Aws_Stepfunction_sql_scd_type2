/* ------------------------------------------------------------
Author: Sumanth
Owner: Audit
Purpose: Show SCD2 history + approvals + change lineage
------------------------------------------------------------ */

-- Full history
select
  location_id,
  zone_sk,
  borough, zone_name, service_zone,
  effective_from, effective_to,
  is_current,
  version_number,
  approval_status,
  approved_at, approved_by,
  source_run_id
from mdm.zone_dim_scd2
order by location_id, effective_from;

-- Point-in-time helper (example)
-- Replace '2025-08-15' with any as_of timestamp
-- select * from mdm.zone_dim_scd2
-- where location_id = 1
--   and effective_from <= timestamp '2025-08-15'
--   and (effective_to is null or effective_to > timestamp '2025-08-15');

-- Changes in this run
select *
from governance.zone_change_audit
where run_id = current_setting('app.run_id', true)
order by changed_at;
