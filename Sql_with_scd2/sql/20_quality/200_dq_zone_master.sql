/* ------------------------------------------------------------
Author: Sumanth
Owner: Data Quality
Purpose: Compute DQ results for zone master feed
------------------------------------------------------------ */

-- Rule 1: business key not null
insert into governance.dq_results(run_id, rule_name, passed, fail_count, sample)
select
  current_setting('app.run_id', true),
  'ZONE_BK_NOT_NULL',
  (count(*) = 0) as passed,
  count(*) as fail_count,
  jsonb_build_object('sample_location_id', min(location_id))
from staging.v_zone_candidate
where location_id is null;

-- Rule 2: uniqueness of business key within incoming run
insert into governance.dq_results(run_id, rule_name, passed, fail_count, sample)
select
  current_setting('app.run_id', true),
  'ZONE_BK_UNIQUE_IN_RUN',
  (count(*) = 0) as passed,
  count(*) as fail_count,
  jsonb_build_object('sample_location_id', min(location_id))
from (
  select location_id
  from staging.v_zone_candidate
  group by location_id
  having count(*) > 1
) d;

-- Rule 3: required attributes not null
insert into governance.dq_results(run_id, rule_name, passed, fail_count, sample)
select
  current_setting('app.run_id', true),
  'ZONE_ATTRS_NOT_NULL',
  (count(*) = 0) as passed,
  count(*) as fail_count,
  jsonb_build_object('sample_location_id', min(location_id))
from staging.v_zone_candidate
where borough is null or zone_name is null or service_zone is null;
