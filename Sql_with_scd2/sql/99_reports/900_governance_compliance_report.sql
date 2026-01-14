/* ------------------------------------------------------------
Author: Sumanth
Owner: Governance
Purpose: Compliance report: DQ + approvals + SCD2 activity
------------------------------------------------------------ */

with dq as (
  select
    run_id,
    sum(case when passed then 0 else 1 end) as failed_rules,
    count(*) as total_rules
  from governance.dq_results
  group by run_id
),
changes as (
  select
    run_id,
    sum(case when action_type like '%INSERT%' then 1 else 0 end) as inserts,
    sum(case when action_type like '%EXPIRE%' then 1 else 0 end) as expires,
    sum(case when action_type = 'NOCHANGE' then 1 else 0 end) as nochange
  from governance.zone_change_audit
  group by run_id
)
select
  p.run_id,
  p.job_name,
  p.trigger_type,
  p.data_owner,
  p.status,
  p.started_at,
  p.ended_at,

  coalesce(dq.total_rules, 0) as dq_rules_executed,
  coalesce(dq.failed_rules, 0) as dq_rules_failed,

  vc.status as mdm_version_status,
  vc.approved_at,
  vc.approved_by,

  coalesce(c.inserts, 0) as scd_inserts,
  coalesce(c.expires, 0) as scd_expires,
  coalesce(c.nochange, 0) as scd_nochange
from governance.pipeline_run_audit p
left join dq on dq.run_id = p.run_id
left join governance.mdm_version_control vc on vc.run_id = p.run_id and vc.entity_name='ZONE'
left join changes c on c.run_id = p.run_id
order by p.started_at desc;
