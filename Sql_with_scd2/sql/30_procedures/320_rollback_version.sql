/* ------------------------------------------------------------
Author: Sumanth
Owner: Governance
Purpose: Roll back changes introduced by a run_id
Strategy:
 - Expire any current rows created by that run_id
 - Re-activate the most recent prior row per business key (if exists)
Audit: logs rollback actions
------------------------------------------------------------ */

do $$
declare
  rid text := current_setting('app.run_id', true);
begin
  -- Expire current rows created by this run
  update mdm.zone_dim_scd2
     set is_current=false,
         effective_to=now()
   where source_run_id=rid
     and is_current=true;

  -- Re-activate the latest historical record per BK that existed before this run
  with prior as (
    select distinct on (location_id)
      zone_sk, location_id
    from mdm.zone_dim_scd2
    where (source_run_id is null or source_run_id <> rid)
    order by location_id, effective_from desc
  )
  update mdm.zone_dim_scd2 d
     set is_current=true,
         effective_to=null
  from prior
  where d.zone_sk = prior.zone_sk;

  update governance.mdm_version_control
     set status='ROLLED_BACK',
         reason='Rollback executed'
   where entity_name='ZONE'
     and run_id=rid;

  insert into governance.zone_change_audit(run_id, action_type, details)
  values (rid, 'ROLLBACK', jsonb_build_object('status','rolled_back'));
end $$;
