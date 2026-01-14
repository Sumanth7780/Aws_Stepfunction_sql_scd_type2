/* ------------------------------------------------------------
Author: Sumanth
Owner: Stewardship
Purpose: Approve the proposed version for the run_id (steward action)
------------------------------------------------------------ */

do $$
declare
  approver text := coalesce(current_user, 'steward');
begin
  -- Approve the version control record
  update governance.mdm_version_control
     set status='APPROVED',
         approved_at=now(),
         approved_by=approver,
         reason='Steward approved run'
   where entity_name='ZONE'
     and run_id=current_setting('app.run_id', true);

  -- Mark dimension records from this run as approved
  update mdm.zone_dim_scd2
     set approval_status='APPROVED',
         approved_at=now(),
         approved_by=approver
   where source_run_id=current_setting('app.run_id', true)
     and approval_status='PROPOSED';
end $$;
