/* ------------------------------------------------------------
Author: Sumanth
Owner: Data Quality
Purpose: Fail the pipeline if any DQ rule failed
------------------------------------------------------------ */

do $$
declare
  failed_rules int;
begin
  select count(*)
  into failed_rules
  from governance.dq_results
  where run_id = current_setting('app.run_id', true)
    and passed = false;

  if failed_rules > 0 then
    raise exception 'DQ_ASSERTION_FAILED: % rules failed for run_id=%',
      failed_rules, current_setting('app.run_id', true);
  end if;
end $$;
