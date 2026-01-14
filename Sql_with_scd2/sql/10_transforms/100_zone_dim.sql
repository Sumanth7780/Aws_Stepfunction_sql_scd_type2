/* ------------------------------------------------------------
Author: Sumanth
Owner: Data Engineering
Purpose: Candidate dataset for SCD2 apply (with stable hashdiff)
Dependencies: staging.stg_zone_master_incoming, pgcrypto extension
------------------------------------------------------------ */

create or replace view staging.v_zone_candidate as
select
  location_id,
  trim(borough) as borough,
  trim(zone_name) as zone_name,
  trim(service_zone) as service_zone,
  encode(
    digest(
      (coalesce(trim(borough),'') || '||' || coalesce(trim(zone_name),'') || '||' || coalesce(trim(service_zone),''))::text,
      'sha256'::text
    ),
    'hex'
  ) as hashdiff,
  run_id
from staging.stg_zone_master_incoming
where run_id = current_setting('app.run_id', true);
