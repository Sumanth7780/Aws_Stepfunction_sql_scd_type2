# Week 3 — Orchestration, Transformation & Data Warehousing Foundations (Governance-First)

**Dataset:** NYC Yellow Taxi Trips  
**Platform:** AWS S3 + AWS Glue (Spark) + Step Functions + CloudWatch + (optional) RDS/Redshift  
**Goal:** Build production-style orchestration with governance gates, implement SQL transformations with tests, and implement SCD Type 2 + master data versioning.

---

## What I Learned (Week 3 Summary)

### 1) Orchestration & Monitoring (Day 11)
- Designed an end-to-end pipeline using **AWS Step Functions** to orchestrate Glue jobs (validated → curated → master).
- Added governance checkpoints:
  - **Data Quality Gate:** fail pipeline if pass rate drops below threshold.
  - **Master Data Freshness Check:** warn if reference/master data is stale.
  - **Audit Trail Logging:** captured who/what/when + paths processed + results of quality checks.
- Implemented operational monitoring:
  - CloudWatch logs for every step
  - alarms for failures/retries
  - run history and traceability

**Outcome:** A repeatable pipeline run produces curated/master outputs with traceable quality + lineage.
### Day 11 — Orchestration & Monitoring 
### Glue / Spark jobs used as pipeline steps
- Day 6 (raw → validated Delta): [`glue_jobs/day6_datalake_delta_raw_to_validated_glue.py`](step_function/day6_datalake_delta_raw_to_validated_glue.py)
- Day 7 (validated → curated enrichment): [`glue_jobs/day7_spark_enrich_and_catalog_prep.py`](step_function/day7_spark_enrich_and_catalog_prep.py)
- Day 8 (quality gates + quarantine + report): [`glue_jobs/day8_glue_quality_gates_validated_to_curated.py`](step_function/day8_glue_quality_gates_validated_to_curated.py)
- Day 9 (MDM matching/dedup + steward queue): [`glue_jobs/day9_mdm_matching_dedup_engine.py`](step_function/day9_mdm_matching_dedup_engine.py)
- Day 10 (lifecycle + audit + orphan detection): [`glue_jobs/day10_mdm_lifecycle_audit_orphans.py`](step_function/day10_mdm_lifecycle_audit_orphans.py)


> Note: Week 3 orchestration (Day 11) calls these jobs in sequence with pass/fail governance gates.

--- 

### Orchestration (Step Functions)
- State machine graph: ![Step Functions Graph](pics/day11_stepfunctions_graph.png)
- Successful execution: ![Execution Success](pics/day11_stepfunctions_graph_Sucess.png)
### Triggers (EventBridge)
- Schedule / S3 trigger rule: ![EventBridge Rule](pics/day11_eventbridge.png)

### Processing (Glue)
- Glue job runs: ![Glue Runs](pics/day11_glue_job_runs.png)


### Outputs (S3)
- Curated output: ![S3 Curated](pics/day11_s3_curated.png)
- Orphans quarantine: ![S3 Orphans](pics/day11_s3_orphan.png)
- Audit outputs: ![S3 Audit](pics/day11_s3_audit.png)
### Audit Table (Dynamodb)
-Audit Table: ![audit table](pics/dynomodb_audit_table_view.png)
### Notifications (SNS)
- SNS topic/subscription: ![SNS Topic](pics/day11_sns.png)
---
---

### 2) Advanced SQL Transformations + Governance as Code (Day 12–13)
- Built modular SQL transformations using:
  - CTEs for readability + reuse
  - staging → fact/dim layers
  - reusable validation queries
- Implemented SQL “unit tests”:
  - row count expectations
  - not-null checks
  - referential integrity checks against master/zone dimensions
- Added governance metadata in SQL headers:
  - author, owner, purpose, dependencies, quality expectations
- Prepared SQL for scheduled execution in Glue/Athena/Redshift context.

**Outcome:** SQL logic is version-controlled, testable, and auditable.

---

### 3) Slowly Changing Dimensions (SCD Type 2) + Master Data Versioning (Day 14–15)
- Implemented SCD Type 2 patterns for dimensions (example: taxi zones / vendors):
  - surrogate key
  - `effective_from`, `effective_to`
  - `is_current`
  - audit columns (`created_at`, `updated_at`, `approved_by`, `change_reason`)
- Wrote point-in-time queries for compliance:
  - “as-of” joins between fact trips and dimension version that was valid at trip time.
- Designed rollback strategy leveraging Delta history / versioned records.

**Outcome:** Historical truth is preserved for reporting, auditing, and dispute resolution.

---

## Key Concepts Demonstrated (Mapping to Dataset)

From TLC schema:
- Trip timestamps: `tpep_pickup_datetime`, `tpep_dropoff_datetime`
- Location keys: `PULocationID`, `DOLocationID`
- Financial fields: `fare_amount`, `total_amount`, `tip_amount`, etc.

These drive:
- **Duration / sanity checks** (pickup <= dropoff)
- **Non-negative amounts checks**
- **Referential integrity** to master zones dimension

---
## Architecture
S3 (Raw Reference Data)
        ↓
AWS Glue (Spark + JDBC)
        ↓
PostgreSQL (Staging Tables)
        ↓
SQL Transforms + Quality Gates
        ↓
SCD Type 2 Dimension (MDM)
        ↓
Audit & Governance Tables


## SQL Linked (Transformations + Quality + Tests)

### Transformations
- Stage trips: [`sql/transformations/01_stg_trips.sql`](Sql_with_scd2/sql/10_transforms/)
- Fact trips: [`sql/transformations/02_fact_trips.sql`](../sql/transformations/02_fact_trips.sql)
- Zone dimension SCD2: [`sql/transformations/03_dim_zone_scd2.sql`](../sql/transformations/03_dim_zone_scd2.sql)

### Data Quality Queries
- Required fields checks: [`sql/quality/dq_required_fields.sql`](../sql/quality/dq_required_fields.sql)
- Referential integrity (PU/DO must exist in master zones): [`sql/quality/dq_referential_integrity.sql`](../sql/quality/dq_referential_integrity.sql)
- Amount sanity checks: [`sql/quality/dq_amount_sanity.sql`](../sql/quality/dq_amount_sanity.sql)

### SQL Tests
- Row count checks: [`sql/tests/test_row_counts.sql`](../sql/tests/test_row_counts.sql)
- DQ threshold checks (pass rate): [`sql/tests/test_dq_thresholds.sql`](../sql/tests/test_dq_thresholds.sql)

---

## Screenshots Checklist (What to capture)



### Day 12–13 — Advanced SQL + Governance (Must-have)
6. **SQL script header showing governance metadata**
   - author/owner/purpose/dependencies/quality  
   - Save as: `docs/images/day12_sql_headers.png`

7. **DQ query results**
   - result table with counts and pass/fail  
   - Save as: `docs/images/day12_dq_sql_results.png`

8. **Glue/Athena/Redshift run proof**
   - job run page OR query execution history  
   - Save as: `docs/images/day13_glue_sql_job_run.png`

---

### Day 14–15 — SCD2 + Versioning (Must-have)
9. **Dimension table schema (SCD2 columns visible)**
   - `effective_from`, `effective_to`, `is_current`, surrogate key  
   - Save as: `docs/images/day14_scd2_table_schema.png`

10. **SCD2 history example**
   - same natural key with 2+ versions (old closed, new current)  
   - Save as: `docs/images/day14_scd2_history_example.png`

11. **Point-in-time query output**
   - “as-of join” returns correct historical dimension row  
   - Save as: `docs/images/day15_point_in_time_query.png`

12. **Rollback / time travel evidence**
   - Delta history or restored version output  
   - Save as: `docs/images/day15_rollback_demo.png`

---

## Embed Screenshots in this README

### Orchestration (Day 11)
![Step Functions Graph](./images/day11_stepfunctions_graph.png)
![EventBridge Rule](./images/day11_eventbridge_rule.png)
![CloudWatch Logs](./images/day11_cloudwatch_logs.png)
![CloudWatch Alarm](./images/day11_cloudwatch_alarm.png)
![Audit Trail Items](./images/day11_audit_table_items.png)

### SQL Governance + Tests (Day 12–13)
![SQL Headers](./images/day12_sql_headers.png)
![DQ SQL Results](./images/day12_dq_sql_results.png)
![Glue SQL Run](./images/day13_glue_sql_job_run.png)

### SCD2 + Versioning (Day 14–15)
![SCD2 Schema](./images/day14_scd2_table_schema.png)
![SCD2 History](./images/day14_scd2_history_example.png)
![Point-in-time Query](./images/day15_point_in_time_query.png)
![Rollback Demo](./images/day15_rollback_demo.png)

---

## Week 3 Deliverables (Checklist)

- [ ] Step Functions pipeline orchestrates Glue jobs end-to-end
- [ ] Quality gate blocks pipeline if DQ below threshold
- [ ] Audit trail captures run_id + inputs + outputs + DQ results
- [ ] SQL scripts include governance headers and are version-controlled
- [ ] SQL tests exist and can be executed on demand
- [ ] SCD Type 2 dimension implemented with point-in-time queries
- [ ] Rollback strategy documented (Delta history / version rollback)

---

## Notes / Next Improvements
- Add SNS notifications for steward review queue / failed DQ thresholds
- Add data freshness SLA checks for master zone updates
- Publish “certified datasets” to Glue Catalog with owner + classification tags
