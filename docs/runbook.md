# Runbook

What to do when the IEDR pipeline fails. Each scenario has the symptom you'll see in alerts, the diagnostic command, and the recovery action.

## On-call escalation

1. **First responder** — check this runbook
2. **L2** — `data-eng-oncall@example.com` (configured in `resources/iedr_pipeline.yml`)
3. **L3** — pipeline owner (data engineering tech lead)

---

## Scenario 1: bronze fails — "Path does not exist"

**Symptom:** Job fails on `bronze_ingest`. Error contains `AnalysisException: Path does not exist`.

**Diagnosis:**
```python
%fs ls /Volumes/<catalog>/default/iedr_data/
# Confirms whether the expected file actually arrived
```

**Causes & fixes:**

| Cause | Action |
|-------|--------|
| Utility delivery delayed (most common) | Notify the IEDR business team. Re-run when the file lands. **Do NOT** populate placeholder data. |
| File pattern changed (utility renamed file) | Update `file_pattern` in the relevant `config/utilities/<id>.yaml`. PR + redeploy. |
| Volume permissions revoked | Check the cluster's service principal has `READ VOLUME` on the volume. |

---

## Scenario 2: silver expectation fails — "der_id_not_null"

**Symptom:** Job fails in `silver_transform` with `AssertionError: [fact_der ...] der_id_not_null: N/M rows failed`.

**Diagnosis:**
```sql
SELECT * FROM iedr_<env>_bronze.utility{N}_install_der
WHERE batch_date = '<batch_date>' AND ProjectID IS NULL;
```

**Action:**
1. **Triage volume:** how many rows? <1% of total → tolerable; >5% → likely a source-format change.
2. **If the utility's CSV format changed**, update `config/utilities/<id>.yaml` and add a regression test. Don't loosen the expectation to silence the alert.
3. **If a small number of bad rows in an otherwise-correct file**, you can manually drop them by re-running with the bad rows pre-filtered, or temporarily switch the expectation severity to `drop` for one batch (revert before next deploy).

---

## Scenario 3: schema drift — "Column not found"

**Symptom:** `AnalysisException: Column 'NYHCPV_csv_NFEEDER' does not exist`.

**Diagnosis:**
```python
spark.read.csv(file_path, header=True).columns
# Compare to what config/utilities/<id>.yaml expects
```

**Action:**
1. Confirm with the utility whether the column rename is intentional and permanent.
2. Update `config/utilities/<id>.yaml` mapping.
3. Add an integration-test fixture that reproduces the new shape so this can't regress silently.

---

## Scenario 4: MERGE conflict / duplicate row

**Symptom:** Duplicate `(utility_id, der_id, status)` keys appearing in `fact_der`.

**Diagnosis:**
```sql
SELECT utility_id, der_id, status, COUNT(*) FROM iedr_<env>_silver.fact_der
GROUP BY 1,2,3 HAVING COUNT(*) > 1;
```

This should never happen — `fact_der` has a `dropDuplicates` and a MERGE on the composite key.

**Likely cause:** A prior pipeline failed mid-MERGE leaving the table in an inconsistent state.

**Recovery:**
```python
from delta.tables import DeltaTable
dt = DeltaTable.forName(spark, f"iedr_{env}_silver.fact_der")
dt.history(20).show()                  # find the last good version
spark.sql(f"RESTORE TABLE iedr_{env}_silver.fact_der TO VERSION AS OF <version>")
```

Then re-run silver for the affected `batch_date`:
```python
dbutils.notebook.run("/Repos/.../silver/silver_transformation",
                     timeout_seconds=1800,
                     arguments={"env": env, "batch_date": "<batch_date>"})
```

---

## Scenario 5: a back-fill is needed

**Use case:** Utility 2 corrected their March data and resent the file in April.

**Action:**
1. Drop the corrected CSV at the volume path.
2. Run bronze, silver, and platinum with `batch_date = "2026-03-01"` (the original drop date, not today):
   ```
   databricks bundle run iedr_pipeline -t prod -- --batch_date 2026-03-01
   ```
3. The bronze ingest's `replaceWhere` clause overwrites only the `2026-03-01` partition — other batches are untouched.
4. Silver MERGE re-upserts with the corrected values; platinum rebuilds.

---

## Scenario 6: pipeline is stuck / running too long

**Threshold:** Each task has `timeout_seconds: 1800` (30 minutes). If a task hits the timeout, it auto-retries up to `max_retries: 2`.

**Investigate:**
1. Check Spark UI for the running cluster. Look for skewed partitions or shuffle spills.
2. If volume has grown significantly, scale `num_workers` in `resources/iedr_pipeline.yml` and redeploy.
3. For repeated timeouts on the same task, check for runaway joins on a utility's data — a Cartesian product can sneak in if a config change accidentally drops a join key.

---

## Useful queries

**What did the last run write?**
```sql
SELECT pipeline_run_id, batch_date, MAX(ingestion_ts), COUNT(*)
FROM iedr_<env>_silver.fact_der
GROUP BY 1, 2
ORDER BY 3 DESC
LIMIT 10;
```

**Per-utility quality snapshot:**
```sql
SELECT * FROM iedr_<env>_platinum.data_quality
ORDER BY ingestion_ts DESC, utility_id, status;
```

**Time-travel a table to debug a regression:**
```sql
DESCRIBE HISTORY iedr_<env>_silver.dim_feeder;
SELECT * FROM iedr_<env>_silver.dim_feeder VERSION AS OF 42;
```
