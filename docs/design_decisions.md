# Design Decisions

This document captures the deliberate trade-offs made in this codebase. For each, alternatives were considered — the chosen path is recorded with rationale so future maintainers (or interview reviewers) can see the reasoning, not just the result.

---

## 1. SCD Type 1 for `dim_feeder` — not Type 2

**Decision:** Upsert in place. New monthly drops overwrite a feeder's `max_hosting_capacity` and `voltage_kv`.

**Why:** The two API queries (Req 2c and 2d) both want the *current* state of the grid. Hosting capacity changes when utilities add or upgrade equipment — historical capacity is operationally interesting but not what the IEDR application asks for today.

**Migration path to Type 2 (when the application needs history):**
- Add `effective_from`, `effective_to`, `is_current` columns
- Change MERGE to close out the old row (`effective_to = batch_date`) and insert a new one
- All audit data is already in place because we stamp `pipeline_run_id` and `batch_date` on every row

The `add_audit_columns()` helper means this migration is local — no cascading changes elsewhere.

---

## 2. Batch ingest — not streaming (yet)

**Decision:** `spark.read.csv` in `bronze/ingest.py`. Auto Loader recipe is provided as a comment block at the bottom of the same file.

**Why:** The case-prompt cadence is monthly. Streaming infrastructure (checkpoints, schema-evolution metadata, dead-letter handling) has real operational cost and is overkill when the actual arrival pattern is a known scheduled drop.

**When to flip the switch:** When utilities start publishing files at unpredictable times, or when one or more utilities exceed the size where a monthly batch run would breach the SLA. The swap is mechanical — replace the read block, change `mode("append")` to `writeStream`, add a checkpoint location. Downstream silver/platinum code is identical.

---

## 3. In-house expectations — not Great Expectations or DLT

**Decision:** A 60-line `iedr/common/expectations.py` with `Expectation(name, predicate, severity)`.

**Why:**
- **Great Expectations** is a mature library, but adds a heavy dependency, JSON-schema-driven config, and a separate test runner. For 6 expectations across 3 tables, the cost-to-value ratio is poor.
- **Delta Live Tables (DLT)** would be a great fit *if* we committed to DLT's pipeline model. We didn't — the `@dlt.table` programming model is incompatible with the simpler ingest-and-MERGE pattern this codebase uses, and DLT pipelines have their own deployment lifecycle that complicates the DAB.

**Migration path to DLT:** the `Expectation` class deliberately mirrors DLT's `@dlt.expect_or_drop` semantics. If we migrate the silver layer to DLT, the *predicates* are reusable verbatim — only the wrapping changes.

---

## 4. Workspace isolation: structure now, separation later

**Decision:** `databricks.yml` parameterizes `workspace_host` per target. All three targets currently default to the same Databricks workspace.

**Why:** A real F500 deployment runs dev/qa/prod in *separate* Databricks workspaces with separate IAM, separate Unity Catalogs, and ideally separate cloud accounts. That isolation is what protects prod from a junior engineer's notebook bug. The take-home repo only has one workspace available, so all three targets share it — but the YAML structure is built to swap them out (`prod.workspace.host = ${var.workspace_host_prod}`) the moment additional workspaces are provisioned.

**The cost of pretending now:** dev and qa runs are visibly prefixed with the deployer's username (Databricks' "development mode"). Prod is locked under `/Workspace/Shared/.bundle/...` and runs as production-mode (no dev-only features, no per-user prefix). Schema names also include the env (`iedr_dev_*` vs `iedr_prod_*`), giving us logical isolation even within the shared workspace.

---

## 5. Surrogate-key choice: `(utility_id, feeder_id)` and `(utility_id, der_id, status)`

**Decision:** Composite natural keys, no monotonically_increasing_id surrogates.

**Why:**
- Different utilities can (and do) use overlapping ID spaces. `utility_id` is essential.
- For `fact_der`, the same `der_id` can appear in both `installed` and `planned` drops if a project moves through the queue and into operation. `status` distinguishes the two record-types and is part of the natural key.
- Surrogate keys add a join hop and a sequence-generation step that we don't need — the natural composite is selective enough for partition pruning and cheap to compute.

---

## 6. Feeder ID resolution: bridge first, direct match fallback

**Decision:** For Utility 1, DER `ProjectCircuitID` is checked against the bridge table first (where it's a circuit-segment ID), then against `dim_feeder.feeder_id` directly (where the utility supplied a feeder ID). DERs that match neither get `feeder_id_unresolved=True` and `feeder_id=null`.

**Why:** The hint in the case prompt warned this is messy: "Most ProjectCircuitID values from utility 1's installed and planned DER datasets ... can be matched against the bridge ... but some can be matched directly against feeder ID." The defensive approach surfaces the mismatch rate to the application via `data_quality.unresolved_feeder_id_count` rather than swallowing it.

---

## 7. Why pyproject.toml — not requirements.txt

**Decision:** All Python dependencies declared in `pyproject.toml`, including the `[dev]` and `[serving]` extras.

**Why:** Single source of truth. The CI workflow installs `.[dev]`, the serving image installs `.[serving]`, and engineers install `.[dev]` locally. No risk of `requirements.txt`, `requirements-dev.txt`, and `setup.py` drifting out of sync — a common F500 pain point.

---

## 8. Why Unity Catalog lineage is deferred

**Decision:** Audit columns on every table; UC column-level lineage left for a follow-up.

**Why:** UC lineage requires the workspace to have UC enabled, every table to be UC-managed (not Hive metastore), and the cluster to be on a UC-compatible runtime. Some of these are workspace-admin decisions outside this repo's scope. The audit columns we DO have answer 90% of practical lineage questions ("which run wrote this?", "what was the source file?") without that infrastructure investment. When UC is fully rolled out, our tables become UC-managed automatically and we get column-level lineage for free.
