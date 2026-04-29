# Architecture

## Goal

Build a scalable, harmonized data platform to ingest, clean, and serve utility circuit and DER data from multiple New York utilities — powering the IEDR SaaS application.

## Two platinum API queries (case-prompt requirements 2c and 2d)

| Requirement | SQL                                                                                       | Optimization                            |
|-------------|-------------------------------------------------------------------------------------------|-----------------------------------------|
| 2c          | `SELECT * FROM platinum.feeder_capacity WHERE max_hosting_capacity > X`                   | Partition `utility_id`, Z-ORDER `max_hosting_capacity` |
| 2d          | `SELECT * FROM platinum.feeder_der_details WHERE feeder_id = X AND utility_id = Y`        | Partition `utility_id`, Z-ORDER `feeder_id`            |

## Data flow

```mermaid
graph TD
    subgraph "Per-utility bronze (append-only)"
        B[utility{N}_circuits<br/>utility{N}_install_der<br/>utility{N}_planned_der<br/>partition: (utility_id, batch_date)]
    end

    subgraph "Silver — canonical model"
        DF[dim_feeder<br/>partition: utility_id<br/>SCD Type 1 MERGE]
        FD[fact_der<br/>partition: utility_id<br/>MERGE on utility_id, der_id, status]
        BR[bridge_u1_circuit_to_feeder<br/>U1-only segment→feeder lookup]
    end

    subgraph "Platinum — API-facing"
        FC[feeder_capacity<br/>partition: utility_id<br/>Z-ORDER: max_hosting_capacity]
        FDD[feeder_der_details<br/>partition: utility_id<br/>Z-ORDER: feeder_id]
        DQ[data_quality<br/>per-utility, per-status metrics]
    end

    B --> BR
    B --> DF
    B --> FD

    DF --> FC
    DF --> FDD
    FD --> FDD
    DF --> DQ
    FD --> DQ
```

## Scalability

| Concern                     | Approach |
|-----------------------------|----------|
| **Volume**                  | Delta columnar storage; `partition by utility_id`; Z-ORDER on the actual API filter columns; `optimizeWrite` and `autoCompact` enabled on the job cluster. |
| **Variety (8+ utilities)**  | One `UtilityAdapter` subclass per utility + one YAML config per utility. The orchestrator does not know how individual utilities differ — it just iterates. |
| **Velocity**                | The `_ingest_one` function in `bronze/ingest.py` ships an Auto Loader (`cloudFiles`) recipe ready to swap in. `batch_date` partitioning means streaming and batch can coexist on the same table. |
| **Future scale**            | Liquid Clustering can replace Z-ORDER on platinum once individual partitions exceed file-size targets. |
| **Cost**                    | Photon-enabled job cluster; `availableNow` trigger for monthly cadence (no idle streaming compute). |

## Environment isolation

Same notebooks, same code — different schema prefixes driven by the `env` widget:

| Env  | Schemas                                                                       |
|------|-------------------------------------------------------------------------------|
| dev  | `iedr_dev_bronze`, `iedr_dev_silver`, `iedr_dev_platinum`                     |
| qa   | `iedr_qa_bronze`, `iedr_qa_silver`, `iedr_qa_platinum`                        |
| prod | `iedr_prod_bronze`, `iedr_prod_silver`, `iedr_prod_platinum`                  |

`databricks.yml` parameterizes `workspace_host` so dev/qa/prod can point to separate workspaces — the take-home repo uses one workspace, but the YAML structure supports the F500 multi-workspace setup (see [design_decisions.md](design_decisions.md#workspace-isolation)).

## Observability

Three layers of visibility:

1. **Audit columns on every row** — `pipeline_run_id`, `batch_date`, `ingestion_ts`. Surgical reprocessing ("re-run only the data that came in last Tuesday") is a SQL filter away.
2. **`platinum.data_quality` table** — per-utility / per-status row counts, null counts, unresolved-feeder-ID counts, last refresh date. The IEDR application can query this directly.
3. **Expectations** — DLT-style assertions (`apply_expectations`) on every silver build. Failed `severity=fail` checks halt the pipeline; `severity=warn` checks log without blocking.

## Missing-data transparency

Three categories surfaced to the application (per the case-prompt: "highlight where there is missing data received from the utilities"):

| Category                       | Where                            | Meaning                                                                 |
|--------------------------------|----------------------------------|-------------------------------------------------------------------------|
| `feeder_data_missing`          | `platinum.feeder_der_details`    | DER record exists but no matching feeder in `dim_feeder`                |
| `null_feeder_id_count`         | `platinum.data_quality`          | Utility provided no circuit ID for the DER                              |
| `unresolved_feeder_id_count`   | `platinum.data_quality`          | Utility provided an ID that maps to no known circuit (e.g. alphanumeric internal codes, IDs from outside the loaded dataset) |

## Data lineage

Every Delta table has standard audit columns:

- `pipeline_run_id` — UUID, generated once per pipeline invocation
- `batch_date` — logical date of the data drop
- `ingestion_ts` — wall-clock timestamp of when the row was written
- `source_file` (bronze only) — origin path

These five columns answer 90% of "what changed when" questions without reaching for Unity Catalog lineage. UC column-level lineage is a future enhancement (noted in `design_decisions.md`).
