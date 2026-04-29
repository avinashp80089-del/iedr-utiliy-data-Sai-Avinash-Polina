# Data Dictionary

Column-level reference for every Delta table the pipeline produces. Audit columns are consistent across all layers — described once at the top.

## Audit columns (every table)

| Column           | Type      | Meaning |
|------------------|-----------|---------|
| `pipeline_run_id`| STRING    | UUID generated once per pipeline invocation. Use this to filter to "the run that wrote these rows." |
| `batch_date`     | DATE      | Logical delivery date of the source data. Multiple runs of the same `batch_date` overwrite each other (idempotent reprocessing). |
| `ingestion_ts`   | TIMESTAMP | Wall-clock time the row was written. Use for SLA monitoring. |

---

## Bronze layer — `iedr_<env>_bronze`

One table per `(utility_id, dataset)`. Schema reflects the raw CSV with two additions: `utility_id` and `source_file`.

### `utility{N}_circuits` / `utility{N}_install_der` / `utility{N}_planned_der`

| Column        | Type   | Meaning |
|---------------|--------|---------|
| *(raw cols)*  | STRING | Verbatim from the CSV. Header BOM stripped; types preserved as strings to absorb drift. |
| `utility_id`  | STRING | Logical utility identifier (`utility1`, `utility2`, ...) |
| `source_file` | STRING | Volume path of the CSV that produced this row |
| *audit*       | —      | See top of doc |

**Partition columns:** `(utility_id, batch_date)`. Reprocessing a single batch is a `replaceWhere` operation, not a full table rewrite.

---

## Silver layer — `iedr_<env>_silver`

### `dim_feeder`

One row per `(utility_id, feeder_id)`. SCD Type 1 (current state only). MERGE-upserted.

| Column                 | Type      | Meaning |
|------------------------|-----------|---------|
| `utility_id`           | STRING    | Source utility |
| `feeder_id`            | STRING    | Canonical feeder identifier; for U1 this is `NYHCPV_csv_NFEEDER` rolled up from segments and stripped of float `.0` suffix |
| `max_hosting_capacity` | DOUBLE    | MW. Feeder-level MAXHC (NOT segment-level NMAXHC) per case-prompt hint #2 |
| `min_hosting_capacity` | DOUBLE    | MW. Feeder-level MIN |
| `last_refresh_date`    | TIMESTAMP | When the utility last refreshed its hosting-capacity calculation |
| `voltage_kv`           | DOUBLE    | Nominal voltage in kV |
| *audit*                | —         | |

**Partition column:** `utility_id`.

### `fact_der`

One row per `(utility_id, der_id, status)`. MERGE-upserted on the composite key.

| Column                  | Type    | Meaning |
|-------------------------|---------|---------|
| `utility_id`            | STRING  | Source utility |
| `der_id`                | STRING  | Project / interconnection-request identifier. Source column varies (DER_ID for U2 installed; INTERCONNECTION_QUEUE_REQUEST_ID for U2 planned; ProjectID for U1) |
| `der_type`              | STRING  | Canonical type label: `Solar`, `Wind`, `Energy Storage`, `Hybrid`, `Unknown`, etc. |
| `capacity_kw`           | DOUBLE  | DER nameplate rating in kW |
| `status`                | STRING  | `installed` or `planned` (lifecycle state — NOT the source-dataset name) |
| `feeder_id`             | STRING  | Resolved canonical feeder; null when the utility's circuit ID could not be matched |
| `feeder_id_unresolved`  | BOOLEAN | True when feeder_id is null AND the source had a non-null circuit ID. Powers `data_quality` and `feeder_data_missing`. |
| *audit*                 | —       | |

**Partition column:** `utility_id`.

### `bridge_u1_circuit_to_feeder`

Utility-1-only intermediate. One row per `(circuit_id, feeder_id)` from the U1 circuits drop, distinct.

| Column       | Type   | Meaning |
|--------------|--------|---------|
| `circuit_id` | STRING | U1 segment-level Circuits_Phase3_CIRCUIT |
| `feeder_id`  | STRING | The feeder that segment belongs to (NYHCPV_csv_NFEEDER) |

Overwritten each run. No audit columns (it's purely derived).

---

## Platinum layer — `iedr_<env>_platinum`

### `feeder_capacity` (Req 2c)

Mirror of `dim_feeder` schema, optimized for the API filter. Same columns, same types.

**Partition column:** `utility_id`.
**Z-ORDER:** `max_hosting_capacity` (the predicate column for Req 2c).

### `feeder_der_details` (Req 2d)

`fact_der` LEFT-JOINed to `dim_feeder` on `(utility_id, feeder_id)`. Includes a synthesized data-quality flag.

| Column                | Type     | Meaning |
|-----------------------|----------|---------|
| *fact_der columns*    | —        | All columns from `fact_der` |
| `voltage_kv`          | DOUBLE   | From the joined dim_feeder; null when the feeder isn't in dim_feeder |
| `last_refresh_date`   | TIMESTAMP| From the joined dim_feeder |
| `feeder_data_missing` | BOOLEAN  | True when `voltage_kv` is null — i.e. the DER references a feeder we don't have circuit data for |

**Partition column:** `utility_id`.
**Z-ORDER:** `feeder_id` (the predicate column for Req 2d).

### `data_quality`

One row per `(utility_id, dataset, status)`.

| Column                       | Type      | Meaning |
|------------------------------|-----------|---------|
| `utility_id`                 | STRING    | |
| `dataset`                    | STRING    | `circuits` or `der` |
| `status`                     | STRING    | `installed`, `planned`, or null (for circuits) |
| `total_rows`                 | LONG      | Row count for this slice |
| `null_feeder_id_count`       | LONG      | DERs where the utility supplied no circuit ID at all |
| `unresolved_feeder_id_count` | LONG      | DERs where the supplied circuit ID couldn't be matched to dim_feeder |
| `last_refresh_date`          | TIMESTAMP | Most recent refresh date in the source data (circuits dataset only) |
| *audit*                      | —         | |
