# Spark Streaming Variant — Build Summary

## What Was Built

A fifth CDC pipeline variant (`spark_streaming/`) that replaces Apache Flink with PySpark Structured Streaming. Everything else — Debezium, Kafka, the staging/output table schema, the PL/pgSQL merge function, and the Python LISTEN/NOTIFY listener — is identical to the `pg_transaction` variant.

## Pipeline Flow

```
PostgreSQL (JSONB) → Debezium → Kafka → Spark Structured Streaming → stg_* tables → PG NOTIFY → merge_cdc_batch() → output_* tables
```

## Files Created (15 total)

### New files (3)

- **`Dockerfile.spark`** — `bitnami/spark:3.5` image with Kafka connector (`spark-sql-kafka`), Kafka clients, token provider, commons-pool2, and PostgreSQL JDBC driver baked in via `curl` (no Maven downloads at runtime).

- **`spark-app/cdc_streaming.py`** — The core PySpark job. Reads Debezium CDC events from Kafka, parses the envelope schema with `from_json()`, resolves `coalesce(after, before)` for delete handling, then uses `foreachBatch` to write all 5 staging tables per micro-batch:
  - `stg_policy` — scalar fields via `get_json_object()`
  - `stg_coverage` — `explode_outer(from_json($.coverages))`
  - `stg_vehicle` — `explode_outer(from_json($.vehicles))`
  - `stg_driver` — double explode: vehicles → drivers
  - `stg_claim` — `explode_outer(from_json($.claims_history))`

- **`README.md`** — Full documentation with architecture diagram, startup sequence, service endpoints, troubleshooting, and comparison table vs the Flink variant.

### Adapted files (3)

- **`docker-compose.yml`** — Removed `flink-jobmanager` + `flink-taskmanager` (2 services). Added single `spark` service running `spark-submit --master local[2]`. Container prefix changed from `pgtxn-` to `spark-`. All other services (postgres-source, postgres-cdc, zookeeper, kafka, kafka-connect, merge-listener, pgadmin, setup) are the same.

- **`scripts/setup.sh`** — Identical Debezium registration logic, updated log messages to reference Spark instead of Flink.

- **`verify-pipeline.sh`** — Replaced Flink health checks (Step 4: JobManager API, TaskManager count, running jobs) with Spark checks (container running, streaming query active in logs). All other steps (seed data, Debezium, Kafka topic, staging tables, merge-listener, output tables, live INSERT/UPDATE/DELETE tests with timing) are unchanged.

### Copied unchanged from `pg_transaction/` (9)

- `postgres-source/01-init.sql`, `02-seed-data.sql`, `postgresql.conf`
- `postgres-cdc/01-init.sql` (staging tables, output tables, merge function, NOTIFY triggers)
- `debezium/register-connector.json`
- `scripts/event-trigger-merge.py`
- `pgadmin/servers.json`, `pgadmin/pgpass`
- `Dockerfile.merge`

### Root README updated

- Added `spark_streaming/` as 5th variant in the description and quick start sections
- Extended comparison table with a `spark_streaming` column
- Updated "four variants" → "five variants" throughout

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Container topology | Single `spark-submit` container | PoC simplicity — no master/worker split needed |
| Batch strategy | `foreachBatch` callback | All 5 staging writes from one micro-batch in a single function |
| Trigger interval | `processingTime="5 seconds"` | Comparable latency to Flink variant (~8-12s end-to-end) |
| Array flattening | `explode()` + `from_json()` | Handles any array length — replaces Flink's UNION ALL with hardcoded indices |
| Scalar field access | `get_json_object()` | Simpler than defining full nested schema for scalar extractions |
| Docker image | `apache/spark-py:latest` (Spark 3.4.0) | `bitnami/spark` tags unavailable; official Apache image works reliably |
| JARs | Baked into Docker image | Avoids Maven downloads at container startup |
| Checkpointing | `/tmp/spark-checkpoints` inside container | Sufficient for PoC — no persistent volume needed |

## Test Results

### Verification script (`verify-pipeline.sh`)

**45 passed, 0 failed, 1 warning.** Pipeline fully operational.

| Test | Result |
|------|--------|
| All 8 services running | PASS |
| Setup exited cleanly | PASS |
| 4 seed policies in source | PASS |
| Debezium connector + task RUNNING | PASS |
| Kafka topic exists with 4 messages | PASS |
| Spark streaming query active | PASS |
| All 5 staging tables populated | PASS |
| Merge listener active, merge completed | PASS |
| All 5 output tables populated | PASS |
| Live INSERT (end-to-end) | PASS — **11,945ms** (5.9s to staging, 6.0s to merge) |
| Live UPDATE (add vehicle + coverage) | PASS — **11,549ms** (5.7s to staging, 5.8s to merge) |
| Live DELETE (remove policy atomically) | PASS — **18,182ms** (12.6s to staging, 5.6s to merge) |
| PGAdmin reachable | PASS |

### Bulk insert test — 50 policies

Inserted 50 randomized policies (each with 1-3 coverages, 1-2 vehicles with nested drivers, 0-3 claims) in a single SQL statement and tracked propagation through the full pipeline.

**End-to-end: 9,498ms** (~9.5 seconds) for all 50 policies to appear in output tables.

| Table | Before | After | Delta |
|-------|--------|-------|-------|
| output_policy | 4 | 54 | +50 |
| output_coverage | 8 | 99 | +91 |
| output_vehicle | 6 | 83 | +77 |
| output_driver | 8 | 130 | +122 |
| output_claim | 6 | 82 | +76 |

**Total rows created across all output tables: 416** (from 50 source records)

Timing breakdown:
- ~2s: INSERT into source PostgreSQL
- ~5s: Spark micro-batch picks up from Kafka, flattens JSONB, writes to staging tables
- ~2s: PG NOTIFY debounce + `merge_cdc_batch()` merges all 5 tables atomically

## Biggest Improvement Over Flink Variant

The Flink SQL job uses ~360 lines of verbose UNION ALL statements with hardcoded array indices (`$.coverages[0]`, `$.coverages[1]`, ... up to `[4]`). The PySpark job handles the same logic in ~200 lines using `explode()`, which works with arrays of any length. The driver extraction (double-nested: `vehicles[].drivers[]`) is particularly cleaner — two `explode_outer()` calls vs 9 UNION ALL branches in Flink.
