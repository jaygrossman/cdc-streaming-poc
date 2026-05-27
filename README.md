# CDC Flink PoC

Proof-of-concept demonstrating Change Data Capture from a PostgreSQL JSONB column, streamed through Kafka via Debezium, flattened into normalized relational tables by Flink SQL, and written back to PostgreSQL.

Everything runs in Docker with a single `docker compose up --build`.

## Architecture

```
┌──────────────┐       ┌──────────┐       ┌───────┐       ┌───────────┐       ┌──────────────┐
│  PostgreSQL  │──CDC──>│ Debezium │──────>│ Kafka │──────>│ Flink SQL │──────>│  PostgreSQL   │
│  (JSONB)     │       │          │       │       │       │ (flatten) │       │ (normalized)  │
└──────────────┘       └──────────┘       └───────┘       └───────────┘       └──────────────┘
```

A single `policy` table with nested JSONB (policyholder, coverages, vehicles, drivers, claims) is flattened into 5 normalized output tables: `output_policy`, `output_coverage`, `output_vehicle`, `output_driver`, `output_claim`.

## Variants

This repo contains five implementations of the pipeline, each in its own directory:

### [`append_new_records/`](append_new_records/)

**Direct Flink upsert** -- the simplest approach.

Flink SQL reads CDC events from Kafka and writes directly to the output tables using the JDBC connector in upsert mode. Each output table has a primary key, so Flink issues `INSERT ... ON CONFLICT ... UPDATE` statements.

- Handles inserts and updates
- Ignores deletes
- Lowest latency (~5-15s end-to-end)
- No intermediate tables or batch processing
- Best for: simple use cases where real-time upserts are sufficient

### [`dbt_upsert/`](dbt_upsert/)

**Flink to staging + dbt incremental merge** -- a more robust approach.

Flink SQL writes every CDC event (including deletes) to append-only staging tables (`stg_*`). A dbt container runs every 30s, merging the latest state from staging into the output tables using incremental models with `delete+insert` strategy.

- Handles inserts, updates, and deletes
- Preserves full change history in staging tables
- Higher latency (~8-10s end-to-end with manual trigger, ~30-60s with automatic loop)
- Staging tables capture the `op` field (c/r/u/d) and event timestamps
- dbt deduplicates by primary key (latest event wins) and applies deletes via post-hooks
- Best for: production-like patterns where audit trails, delete handling, and batch merge control matter

### [`dbt_event_trigger/`](dbt_event_trigger/)

**Flink to staging + event-driven dbt** -- the most responsive dbt approach.

Same staging + dbt architecture as `dbt_upsert`, but replaces the polling loop with PostgreSQL LISTEN/NOTIFY. When Flink writes to staging tables, a trigger sends a notification. A Python listener debounces for 3s then runs `dbt run` -- only when new data actually arrives.

- Same insert/update/delete handling and change history as `dbt_upsert`
- Near-real-time latency (~6-10s end-to-end)
- Zero wasted dbt runs when no data arrives
- Uses PostgreSQL triggers + Python psycopg2 LISTEN/NOTIFY
- Best for: when you want dbt's merge control without the latency penalty of polling

### [`pg_transaction/`](pg_transaction/)

**Flink to staging + PG transaction merge** -- the no-dbt approach.

Same staging architecture as the dbt variants, but replaces dbt entirely with a single PL/pgSQL function (`merge_cdc_batch()`) that merges all 5 output tables inside one Postgres transaction. Uses LISTEN/NOTIFY for event-driven triggering. Splits into separate source and CDC databases.

- Handles inserts, updates, and deletes
- All 5 output tables update atomically (single transaction)
- No dbt dependency -- pure SQL merge logic
- Separate source and CDC databases (production-like topology)
- Watermark-based staging tracking (no re-scanning)
- Best for: when you want atomic cross-table consistency, don't need dbt, and prefer a simpler operational footprint

### [`spark_streaming/`](spark_streaming/)

**Spark Structured Streaming + PG transaction merge** -- the Spark alternative.

Same staging + PL/pgSQL merge architecture as `pg_transaction`, but replaces Flink SQL with PySpark Structured Streaming. Uses `explode()` + `from_json()` for JSON array flattening instead of Flink's UNION ALL with hardcoded indices. Runs as a single `spark-submit` container with micro-batch processing (every 5s).

- Handles inserts, updates, and deletes
- All 5 output tables update atomically (single transaction)
- No dbt dependency -- pure SQL merge logic
- Separate source and CDC databases (production-like topology)
- `explode()` handles JSON arrays of any length (no hardcoded index limits)
- Tested: 50 policies (416 output rows) propagated end-to-end in ~9.5s
- Best for: when you prefer PySpark over Flink SQL, want cleaner array handling, or your team already uses Spark

## Comparison

| | append_new_records | dbt_upsert | dbt_event_trigger | pg_transaction | spark_streaming |
|---|---|---|---|---|---|
| Stream processor | Flink SQL | Flink SQL | Flink SQL | Flink SQL | Spark Structured Streaming |
| Writes to | `output_*` (upsert) | `stg_*` (append-only) | `stg_*` (append-only) | `stg_*` (append-only) | `stg_*` (append-only) |
| Merge strategy | Flink JDBC upsert | dbt incremental | dbt incremental | PL/pgSQL DELETE+INSERT | PL/pgSQL DELETE+INSERT |
| Delete handling | Ignored | Captured and applied | Captured and applied | In-transaction DELETE | In-transaction DELETE |
| Atomicity | Per-table | Per-table | Per-table | All 5 tables in 1 txn | All 5 tables in 1 txn |
| Databases | 1 (shared) | 1 (shared) | 1 (shared) | 2 (source + cdc) | 2 (source + cdc) |
| dbt required | No | Yes | Yes | No | No |
| Trigger | N/A | 30s polling loop | PG NOTIFY | PG NOTIFY | PG NOTIFY |
| End-to-end latency | ~5-15s | ~30-60s (loop) | ~6-10s (event-driven) | ~5-10s (event-driven) | ~10-18s (micro-batch) |
| Idle overhead | None | dbt runs even with no data | Zero when idle | Zero when idle | Zero when idle |

## Prerequisites

- Docker Desktop for Mac (or Docker Engine + Docker Compose on Linux)
- No other local installs required

## Quick Start

Pick a variant and run:

```bash
cd append_new_records
docker compose up --build
```

or

```bash
cd dbt_upsert
docker compose up --build
```

or

```bash
cd dbt_event_trigger
docker compose up --build
```

or

```bash
cd pg_transaction
docker compose up --build
```

or

```bash
cd spark_streaming
docker compose up --build
```

Each variant includes a verification script that checks every component and runs a live end-to-end test with timing:

```bash
./verify-pipeline.sh
```

## Tear Down

From the variant directory:

```bash
docker compose down -v
```

## Services

All five variants share the same core infrastructure:

| Service | Port | Description |
|---------|------|-------------|
| PostgreSQL | 5432 | Source + output database |
| Zookeeper | 2181 | Kafka coordination |
| Kafka | 9092 | Event streaming |
| Kafka Connect (Debezium) | 8083 | CDC connector |
| Flink JobManager | 8081 | Stream processing (dashboard) |
| Flink TaskManager | -- | Stream processing (worker) |
| PGAdmin | 5050 | Database UI (admin@admin.com / admin) |

The `dbt_upsert` and `dbt_event_trigger` variants add a **dbt** container. The `pg_transaction` variant adds a **merge-listener** container and uses two separate PostgreSQL instances (source on port 5433, CDC on port 5432). The `spark_streaming` variant replaces Flink with a **Spark Structured Streaming** container and uses the same merge-listener + dual-database topology as `pg_transaction`.
