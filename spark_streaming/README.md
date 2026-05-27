# CDC Spark Streaming PoC -- Spark Structured Streaming Variant

PostgreSQL (JSONB) -> Debezium CDC -> Kafka -> Spark Structured Streaming -> Staging Tables -> PG NOTIFY -> merge_cdc_batch() -> Output Tables

This variant replaces Flink SQL with PySpark Structured Streaming. It uses the same staging + PL/pgSQL merge architecture as the `pg_transaction` variant: a single `merge_cdc_batch()` function merges all 5 output tables inside one Postgres transaction. Uses LISTEN/NOTIFY for event-driven triggering. Splits into separate source and CDC databases.

## Prerequisites

- Docker Desktop for Mac (or Docker Engine + Docker Compose on Linux)
- No other local installs required

## Quick Start

```bash
cd spark_streaming
docker compose up --build
```

## Architecture

```
┌──────────────┐       ┌──────────┐       ┌───────┐       ┌─────────────────┐
│ postgres-     │──CDC──>│ Debezium │──────>│ Kafka │──────>│ Spark Structured│
│ source        │       │          │       │       │       │ Streaming       │
│ source_db     │       └──────────┘       └───────┘       │ (PySpark)       │
│ port 5433     │                                          └────────┬────────┘
└──────────────┘                                                    │ JDBC append
                                                            ┌──────▼──────────┐
                                                            │ postgres-cdc    │
                                                            │ cdc_db          │
                                                            │ port 5432       │
                                                            │                 │
                                                            │ stg_* tables ───── NOTIFY ──┐
                                                            │ output_* tables │            │
                                                            │ merge_watermark │      ┌─────▼──────┐
                                                            └─────────────────┘      │  Python    │
                                                                     ▲               │  LISTEN +  │
                                                                     │               │  debounce  │
                                                                     └───────────────┤            │
                                                               merge_cdc_batch()     └────────────┘
```

## How It Works

1. **Source DB** (`postgres-source`): Holds the `policy` table with JSONB data. Debezium captures WAL changes.
2. **Spark Structured Streaming**: Reads CDC events from Kafka in micro-batches (every 5s), flattens nested JSONB into 5 entity types using `explode()` + `from_json()`, writes to append-only `stg_*` tables on the CDC DB.
3. **NOTIFY triggers**: Each staging table has an `AFTER INSERT` trigger that fires `pg_notify('stg_data_arrived', table_name)`.
4. **Merge listener**: A Python service LISTENs for notifications, debounces for 2s, then calls `SELECT merge_cdc_batch()`.
5. **`merge_cdc_batch()`**: A PL/pgSQL function that runs as a single transaction:
   - For each entity type, finds new staging rows since the last watermark
   - Deduplicates by primary key (latest event wins via `ROW_NUMBER()`)
   - DELETEs matching keys from the output table
   - INSERTs the latest non-delete rows
   - Advances the watermark
   - Returns a JSON summary with row counts and timing

All 5 output tables are updated atomically in one transaction. If any entity fails, the entire batch rolls back.

## Key Differences vs Flink Variant

| Aspect | pg_transaction (Flink) | spark_streaming (Spark) |
|--------|----------------------|------------------------|
| Stream processor | Flink SQL | PySpark Structured Streaming |
| Array handling | UNION ALL with fixed indices (max N) | `explode()` — handles any array length |
| Language | SQL (declarative) | Python (programmatic) |
| Latency model | True streaming (per-event) | Micro-batch (every 5s) |
| Containers | 2 (JobManager + TaskManager) | 1 (spark-submit) |
| Merge strategy | PL/pgSQL (same) | PL/pgSQL (same) |
| Delete handling | In-transaction DELETE (same) | In-transaction DELETE (same) |
| Atomicity | All 5 tables in 1 txn (same) | All 5 tables in 1 txn (same) |
| Databases | 2 (source + cdc) (same) | 2 (source + cdc) (same) |

## What Happens on Startup

1. **postgres-source** starts with logical replication, creates `policy` table, seeds 4 records
2. **postgres-cdc** starts, creates staging + output tables, merge function, NOTIFY triggers
3. **Zookeeper + Kafka** start
4. **Kafka Connect (Debezium)** connects to postgres-source, captures WAL changes
5. **Setup container** registers the Debezium connector
6. **Spark Structured Streaming** reads CDC events from Kafka, flattens JSONB, writes to staging tables on postgres-cdc
7. **Merge listener** waits 60s, runs initial merge, then listens for NOTIFY events

## Service Endpoints

| Service | URL/Port | Credentials |
|---------|----------|-------------|
| PGAdmin | http://localhost:5051 | admin@admin.com / admin |
| Kafka Connect REST | http://localhost:8083 | -- |
| Source PostgreSQL | localhost:5433 | cdc_user / cdc_pass / source_db |
| CDC PostgreSQL | localhost:5434 | cdc_user / cdc_pass / cdc_db |

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `DEBOUNCE_SECONDS` | `2` | Seconds to wait after first notification before merging |
| `INITIAL_WAIT_SECONDS` | `60` | Seconds to wait on startup for Spark to populate staging |

## Verify

```bash
./verify-pipeline.sh
```

The script tests insert, update, and delete propagation end-to-end with timing.

## Tested Performance

### Single-record operations

| Operation | Source to Staging | Staging to Output | End-to-end |
|-----------|-------------------|-------------------|------------|
| INSERT | ~6s | ~6s | ~12s |
| UPDATE | ~6s | ~6s | ~12s |
| DELETE | ~13s | ~6s | ~18s |

### Bulk insert — 50 policies

Inserted 50 randomized policies (each with 1-3 coverages, 1-2 vehicles with nested drivers, 0-3 claims) in a single SQL statement.

**End-to-end: ~9.5 seconds** for all 50 policies to appear in output tables.

| Table | Before | After | Delta |
|-------|--------|-------|-------|
| output_policy | 4 | 54 | +50 |
| output_coverage | 8 | 99 | +91 |
| output_vehicle | 6 | 83 | +77 |
| output_driver | 8 | 130 | +122 |
| output_claim | 6 | 82 | +76 |

**416 total output rows** created from 50 source records, all merged atomically.

Timing breakdown:
- ~2s: INSERT into source PostgreSQL
- ~5s: Spark micro-batch picks up from Kafka, flattens JSONB, writes to staging
- ~2s: PG NOTIFY debounce + `merge_cdc_batch()` merges all 5 tables in one transaction

## Tear Down

```bash
docker compose down -v
```

## Troubleshooting

### Spark job not producing data
- Check Spark logs: `docker compose logs spark --tail 50`
- Verify Kafka topic has data: `docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic cdc.public.policy --from-beginning --max-messages 1`

### Merge not triggering
- Check listener logs: `docker compose logs merge-listener --tail 30`
- Verify NOTIFY triggers: `docker compose exec postgres-cdc psql -U cdc_user -d cdc_db -c "SELECT tgname FROM pg_trigger WHERE tgname LIKE 'stg_%';"`
- Run merge manually: `docker compose exec postgres-cdc psql -U cdc_user -d cdc_db -c "SELECT merge_cdc_batch();"`

### Output tables empty but staging has data
- Check watermarks: `docker compose exec postgres-cdc psql -U cdc_user -d cdc_db -c "SELECT * FROM merge_watermark;"`
- The initial merge runs after 60s. Check: `docker compose logs merge-listener | grep "initial merge"`

### Debezium not connecting
- Source DB is on port 5433 externally but 5432 internally (Docker network)
- Check connector: `curl -s http://localhost:8083/connectors/policy-connector/status | python3 -m json.tool`
