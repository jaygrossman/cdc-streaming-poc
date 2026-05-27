# Prompt: Create `spark_streaming/` Variant

## Context

This repo (`cdc-flink-poc`) contains four CDC pipeline variants that all share the same pattern:

PostgreSQL (JSONB) -> Debezium CDC -> Kafka -> Stream Processor -> Staging Tables -> Merge -> Output Tables

The existing `pg_transaction/` variant uses Flink SQL as the stream processor and a PL/pgSQL function (`merge_cdc_batch()`) triggered via PG LISTEN/NOTIFY to merge staging data into output tables atomically.

Your task is to create a new variant called `spark_streaming/` that replaces Flink with Spark Structured Streaming (PySpark). Everything else — the source database, Debezium, Kafka, the CDC database schema, the merge function, the Python NOTIFY listener — stays the same.

## Reference Implementation

Use `pg_transaction/` as the base. Read all files in that directory to understand the full architecture before writing any code. The key files are:

- `docker-compose.yml` — full service topology
- `Dockerfile.flink` — Flink image with Kafka/JDBC JARs
- `flink-sql/submit-jobs.sql` — Flink SQL that reads Kafka, flattens JSONB, writes to staging tables
- `postgres-cdc/01-init.sql` — staging tables, output tables, `merge_cdc_batch()`, NOTIFY triggers
- `postgres-source/01-init.sql` — source `policy` table
- `postgres-source/02-seed-data.sql` — seed data (4 policies)
- `postgres-source/postgresql.conf` — logical replication config
- `scripts/event-trigger-merge.py` — Python LISTEN/NOTIFY merge listener
- `scripts/setup.sh` — Debezium connector registration
- `scripts/start-jobmanager.sh` — Flink job submission script
- `debezium/register-connector.json` — Debezium connector config
- `pgadmin/servers.json` and `pgadmin/pgpass` — PGAdmin config
- `verify-pipeline.sh` — end-to-end verification script
- `README.md` — variant documentation

## What to Create

Create the directory `spark_streaming/` with the following structure:

```
spark_streaming/
├── docker-compose.yml
├── Dockerfile.spark
├── Dockerfile.merge
├── spark-app/
│   └── cdc_streaming.py
├── postgres-source/
│   ├── 01-init.sql
│   ├── 02-seed-data.sql
│   └── postgresql.conf
├── postgres-cdc/
│   └── 01-init.sql
├── debezium/
│   └── register-connector.json
├── scripts/
│   ├── event-trigger-merge.py
│   └── setup.sh
├── pgadmin/
│   ├── servers.json
│   └── pgpass
├── verify-pipeline.sh
└── README.md
```

## Design Decisions

### 1. Single Spark container (no master/worker split)

Use a single container that runs `spark-submit` directly. No separate Spark master or worker services — this is a PoC, not a cluster. The container should start, submit the streaming job, and keep running.

### 2. Use `foreachBatch` to write all 5 staging tables per micro-batch

The PySpark job should use `foreachBatch` to process each micro-batch and write all 5 staging tables (stg_policy, stg_coverage, stg_vehicle, stg_driver, stg_claim) within a single callback. This keeps the logic centralized and easy to follow.

### 3. Trigger interval: 5 seconds

Use `.trigger(processingTime="5 seconds")` as the micro-batch interval. This gives comparable end-to-end latency to the Flink variant (~8-10s total including debounce and merge).

### 4. Use `explode()` for JSON arrays instead of UNION ALL

The biggest improvement over the Flink SQL approach: use PySpark's `explode()` + `from_json()` to handle nested JSON arrays (coverages, vehicles, drivers, claims). This handles arrays of any length — no hardcoded index limits.

For scalar fields (policy-level data), use `get_json_object()`.

### 5. Docker image: `bitnami/spark:3.5`

Use the Bitnami Spark image. It's smaller and simpler for Docker Compose than the official Apache image. Include the following JARs (download or use `--packages`):

- `spark-sql-kafka` — for `.readStream.format("kafka")`
- `postgresql` JDBC driver — for `.write.jdbc()`

Prefer baking the JARs into the Docker image (wget in Dockerfile) over `--packages` to avoid Maven downloads at runtime.

### 6. Checkpoint directory

Spark Structured Streaming requires a checkpoint location. Use a local directory inside the container (e.g., `/tmp/spark-checkpoints`). For this PoC, a Docker volume is not necessary.

## Detailed Specification for `cdc_streaming.py`

### Kafka source

```python
raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "cdc.public.policy") \
    .option("startingOffsets", "earliest") \
    .load()
```

### Parse the Debezium envelope

The Kafka value is a JSON string with the Debezium envelope structure:

```json
{
  "before": {"id": 1, "data": "{...json...}", "created_at": "...", "updated_at": "..."},
  "after":  {"id": 1, "data": "{...json...}", "created_at": "...", "updated_at": "..."},
  "op": "c",
  "ts_ms": 1234567890
}
```

- `before` is null for inserts (`c`, `r`); `after` is null for deletes (`d`).
- `data` is a JSON string (the JSONB column), NOT a parsed struct — so it must be accessed with `get_json_object()` or `from_json()`.
- Define the Debezium envelope as a StructType schema and parse with `from_json`.

### Resolve before/after

Use `coalesce(after, before)` to get the record data regardless of operation type. Deletes only have `before`.

### Flatten and write to staging tables

In the `foreachBatch` callback, derive 5 DataFrames from the micro-batch:

1. **stg_policy** — scalar fields extracted with `get_json_object()`:
   - `policy_id`, `policy_number`, `status`, `effective_date`, `expiration_date`
   - `holder_first_name`, `holder_last_name`, `holder_dob`, `holder_email`, `holder_phone`
   - `holder_street`, `holder_city`, `holder_state`, `holder_zip`
   - `source_event_time`, `op`, `event_time`

2. **stg_coverage** — `explode(from_json(get_json_object(data, '$.coverages'), array_schema))`:
   - `policy_id`, `coverage_type`, `coverage_limit`, `deductible`, `premium`, `op`, `event_time`

3. **stg_vehicle** — `explode(from_json(get_json_object(data, '$.vehicles'), array_schema))`:
   - `policy_id`, `vin`, `year_made`, `make`, `model`, `op`, `event_time`

4. **stg_driver** — explode vehicles first, then explode nested `drivers` array:
   - `policy_id`, `vehicle_vin`, `driver_name`, `license_number`, `is_primary`, `op`, `event_time`

5. **stg_claim** — `explode(from_json(get_json_object(data, '$.claims_history'), array_schema))`:
   - `policy_id`, `claim_id`, `claim_date`, `amount`, `status`, `description`, `op`, `event_time`

Filter out rows where the array element is null (e.g., empty arrays produce no rows after explode, which is the desired behavior).

### JDBC write

```python
df.write.jdbc(
    url="jdbc:postgresql://postgres-cdc:5432/cdc_db",
    table="stg_policy",
    mode="append",
    properties={"user": "cdc_user", "password": "cdc_pass", "driver": "org.postgresql.Driver"}
)
```

## Files to Copy Unchanged from `pg_transaction/`

These files are identical — copy them directly:

- `postgres-source/01-init.sql`
- `postgres-source/02-seed-data.sql`
- `postgres-source/postgresql.conf`
- `postgres-cdc/01-init.sql`
- `debezium/register-connector.json`
- `scripts/event-trigger-merge.py`
- `pgadmin/servers.json`
- `pgadmin/pgpass`
- `Dockerfile.merge`

## Files to Adapt from `pg_transaction/`

- **`docker-compose.yml`**: Remove `flink-jobmanager` and `flink-taskmanager`. Add a single `spark` service. Keep all other services identical. Use container name prefix `spark-` instead of `pgtxn-`.
- **`scripts/setup.sh`**: Same Debezium registration logic. Remove any Flink-specific wait logic if present. Add a wait for the Spark container to be ready if needed.
- **`verify-pipeline.sh`**: Adapt health checks to reference Spark instead of Flink. Same end-to-end test logic (insert, update, delete with timing).
- **`README.md`**: Document the Spark variant with architecture diagram, service table, and comparison to the Flink variant.

## Files to Create New

- **`Dockerfile.spark`**: Based on `bitnami/spark:3.5`. Download `spark-sql-kafka` connector JAR and `postgresql` JDBC driver JAR.
- **`spark-app/cdc_streaming.py`**: The PySpark Structured Streaming job as described above.

## Testing

After creating all files, the variant should work with:

```bash
cd spark_streaming
docker compose up --build
```

And verify with:

```bash
./verify-pipeline.sh
```

The verification script should confirm:
1. All services are healthy (Postgres source, Postgres CDC, Kafka, Debezium, Spark)
2. Debezium connector is running
3. Seed data has flowed through to output tables (4 policies)
4. A live INSERT propagates end-to-end
5. A live UPDATE propagates end-to-end
6. A live DELETE propagates end-to-end

## Comparison Row for Root README

After creating the variant, update the root `README.md` to add `spark_streaming` as a fifth variant in the comparison table and variant list.
