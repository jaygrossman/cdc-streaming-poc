#!/usr/bin/env python3
"""
Event-driven merge trigger service.

Connects to PostgreSQL, LISTENs on the 'stg_data_arrived' channel,
and calls merge_cdc_batch() when new staging data arrives. Debounces
notifications to batch nearby events.
"""

import os
import sys
import time
import json
import select
import logging

import psycopg2
import psycopg2.extensions

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("merge-trigger")

PGHOST = os.environ.get("PGHOST", "postgres-cdc")
PGPORT = os.environ.get("PGPORT", "5432")
PGUSER = os.environ.get("PGUSER", "cdc_user")
PGPASSWORD = os.environ.get("PGPASSWORD", "cdc_pass")
PGDATABASE = os.environ.get("PGDATABASE", "cdc_db")
DEBOUNCE_SECONDS = float(os.environ.get("DEBOUNCE_SECONDS", "2"))
INITIAL_WAIT_SECONDS = int(os.environ.get("INITIAL_WAIT_SECONDS", "60"))


def connect():
    return psycopg2.connect(
        host=PGHOST, port=PGPORT,
        user=PGUSER, password=PGPASSWORD,
        dbname=PGDATABASE,
    )


def wait_for_postgres():
    log.info("Waiting for PostgreSQL at %s:%s ...", PGHOST, PGPORT)
    while True:
        try:
            conn = connect()
            conn.close()
            log.info("PostgreSQL is ready.")
            return
        except psycopg2.OperationalError:
            time.sleep(2)


def run_merge(merge_conn):
    log.info("Running merge_cdc_batch() ...")
    start = time.time()
    try:
        cur = merge_conn.cursor()
        cur.execute("SELECT merge_cdc_batch();")
        result_json = cur.fetchone()[0]
        merge_conn.commit()
        elapsed = time.time() - start

        if isinstance(result_json, str):
            result = json.loads(result_json)
        else:
            result = result_json

        total_rows = sum(
            result[k]["rows_merged"]
            for k in ("policy", "coverage", "vehicle", "driver", "claim")
        )
        db_elapsed = result.get("elapsed_ms", 0)

        log.info(
            "Merge completed in %.1fs (DB: %.0fms). Rows merged: policy=%d coverage=%d vehicle=%d driver=%d claim=%d (total=%d)",
            elapsed, db_elapsed,
            result["policy"]["rows_merged"],
            result["coverage"]["rows_merged"],
            result["vehicle"]["rows_merged"],
            result["driver"]["rows_merged"],
            result["claim"]["rows_merged"],
            total_rows,
        )
    except Exception as e:
        merge_conn.rollback()
        log.error("merge_cdc_batch() FAILED: %s", e)
        raise


def listen_loop():
    listen_conn = connect()
    listen_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    merge_conn = connect()

    cur = listen_conn.cursor()
    cur.execute("LISTEN stg_data_arrived;")
    log.info("Listening on channel 'stg_data_arrived' ...")

    try:
        while True:
            if select.select([listen_conn], [], [], 60) == ([], [], []):
                continue

            listen_conn.poll()
            if not listen_conn.notifies:
                continue

            tables_seen = set()
            while listen_conn.notifies:
                notify = listen_conn.notifies.pop(0)
                tables_seen.add(notify.payload)

            log.info(
                "Notification(s) received from: %s. Debouncing %.1fs ...",
                ", ".join(sorted(tables_seen)),
                DEBOUNCE_SECONDS,
            )

            debounce_deadline = time.time() + DEBOUNCE_SECONDS
            while time.time() < debounce_deadline:
                remaining = debounce_deadline - time.time()
                if remaining <= 0:
                    break
                if select.select([listen_conn], [], [], remaining) != ([], [], []):
                    listen_conn.poll()
                    while listen_conn.notifies:
                        notify = listen_conn.notifies.pop(0)
                        tables_seen.add(notify.payload)

            log.info(
                "Debounce complete. Tables with new data: %s",
                ", ".join(sorted(tables_seen)),
            )

            run_merge(merge_conn)
    finally:
        listen_conn.close()
        merge_conn.close()


def main():
    wait_for_postgres()

    log.info(
        "Waiting %ds for Spark to populate staging tables...",
        INITIAL_WAIT_SECONDS,
    )
    time.sleep(INITIAL_WAIT_SECONDS)

    log.info("Running initial merge...")
    merge_conn = connect()
    try:
        run_merge(merge_conn)
    finally:
        merge_conn.close()

    log.info("Entering event-driven listen loop (debounce=%.1fs)", DEBOUNCE_SECONDS)
    while True:
        try:
            listen_loop()
        except psycopg2.OperationalError as e:
            log.warning("PostgreSQL connection lost: %s. Reconnecting in 5s...", e)
            time.sleep(5)
        except Exception as e:
            log.exception("Unexpected error in listen loop: %s", e)
            time.sleep(5)


if __name__ == "__main__":
    main()
