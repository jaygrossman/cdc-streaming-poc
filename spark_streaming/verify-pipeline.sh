#!/bin/bash
###############################################################################
# verify-pipeline.sh (spark_streaming variant)
# Runs end-to-end diagnostics including insert, update, and delete tests.
# Run from the spark_streaming/ directory.
###############################################################################

set -o pipefail

PASS=0
FAIL=0
WARN=0

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

pass() { ((PASS++)); echo -e "  ${GREEN}✔ PASS${NC} $1"; }
fail() { ((FAIL++)); echo -e "  ${RED}✘ FAIL${NC} $1"; }
warn() { ((WARN++)); echo -e "  ${YELLOW}⚠ WARN${NC} $1"; }
info() { echo -e "  ${CYAN}ℹ${NC} $1"; }
header() { echo ""; echo -e "${BOLD}[$1] $2${NC}"; }
now_ms() { python3 -c "import time; print(int(time.time()*1000))"; }

SRC="docker compose exec -T postgres-source psql -U cdc_user -d source_db -tAc"
CDC="docker compose exec -T postgres-cdc psql -U cdc_user -d cdc_db -tAc"

###############################################################################
header "STEP 0" "Docker Compose services"
###############################################################################

SERVICES_JSON=$(docker compose ps --format json 2>/dev/null)
if [ $? -ne 0 ] || [ -z "$SERVICES_JSON" ]; then
    fail "docker compose ps failed — are you in the project directory?"
    echo ""
    echo -e "${RED}Cannot continue. Exiting.${NC}"
    exit 1
fi

EXPECTED_RUNNING=("postgres-source" "postgres-cdc" "zookeeper" "kafka" "kafka-connect" "spark" "pgadmin" "merge-listener")

for svc in "${EXPECTED_RUNNING[@]}"; do
    state=$(echo "$SERVICES_JSON" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for s in data:
    if s['Service'] == '$svc':
        print(s['State'])
        break
" 2>/dev/null)
    if [ -z "$state" ]; then
        fail "$svc — service not found"
    elif [[ "$state" == *"running"* ]]; then
        pass "$svc is running"
    else
        fail "$svc state: $state"
    fi
done

setup_info=$(echo "$SERVICES_JSON" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for s in data:
    if s['Service'] == 'setup':
        print(s['State'], s.get('ExitCode', ''))
        break
" 2>/dev/null)
if [ -n "$setup_info" ]; then
    setup_state=$(echo "$setup_info" | awk '{print $1}')
    setup_exit=$(echo "$setup_info" | awk '{print $2}')
    if [[ "$setup_state" == *"exited"* ]] && [[ "$setup_exit" == "0" ]]; then
        pass "setup exited cleanly (code 0)"
    else
        fail "setup state: $setup_state, exit code: $setup_exit"
    fi
else
    warn "setup service not found"
fi

###############################################################################
header "STEP 1" "PostgreSQL source — seed data"
###############################################################################

SEED_COUNT=$($SRC "SELECT count(*) FROM policy;" 2>/dev/null | tr -d '[:space:]')
if [ -z "$SEED_COUNT" ]; then
    fail "Could not query policy table on postgres-source"
elif [ "$SEED_COUNT" -ge 3 ]; then
    pass "source policy table has $SEED_COUNT seed records"
else
    warn "source policy table has $SEED_COUNT records (expected >= 3)"
fi

###############################################################################
header "STEP 2" "Debezium connector status"
###############################################################################

CONNECTOR_STATUS=$(curl -s http://localhost:8083/connectors/policy-connector/status 2>/dev/null)
if [ -z "$CONNECTOR_STATUS" ]; then
    fail "Kafka Connect API not reachable"
else
    CONN_STATE=$(echo "$CONNECTOR_STATUS" | python3 -c "import sys,json; print(json.load(sys.stdin)['connector']['state'])" 2>/dev/null)
    TASK_STATE=$(echo "$CONNECTOR_STATUS" | python3 -c "import sys,json; tasks=json.load(sys.stdin)['tasks']; print(tasks[0]['state'] if tasks else 'NO_TASKS')" 2>/dev/null)
    [ "$CONN_STATE" = "RUNNING" ] && pass "Connector state: RUNNING" || fail "Connector state: ${CONN_STATE:-UNKNOWN}"
    [ "$TASK_STATE" = "RUNNING" ] && pass "Task state: RUNNING" || fail "Task state: ${TASK_STATE:-UNKNOWN}"
fi

###############################################################################
header "STEP 3" "Kafka topic — CDC events"
###############################################################################

TOPIC_EXISTS=$(docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | grep "cdc.public.policy")
if [ -z "$TOPIC_EXISTS" ]; then
    fail "Topic cdc.public.policy does not exist"
else
    pass "Topic cdc.public.policy exists"
    MSG_COUNT=$(docker compose exec -T kafka kafka-run-class kafka.tools.GetOffsetShell \
        --broker-list localhost:9092 --topic cdc.public.policy --time -1 2>/dev/null | awk -F: '{sum+=$3} END {print sum}')
    [ -n "$MSG_COUNT" ] && [ "$MSG_COUNT" -gt 0 ] && pass "Topic has $MSG_COUNT message(s)" || warn "Topic appears empty"
fi

###############################################################################
header "STEP 4" "Spark — streaming job status"
###############################################################################

SPARK_RUNNING=$(echo "$SERVICES_JSON" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for s in data:
    if s['Service'] == 'spark':
        print(s['State'])
        break
" 2>/dev/null)

if [[ "$SPARK_RUNNING" == *"running"* ]]; then
    pass "Spark streaming container is running"
    SPARK_LOG=$(docker compose logs spark 2>/dev/null | tail -50)
    echo "$SPARK_LOG" | grep -q "Streaming query started" && pass "Spark streaming query is active" || warn "Streaming query may not be active yet"
else
    fail "Spark streaming container is not running (state: $SPARK_RUNNING)"
fi

###############################################################################
header "STEP 5" "Staging tables — Spark output (postgres-cdc)"
###############################################################################

for tbl in stg_policy stg_coverage stg_vehicle stg_driver stg_claim; do
    COUNT=$($CDC "SELECT count(*) FROM $tbl;" 2>/dev/null | tr -d '[:space:]')
    if [ -z "$COUNT" ]; then fail "$tbl — could not query"
    elif [ "$COUNT" -gt 0 ]; then pass "$tbl has $COUNT row(s)"
    else fail "$tbl is empty"
    fi
done

OP_CHECK=$($CDC "SELECT DISTINCT op FROM stg_policy ORDER BY op;" 2>/dev/null | tr -d '[:space:]')
[ -n "$OP_CHECK" ] && pass "stg_policy has op values: $OP_CHECK" || warn "Could not verify op column"

###############################################################################
header "STEP 6" "Merge-listener status"
###############################################################################

ML_LOG=$(docker compose logs merge-listener 2>/dev/null | tail -80)
echo "$ML_LOG" | grep -q "Merge completed" && pass "merge_cdc_batch() has completed at least once" || warn "No merge completions yet"
echo "$ML_LOG" | grep -q "Listening on channel" && pass "Listener is active on 'stg_data_arrived'" || warn "Listener may not be active"
echo "$ML_LOG" | grep -q "Notification(s) received" && pass "Listener has received notifications" || warn "No notifications received yet"

###############################################################################
header "STEP 7" "Output tables — merged data (postgres-cdc)"
###############################################################################

for tbl in output_policy output_coverage output_vehicle output_driver output_claim; do
    COUNT=$($CDC "SELECT count(*) FROM $tbl;" 2>/dev/null | tr -d '[:space:]')
    if [ -z "$COUNT" ]; then fail "$tbl — could not query"
    elif [ "$COUNT" -gt 0 ]; then pass "$tbl has $COUNT row(s)"
    else fail "$tbl is empty"
    fi
done

###############################################################################
header "STEP 8" "Live INSERT test"
###############################################################################

LIVE_TAG="POL-VERIFY-$(date +%s)"
info "Inserting test record: $LIVE_TAG"
INSERT_START=$(now_ms)

docker compose exec -T postgres-source psql -U cdc_user -d source_db -c "
INSERT INTO policy (data) VALUES ('{
  \"policy_number\": \"$LIVE_TAG\",
  \"status\": \"active\",
  \"effective_date\": \"2024-06-01\",
  \"expiration_date\": \"2025-06-01\",
  \"policyholder\": {
    \"first_name\": \"Verify\", \"last_name\": \"Script\", \"date_of_birth\": \"1990-01-01\",
    \"contact\": {
      \"email\": \"verify@test.com\", \"phone\": \"+1-555-0000\",
      \"address\": {\"street\": \"1 Verify Lane\", \"city\": \"Testburg\", \"state\": \"NY\", \"zip\": \"10001\"}
    }
  },
  \"coverages\": [{\"type\": \"liability\", \"limit\": 100000, \"deductible\": 500, \"premium\": 600.00}],
  \"vehicles\": [{
    \"vin\": \"VERIFY12345678901\", \"year\": 2024, \"make\": \"Toyota\", \"model\": \"Camry\",
    \"drivers\": [{\"name\": \"Verify Script\", \"license_number\": \"V000-0000-0001\", \"is_primary\": true}]
  }],
  \"claims_history\": []
}'::jsonb);
" > /dev/null 2>&1

info "Waiting for staging (up to 30s)..."
STG_FOUND=false
for i in $(seq 1 6); do
    sleep 5
    STG=$($CDC "SELECT count(*) FROM stg_policy WHERE policy_number = '$LIVE_TAG';" 2>/dev/null | tr -d '[:space:]')
    if [ "${STG:-0}" -ge 1 ]; then
        STG_FOUND=true
        STG_ARRIVED=$(now_ms)
        STG_ELAPSED=$(( STG_ARRIVED - INSERT_START ))
        pass "Record in staging after ${STG_ELAPSED}ms"
        break
    fi
done
[ "$STG_FOUND" = false ] && fail "Record did not reach staging within 30s"

info "Waiting for auto-merge into output (up to 30s)..."
INSERT_OK=false
for i in $(seq 1 6); do
    sleep 5
    OUT=$($CDC "SELECT count(*) FROM output_policy WHERE policy_number = '$LIVE_TAG';" 2>/dev/null | tr -d '[:space:]')
    if [ "${OUT:-0}" -ge 1 ]; then
        INSERT_OK=true
        OUT_ARRIVED=$(now_ms)
        E2E=$(( OUT_ARRIVED - INSERT_START ))
        MERGE_LAG=$(( OUT_ARRIVED - STG_ARRIVED ))
        pass "Record auto-merged into output_policy"
        info "Timing:"
        info "  Source -> Staging: ${STG_ELAPSED}ms"
        info "  Staging -> Output: ${MERGE_LAG}ms"
        info "  End-to-end:        ${E2E}ms"

        # Get policy_id for later tests
        LIVE_POLICY_ID=$($CDC "SELECT policy_id FROM output_policy WHERE policy_number = '$LIVE_TAG';" 2>/dev/null | tr -d '[:space:]')

        COV=$($CDC "SELECT count(*) FROM output_coverage WHERE policy_id = $LIVE_POLICY_ID;" 2>/dev/null | tr -d '[:space:]')
        VEH=$($CDC "SELECT count(*) FROM output_vehicle WHERE policy_id = $LIVE_POLICY_ID;" 2>/dev/null | tr -d '[:space:]')
        DRV=$($CDC "SELECT count(*) FROM output_driver WHERE policy_id = $LIVE_POLICY_ID;" 2>/dev/null | tr -d '[:space:]')
        [ "${COV:-0}" -ge 1 ] && pass "output_coverage: $COV row(s)" || warn "output_coverage: 0 rows"
        [ "${VEH:-0}" -ge 1 ] && pass "output_vehicle: $VEH row(s)" || warn "output_vehicle: 0 rows"
        [ "${DRV:-0}" -ge 1 ] && pass "output_driver: $DRV row(s)" || warn "output_driver: 0 rows"
        break
    fi
done
[ "$INSERT_OK" = false ] && fail "Record did not auto-merge within 30s"

###############################################################################
header "STEP 8b" "Live UPDATE test — add vehicle + coverage"
###############################################################################

if [ "$INSERT_OK" = true ]; then
    SRC_ID=$(docker compose exec -T postgres-source psql -U cdc_user -d source_db -tAc \
        "SELECT id FROM policy WHERE data->>'policy_number' = '$LIVE_TAG';" 2>/dev/null | tr -d '[:space:]')

    COV_BEFORE=$($CDC "SELECT count(*) FROM output_coverage WHERE policy_id = $LIVE_POLICY_ID;" 2>/dev/null | tr -d '[:space:]')
    VEH_BEFORE=$($CDC "SELECT count(*) FROM output_vehicle WHERE policy_id = $LIVE_POLICY_ID;" 2>/dev/null | tr -d '[:space:]')

    info "Policy $LIVE_TAG (source id=$SRC_ID, output policy_id=$LIVE_POLICY_ID): $COV_BEFORE coverage(s), $VEH_BEFORE vehicle(s)"
    info "Adding collision coverage + Subaru Outback..."

    UPDATE_START=$(now_ms)

    docker compose exec -T postgres-source psql -U cdc_user -d source_db -c "
    UPDATE policy SET data = data
        || '{\"coverages\": [
            {\"type\": \"liability\", \"limit\": 100000, \"deductible\": 500, \"premium\": 600.00},
            {\"type\": \"collision\", \"limit\": 50000, \"deductible\": 1000, \"premium\": 475.00}
        ]}'::jsonb
        || '{\"vehicles\": [
            {\"vin\": \"VERIFY12345678901\", \"year\": 2024, \"make\": \"Toyota\", \"model\": \"Camry\",
             \"drivers\": [{\"name\": \"Verify Script\", \"license_number\": \"V000-0000-0001\", \"is_primary\": true}]},
            {\"vin\": \"UPDATE98765432100\", \"year\": 2025, \"make\": \"Subaru\", \"model\": \"Outback\",
             \"drivers\": [{\"name\": \"Verify Partner\", \"license_number\": \"V000-0000-0002\", \"is_primary\": true}]}
        ]}'::jsonb
    WHERE id = $SRC_ID;
    " > /dev/null 2>&1

    info "Waiting for update in staging (up to 30s)..."
    UPD_STG=false
    for i in $(seq 1 6); do
        sleep 5
        UC=$($CDC "SELECT count(*) FROM stg_policy WHERE policy_id = $LIVE_POLICY_ID AND op = 'u';" 2>/dev/null | tr -d '[:space:]')
        if [ "${UC:-0}" -ge 1 ]; then
            UPD_STG=true
            UPD_STG_MS=$(now_ms)
            UPD_STG_ELAPSED=$(( UPD_STG_MS - UPDATE_START ))
            pass "Update in staging after ${UPD_STG_ELAPSED}ms"
            break
        fi
    done
    [ "$UPD_STG" = false ] && fail "Update did not reach staging within 30s"

    info "Waiting for auto-merge of update (up to 60s)..."
    UPD_OK=false
    for i in $(seq 1 12); do
        sleep 5
        COV_AFTER=$($CDC "SELECT count(*) FROM output_coverage WHERE policy_id = $LIVE_POLICY_ID;" 2>/dev/null | tr -d '[:space:]')
        VEH_AFTER=$($CDC "SELECT count(*) FROM output_vehicle WHERE policy_id = $LIVE_POLICY_ID;" 2>/dev/null | tr -d '[:space:]')
        if [ "${COV_AFTER:-0}" -ge 2 ] && [ "${VEH_AFTER:-0}" -ge 2 ]; then
            UPD_OK=true
            UPD_DONE=$(now_ms)
            UPD_E2E=$(( UPD_DONE - UPDATE_START ))
            DRV_AFTER=$($CDC "SELECT count(*) FROM output_driver WHERE policy_id = $LIVE_POLICY_ID;" 2>/dev/null | tr -d '[:space:]')
            pass "output_coverage: $COV_BEFORE -> $COV_AFTER"
            pass "output_vehicle: $VEH_BEFORE -> $VEH_AFTER"
            [ "${DRV_AFTER:-0}" -ge 2 ] && pass "output_driver: 1 -> $DRV_AFTER" || fail "output_driver: expected >= 2, got $DRV_AFTER"
            if [ "$UPD_STG" = true ]; then
                UPD_MERGE=$(( UPD_DONE - UPD_STG_MS ))
                info "Update timing:"
                info "  Source -> Staging: ${UPD_STG_ELAPSED}ms"
                info "  Staging -> Output: ${UPD_MERGE}ms"
                info "  End-to-end:        ${UPD_E2E}ms"
            fi
            break
        fi
    done
    [ "$UPD_OK" = false ] && fail "Update did not auto-merge within 60s"
else
    info "Skipping update test — insert test did not pass"
fi

###############################################################################
header "STEP 8c" "Live DELETE test — remove policy from source"
###############################################################################

if [ "$INSERT_OK" = true ]; then
    info "Deleting policy $LIVE_TAG (source id=$SRC_ID) from source DB..."

    DEL_START=$(now_ms)

    docker compose exec -T postgres-source psql -U cdc_user -d source_db -c \
        "DELETE FROM policy WHERE id = $SRC_ID;" > /dev/null 2>&1

    info "Waiting for delete event in staging (up to 30s)..."
    DEL_STG=false
    for i in $(seq 1 6); do
        sleep 5
        DC=$($CDC "SELECT count(*) FROM stg_policy WHERE policy_id = $LIVE_POLICY_ID AND op = 'd';" 2>/dev/null | tr -d '[:space:]')
        if [ "${DC:-0}" -ge 1 ]; then
            DEL_STG=true
            DEL_STG_MS=$(now_ms)
            DEL_STG_ELAPSED=$(( DEL_STG_MS - DEL_START ))
            pass "Delete event (op='d') in staging after ${DEL_STG_ELAPSED}ms"
            break
        fi
    done
    [ "$DEL_STG" = false ] && fail "Delete event did not reach staging within 30s"

    info "Waiting for auto-merge to remove from output (up to 30s)..."
    DEL_OK=false
    for i in $(seq 1 6); do
        sleep 5
        POLICY_GONE=$($CDC "SELECT count(*) FROM output_policy WHERE policy_id = $LIVE_POLICY_ID;" 2>/dev/null | tr -d '[:space:]')
        if [ "${POLICY_GONE:-1}" = "0" ]; then
            DEL_OK=true
            DEL_DONE=$(now_ms)
            DEL_E2E=$(( DEL_DONE - DEL_START ))

            COV_GONE=$($CDC "SELECT count(*) FROM output_coverage WHERE policy_id = $LIVE_POLICY_ID;" 2>/dev/null | tr -d '[:space:]')
            VEH_GONE=$($CDC "SELECT count(*) FROM output_vehicle WHERE policy_id = $LIVE_POLICY_ID;" 2>/dev/null | tr -d '[:space:]')
            DRV_GONE=$($CDC "SELECT count(*) FROM output_driver WHERE policy_id = $LIVE_POLICY_ID;" 2>/dev/null | tr -d '[:space:]')
            CLM_GONE=$($CDC "SELECT count(*) FROM output_claim WHERE policy_id = $LIVE_POLICY_ID;" 2>/dev/null | tr -d '[:space:]')

            pass "output_policy: deleted (0 rows)"
            [ "${COV_GONE:-1}" = "0" ] && pass "output_coverage: deleted (0 rows)" || fail "output_coverage: expected 0, got $COV_GONE"
            [ "${VEH_GONE:-1}" = "0" ] && pass "output_vehicle: deleted (0 rows)" || fail "output_vehicle: expected 0, got $VEH_GONE"
            [ "${DRV_GONE:-1}" = "0" ] && pass "output_driver: deleted (0 rows)" || fail "output_driver: expected 0, got $DRV_GONE"
            [ "${CLM_GONE:-1}" = "0" ] && pass "output_claim: deleted (0 rows)" || fail "output_claim: expected 0, got $CLM_GONE"

            if [ "$DEL_STG" = true ]; then
                DEL_MERGE=$(( DEL_DONE - DEL_STG_MS ))
                info "Delete timing:"
                info "  Source -> Staging: ${DEL_STG_ELAPSED}ms"
                info "  Staging -> Output: ${DEL_MERGE}ms"
                info "  End-to-end:        ${DEL_E2E}ms"
            fi
            break
        fi
    done
    [ "$DEL_OK" = false ] && fail "Delete did not propagate to output tables within 30s"
else
    info "Skipping delete test — insert test did not pass"
fi

###############################################################################
header "STEP 9" "PGAdmin"
###############################################################################

PGADMIN_HTTP=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:5051/login 2>/dev/null)
[ "$PGADMIN_HTTP" = "200" ] && pass "PGAdmin is reachable at http://localhost:5051" || fail "PGAdmin not reachable (HTTP $PGADMIN_HTTP)"

###############################################################################
# Summary
###############################################################################

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo -e "${BOLD}RESULTS${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo -e "  ${GREEN}✔ Passed: $PASS${NC}"
echo -e "  ${YELLOW}⚠ Warnings: $WARN${NC}"
echo -e "  ${RED}✘ Failed: $FAIL${NC}"
echo ""

if [ "$FAIL" -eq 0 ]; then
    echo -e "${GREEN}${BOLD}Pipeline is fully operational.${NC}"
    exit 0
elif [ "$FAIL" -le 2 ]; then
    echo -e "${YELLOW}${BOLD}Pipeline is partially working. Check failures above.${NC}"
    exit 1
else
    echo -e "${RED}${BOLD}Pipeline has significant issues. Debug from Step 0 downward.${NC}"
    exit 2
fi
