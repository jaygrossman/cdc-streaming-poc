#!/bin/sh
set -e

echo "=== CDC Spark Streaming PoC Setup ==="

echo "Waiting for Kafka Connect REST API..."
until curl -s -o /dev/null -w "%{http_code}" http://kafka-connect:8083/connectors | grep -q "200"; do
    echo "  Kafka Connect not ready yet, retrying in 3s..."
    sleep 3
done
echo "Kafka Connect is ready."

echo ""
echo "Registering Debezium connector..."
RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" -X POST http://kafka-connect:8083/connectors \
    -H "Content-Type: application/json" \
    -d @/opt/setup/register-connector.json)

if [ "$RESPONSE" = "201" ] || [ "$RESPONSE" = "200" ]; then
    echo "Debezium connector registered successfully (HTTP $RESPONSE)."
elif [ "$RESPONSE" = "409" ]; then
    echo "Debezium connector already exists (HTTP 409), skipping."
else
    echo "WARNING: Unexpected response registering connector: HTTP $RESPONSE"
    curl -s -X POST http://kafka-connect:8083/connectors \
        -H "Content-Type: application/json" \
        -d @/opt/setup/register-connector.json
fi

echo ""
echo "Verifying connector status..."
sleep 5
curl -s http://kafka-connect:8083/connectors/policy-connector/status
echo ""

echo ""
echo "=== Setup complete ==="
echo "Debezium is now capturing changes from the policy table."
echo "Spark Structured Streaming job will process CDC events from Kafka."
