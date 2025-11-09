#!/bin/bash

set -euo pipefail

echo "🔌 Registering Debezium PostgreSQL Connector..."

# Delete existing connector if it exists (to allow re-registration)
echo "🧹 Cleaning up existing connector..."
curl -sS -X DELETE http://kafka-connect:8083/connectors/postgres-connector 2>/dev/null || true
sleep 2

# Register the connector
echo "📝 Registering Debezium connector..."
curl -sS -X POST http://kafka-connect:8083/connectors \
  -H "Content-Type: application/json" \
  -d @/scripts/debezium/postgres-connector.json || true

echo ""
echo "✅ Debezium connector registration attempted. Current connectors:"
curl -sS http://kafka-connect:8083/connectors || true

