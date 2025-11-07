#!/bin/bash

set -euo pipefail

echo "🔌 Registering Debezium PostgreSQL Connector..."

curl -sS -X POST http://kafka-connect:8083/connectors \
  -H "Content-Type: application/json" \
  -d @/debezium-connectors/postgres-connector.json || true

echo ""
echo "✅ Debezium connector registration attempted. Current connectors:"
curl -sS http://kafka-connect:8083/connectors || true

