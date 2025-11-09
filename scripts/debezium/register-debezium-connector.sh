#!/bin/bash

set -euo pipefail

echo "🔌 Registering Debezium PostgreSQL Connector..."

# Get credentials from environment
DB_USER=${DB_USER}
DB_PASSWORD=${DB_PASSWORD}
DB_NAME=${DB_NAME}

# Delete existing connector if it exists (to allow re-registration)
echo "🧹 Cleaning up existing connector..."
curl -sS -X DELETE http://kafka-connect:8083/connectors/postgres-connector 2>/dev/null || true
sleep 2

# Create temporary JSON with substituted values
TEMP_JSON=$(mktemp)
sed "s|\"database.user\": \"postgres\"|\"database.user\": \"${DB_USER}\"|g; \
     s|\"database.password\": \"password\"|\"database.password\": \"${DB_PASSWORD}\"|g; \
     s|\"database.dbname\": \"business_db\"|\"database.dbname\": \"${DB_NAME}\"|g" \
     /scripts/debezium/postgres-connector.json > "$TEMP_JSON"

# Register the connector
echo "📝 Registering Debezium connector..."
curl -sS -X POST http://kafka-connect:8083/connectors \
  -H "Content-Type: application/json" \
  -d @"$TEMP_JSON" || true

rm -f "$TEMP_JSON"

echo ""
echo "✅ Debezium connector registration attempted. Current connectors:"
curl -sS http://kafka-connect:8083/connectors || true

