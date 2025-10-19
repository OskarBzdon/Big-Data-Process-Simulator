#!/bin/bash

# Register Debezium Connector Script
echo "🔌 Registering Debezium PostgreSQL Connector..."

# Register the connector
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium-connectors/postgres-connector.json

echo ""
echo "✅ Debezium connector registered!"
echo "🔍 Check connector status at: http://localhost:8083/connectors"
