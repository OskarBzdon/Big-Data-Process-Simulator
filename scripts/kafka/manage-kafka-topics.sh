#!/bin/bash

set -euo pipefail

echo "📝 Creating Kafka topics..."

# Create business events topic (idempotent)
kafka-topics \
  --create \
  --if-not-exists \
  --topic business-ncr-ride-bookings \
  --bootstrap-server kafka:29092 \
  --partitions 3 \
  --replication-factor 1 || true

echo "✅ Topics created successfully!"

echo "📋 Current topics:"
kafka-topics --list --bootstrap-server kafka:29092

