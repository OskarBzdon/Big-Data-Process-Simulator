#!/bin/bash

echo "📋 Managing Kafka Topics for JSON Streaming..."

# Wait for Kafka to be ready
echo "⏳ Waiting for Kafka to be ready..."
sleep 30

# Create topics for business events
echo "📝 Creating Kafka topics..."

# Business events topics
docker exec business_kafka kafka-topics --create --topic business-ncr-ride-bookings --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# Debezium CDC topics (will be created automatically)
echo "✅ Topics created successfully!"

# List all topics
echo "📋 Current topics:"
docker exec business_kafka kafka-topics --list --bootstrap-server localhost:9092
