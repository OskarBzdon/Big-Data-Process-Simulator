#!/bin/bash

# Debezium Setup Script - Lightweight Implementation
echo "🚀 Setting up Debezium for PostgreSQL Change Data Capture..."

# Create init-scripts directory
mkdir -p init-scripts

# Create PostgreSQL replication setup script
cat > init-scripts/01-setup-replication.sql << 'EOF'
-- Enable logical replication for Debezium
ALTER SYSTEM SET wal_level = logical;
ALTER SYSTEM SET max_wal_senders = 10;
ALTER SYSTEM SET max_replication_slots = 10;
ALTER SYSTEM SET max_connections = 200;

-- Create replication user for Debezium
CREATE USER debezium_user WITH REPLICATION LOGIN PASSWORD 'debezium_password';

-- Grant necessary permissions
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium_user;
GRANT USAGE ON SCHEMA public TO debezium_user;
EOF

# Create Debezium connector configuration
mkdir -p debezium-connectors
cat > debezium-connectors/postgres-connector.json << 'EOF'
{
  "name": "postgres-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "debezium_user",
    "database.password": "debezium_password",
    "database.dbname": "business_db",
    "database.server.name": "business_postgres",
    "table.include.list": "public.business_.*",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter.schemas.enable": "false",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "false",
    "transforms.unwrap.delete.handling.mode": "drop",
    "transforms.unwrap.add.fields": "op,ts_ms"
  }
}
EOF

# Update docker-compose.yml to add Kafka services
cat >> docker-compose.yml << 'EOF'

  zookeeper:
    image: confluentinc/cp-zookeeper:7.4.0
    container_name: business_zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    networks:
      - business_network
    restart: unless-stopped

  kafka:
    image: confluentinc/cp-kafka:7.4.0
    container_name: business_kafka
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092,PLAINTEXT_INTERNAL://kafka:29092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_INTERNAL:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT_INTERNAL
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'true'
    networks:
      - business_network
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "kafka-broker-api-versions", "--bootstrap-server", "localhost:9092"]
      interval: 30s
      timeout: 10s
      retries: 3

  kafka-connect:
    image: confluentinc/cp-kafka-connect:7.4.0
    container_name: business_kafka_connect
    depends_on:
      - kafka
    ports:
      - "8083:8083"
    environment:
      CONNECT_BOOTSTRAP_SERVERS: kafka:29092
      CONNECT_GROUP_ID: 1
      CONNECT_CONFIG_STORAGE_TOPIC: my_connect_configs
      CONNECT_OFFSET_STORAGE_TOPIC: my_connect_offsets
      CONNECT_STATUS_STORAGE_TOPIC: my_connect_statuses
      CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR: 1
      CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR: 1
      CONNECT_STATUS_STORAGE_REPLICATION_FACTOR: 1
      CONNECT_KEY_CONVERTER: org.apache.kafka.connect.json.JsonConverter
      CONNECT_VALUE_CONVERTER: org.apache.kafka.connect.json.JsonConverter
      CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE: false
      CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE: false
      CONNECT_PLUGIN_PATH: "/usr/share/java,/usr/share/confluent-hub-components"
      CONNECT_LOG4J_LOGGERS: org.apache.zookeeper=ERROR,org.I0Itec.zkclient=ERROR,org.reflections=ERROR
    command:
      - bash
      - -c
      - |
        echo "Installing Debezium PostgreSQL connector..."
        confluent-hub install --no-prompt debezium/debezium-connector-postgresql:2.4.0
        echo "Starting Kafka Connect..."
        /etc/confluent/docker/run
    networks:
      - business_network
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8083/connectors"]
      interval: 30s
      timeout: 10s
      retries: 3
EOF

# Create verification script
cat > verify-debezium.py << 'EOF'
#!/usr/bin/env python3
"""
Debezium Verification Script
Tests that changes in PostgreSQL are captured by Debezium and sent to Kafka in JSON format
"""

import time
import json
import psycopg2
from kafka import KafkaConsumer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_debezium_capture():
    """Test that Debezium captures PostgreSQL changes and sends them to Kafka."""
    
    logger.info("🔍 Testing Debezium Change Data Capture...")
    
    # Connect to PostgreSQL
    try:
        conn = psycopg2.connect(
            host="localhost",
            port="5432",
            database="business_db",
            user="postgres",
            password="password"
        )
        conn.autocommit = True
        cursor = conn.cursor()
        logger.info("✅ Connected to PostgreSQL")
    except Exception as e:
        logger.error(f"❌ Failed to connect to PostgreSQL: {e}")
        return False
    
    # Connect to Kafka
    try:
        consumer = KafkaConsumer(
            'business_postgres.public.business_ncr_ride_bookings',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='latest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        logger.info("✅ Connected to Kafka")
    except Exception as e:
        logger.error(f"❌ Failed to connect to Kafka: {e}")
        return False
    
    # Make a change to PostgreSQL
    try:
        logger.info("📝 Making test change to PostgreSQL...")
        cursor.execute("""
            INSERT INTO business_ncr_ride_bookings (ncr_ride_bookings_csv_date, ncr_ride_bookings_csv_time, ncr_ride_bookings_csv_booking_id, ncr_ride_bookings_csv_booking_status, ncr_ride_bookings_csv_customer_id, ncr_ride_bookings_csv_vehicle_type, ncr_ride_bookings_csv_pickup_location, ncr_ride_bookings_csv_drop_location)
            VALUES ('2024-01-01', '2024-01-01 12:00:00', 'TEST999', 'Completed', 'TEST_CUSTOMER', 'Test Vehicle', 'Test Pickup', 'Test Drop')
        """)
        logger.info("✅ Test record inserted")
    except Exception as e:
        logger.error(f"❌ Failed to insert test record: {e}")
        return False
    
    # Wait for Debezium to capture the change
    logger.info("⏳ Waiting for Debezium to capture change...")
    timeout = 30
    start_time = time.time()
    
    try:
        for message in consumer:
            if time.time() - start_time > timeout:
                logger.error("❌ Timeout waiting for Debezium message")
                return False
            
            if message.value:
                logger.info("✅ Received Debezium message:")
                logger.info(f"   Topic: {message.topic}")
                logger.info(f"   Message: {json.dumps(message.value, indent=2)}")
                
                # Verify it's JSON format
                if isinstance(message.value, dict):
                    logger.info("✅ Message is in JSON format")
                    return True
                else:
                    logger.error("❌ Message is not in JSON format")
                    return False
                    
    except Exception as e:
        logger.error(f"❌ Error reading from Kafka: {e}")
        return False
    finally:
        cursor.close()
        conn.close()
        consumer.close()

if __name__ == "__main__":
    success = test_debezium_capture()
    if success:
        print("\n🎉 Debezium Change Data Capture is working correctly!")
    else:
        print("\n❌ Debezium Change Data Capture test failed!")
        exit(1)
EOF

chmod +x verify-debezium.py

echo "✅ Debezium setup complete!"
echo ""
echo "📋 Next steps:"
echo "1. Run: docker-compose up --build"
echo "2. Wait for all services to start (about 2-3 minutes)"
echo "3. Run: python verify-debezium.py"
echo ""
echo "🔗 Access points:"
echo "- Kafka Connect: http://localhost:8083"
echo "- pgAdmin: http://localhost:8080"
echo "- PostgreSQL: localhost:5432"
