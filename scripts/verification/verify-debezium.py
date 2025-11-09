#!/usr/bin/env python3
"""
Debezium Verification Script
Tests that changes in PostgreSQL are captured by Debezium and sent to Kafka in JSON format
"""

import time
import json
import os
import psycopg2
from kafka import KafkaConsumer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_debezium_capture():
    """Test that Debezium captures PostgreSQL changes and sends them to Kafka."""
    
    logger.info("🔍 Testing Debezium Change Data Capture...")
    
    # Resolve configuration from environment
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS").split(",")

    # Connect to PostgreSQL
    try:
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password
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
            bootstrap_servers=kafka_bootstrap,
            auto_offset_reset='latest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        logger.info("✅ Connected to Kafka")
    except Exception as e:
        logger.error(f"❌ Failed to connect to Kafka: {e}")
        return False
    
    # Make a change to PostgreSQL (use a generic column we know exists)
    try:
        logger.info("📝 Making test change to PostgreSQL...")
        cursor.execute(
            "INSERT INTO business_ncr_ride_bookings (created_at) VALUES (CURRENT_TIMESTAMP)"
        )
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
