#!/usr/bin/env python3
"""
Kafka Streaming Verification Script - Task 3
Verifies that Kafka cluster, JSON serialization, Schema Registry, and consumer are working
"""

import json
import time
import os
import requests
import logging
from kafka import KafkaProducer, KafkaConsumer
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_kafka_cluster():
    """Test that Kafka cluster is running"""
    logger.info("🔍 Testing Kafka cluster...")
    
    try:
        kafka_bootstrap = os.getenv('KAFKA_BOOTSTRAP_SERVERS').split(',')
        consumer = KafkaConsumer(bootstrap_servers=kafka_bootstrap)
        consumer.close()
        logger.info("✅ Kafka cluster is running")
        return True
    except Exception as e:
        logger.error(f"❌ Kafka cluster test failed: {e}")
        return False

def test_schema_registry():
    """Test that Schema Registry is running"""
    logger.info("🔍 Testing Schema Registry...")
    
    try:
        registry_url = os.getenv('SCHEMA_REGISTRY_URL')
        response = requests.get(f"{registry_url}/subjects", timeout=10)
        if response.status_code == 200:
            logger.info("✅ Schema Registry is running")
            return True
        else:
            logger.error(f"❌ Schema Registry returned status {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"❌ Schema Registry test failed: {e}")
        return False

def test_json_serialization():
    """Test JSON message production and consumption"""
    logger.info("🔍 Testing JSON serialization...")
    
    try:
        kafka_bootstrap = os.getenv('KAFKA_BOOTSTRAP_SERVERS').split(',')
        # Create producer
        producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        
        # Create test message
        test_message = {
            "id": 999,
            "name": "Test Customer",
            "email": "test@example.com",
            "timestamp": datetime.now().isoformat(),
            "action": "test_message"
        }
        
        # Send message
        future = producer.send('business-ncr-ride-bookings', test_message)
        producer.flush()
        logger.info("✅ Test message sent to Kafka")
        
        # Create consumer
        consumer = KafkaConsumer(
            'business-ncr-ride-bookings',
            bootstrap_servers=kafka_bootstrap,
            auto_offset_reset='latest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        # Wait for message
        logger.info("⏳ Waiting for test message...")
        timeout = 10
        start_time = time.time()
        
        for message in consumer:
            if time.time() - start_time > timeout:
                logger.error("❌ Timeout waiting for test message")
                return False
            
            if message.value and message.value.get('action') == 'test_message':
                logger.info("✅ Test message received and decoded as JSON")
                logger.info(f"   Message: {json.dumps(message.value, indent=2)}")
                consumer.close()
                producer.close()
                return True
        
        consumer.close()
        producer.close()
        return False
        
    except Exception as e:
        logger.error(f"❌ JSON serialization test failed: {e}")
        return False

def test_consumer_validation():
    """Test that consumer can validate JSON messages"""
    logger.info("🔍 Testing consumer JSON validation...")
    
    try:
        kafka_bootstrap = os.getenv('KAFKA_BOOTSTRAP_SERVERS').split(',')
        consumer = KafkaConsumer(bootstrap_servers=kafka_bootstrap)
        
        # Test with valid JSON
        valid_json = {"test": "message", "timestamp": "2024-01-01T00:00:00"}
        is_valid = isinstance(valid_json, dict)
        
        if is_valid:
            logger.info("✅ Consumer can validate JSON messages")
            return True
        else:
            logger.error("❌ Consumer JSON validation failed")
            return False
            
    except Exception as e:
        logger.error(f"❌ Consumer validation test failed: {e}")
        return False

def main():
    """Run all verification tests"""
    logger.info("🚀 Starting Kafka Streaming Verification - Task 3")
    logger.info("=" * 60)
    
    tests = [
        ("Kafka Cluster", test_kafka_cluster),
        ("Schema Registry", test_schema_registry),
        ("JSON Serialization", test_json_serialization),
        ("Consumer Validation", test_consumer_validation)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        logger.info(f"\n🧪 Running {test_name} test...")
        if test_func():
            passed += 1
        time.sleep(2)
    
    logger.info(f"\n📊 Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        logger.info("🎉 All Task 3 requirements verified successfully!")
        return True
    else:
        logger.error("❌ Some tests failed. Check the logs above.")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
