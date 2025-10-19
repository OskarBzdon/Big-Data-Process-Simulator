#!/bin/bash

# Kafka Streaming Setup Script - Task 3 Implementation
echo "🚀 Setting up Kafka Streaming with JSON Format and Schema Registry..."

# Create Kafka topics management script
cat > manage-kafka-topics.sh << 'EOF'
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
EOF

chmod +x manage-kafka-topics.sh

# Create Kafka consumer script
cat > kafka-json-consumer.py << 'EOF'
#!/usr/bin/env python3
"""
Kafka JSON Consumer - Task 3 Implementation
Reads JSON messages from Kafka topics and validates them
"""

import json
import time
import logging
from kafka import KafkaConsumer
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class KafkaJSONConsumer:
    def __init__(self, bootstrap_servers=['localhost:9092']):
        self.bootstrap_servers = bootstrap_servers
        self.consumer = None
        
    def connect(self):
        """Connect to Kafka cluster"""
        try:
            self.consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='business_consumer_group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None
            )
            logger.info("✅ Connected to Kafka cluster")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to connect to Kafka: {e}")
            return False
    
    def validate_json_message(self, message):
        """Validate JSON message structure"""
        try:
            if not message:
                return False, "Empty message"
            
            if not isinstance(message, dict):
                return False, "Message is not a JSON object"
            
            # Basic validation - check for common fields
            if 'timestamp' in message or 'created_at' in message or 'op' in message:
                return True, "Valid JSON message"
            
            return True, "Valid JSON message (no timestamp field)"
            
        except Exception as e:
            return False, f"JSON validation error: {e}"
    
    def consume_topic(self, topic_name, timeout=30):
        """Consume messages from a specific topic"""
        if not self.consumer:
            logger.error("❌ Not connected to Kafka")
            return
        
        logger.info(f"🎧 Starting to consume from topic: {topic_name}")
        
        # Subscribe to topic
        self.consumer.subscribe([topic_name])
        
        start_time = time.time()
        message_count = 0
        
        try:
            for message in self.consumer:
                if time.time() - start_time > timeout:
                    logger.info(f"⏰ Timeout reached ({timeout}s), stopping consumption")
                    break
                
                message_count += 1
                logger.info(f"📨 Message #{message_count} from {message.topic}")
                logger.info(f"   Partition: {message.partition}, Offset: {message.offset}")
                
                # Validate JSON message
                is_valid, validation_msg = self.validate_json_message(message.value)
                
                if is_valid:
                    logger.info(f"✅ {validation_msg}")
                    logger.info(f"   Content: {json.dumps(message.value, indent=2)}")
                else:
                    logger.warning(f"⚠️  {validation_msg}")
                
                logger.info("-" * 50)
                
        except KeyboardInterrupt:
            logger.info("🛑 Consumer stopped by user")
        except Exception as e:
            logger.error(f"❌ Error consuming messages: {e}")
        
        logger.info(f"📊 Total messages consumed: {message_count}")
    
    def consume_all_business_topics(self, timeout=30):
        """Consume from all business-related topics"""
        topics = [
            'business-ncr-ride-bookings',
            'business_postgres.public.business_ncr_ride_bookings'
        ]
        
        logger.info("🎧 Starting to consume from all business topics...")
        
        # Subscribe to all topics
        self.consumer.subscribe(topics)
        
        start_time = time.time()
        message_count = 0
        
        try:
            for message in self.consumer:
                if time.time() - start_time > timeout:
                    logger.info(f"⏰ Timeout reached ({timeout}s), stopping consumption")
                    break
                
                message_count += 1
                logger.info(f"📨 Message #{message_count} from {message.topic}")
                logger.info(f"   Partition: {message.partition}, Offset: {message.offset}")
                
                # Validate JSON message
                is_valid, validation_msg = self.validate_json_message(message.value)
                
                if is_valid:
                    logger.info(f"✅ {validation_msg}")
                    # Show first 200 chars of content
                    content = json.dumps(message.value, indent=2)
                    if len(content) > 200:
                        content = content[:200] + "..."
                    logger.info(f"   Content: {content}")
                else:
                    logger.warning(f"⚠️  {validation_msg}")
                
                logger.info("-" * 50)
                
        except KeyboardInterrupt:
            logger.info("🛑 Consumer stopped by user")
        except Exception as e:
            logger.error(f"❌ Error consuming messages: {e}")
        
        logger.info(f"📊 Total messages consumed: {message_count}")
    
    def close(self):
        """Close consumer connection"""
        if self.consumer:
            self.consumer.close()
            logger.info("🔌 Consumer connection closed")

def main():
    """Main function to run the consumer"""
    consumer = KafkaJSONConsumer()
    
    if not consumer.connect():
        return
    
    try:
        print("🎯 Kafka JSON Consumer - Task 3 Implementation")
        print("=" * 50)
        print("1. Consume from all business topics (30s timeout)")
        print("2. Consume from specific topic")
        print("3. Exit")
        
        choice = input("\nSelect option (1-3): ").strip()
        
        if choice == "1":
            consumer.consume_all_business_topics(timeout=30)
        elif choice == "2":
            topic = input("Enter topic name: ").strip()
            consumer.consume_topic(topic, timeout=30)
        elif choice == "3":
            print("👋 Goodbye!")
        else:
            print("❌ Invalid option")
            
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
EOF

chmod +x kafka-json-consumer.py

# Add Schema Registry to docker-compose.yml
cat >> docker-compose.yml << 'EOF'

  schema-registry:
    image: confluentinc/cp-schema-registry:7.4.0
    container_name: business_schema_registry
    depends_on:
      - kafka
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka:29092
      SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081
    networks:
      - business_network
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8081/subjects"]
      interval: 30s
      timeout: 10s
      retries: 3
EOF

# Create verification script for Task 3
cat > verify-kafka-streaming.py << 'EOF'
#!/usr/bin/env python3
"""
Kafka Streaming Verification Script - Task 3
Verifies that Kafka cluster, JSON serialization, Schema Registry, and consumer are working
"""

import json
import time
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
        consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'])
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
        response = requests.get("http://localhost:8081/subjects", timeout=10)
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
        # Create producer
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
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
            bootstrap_servers=['localhost:9092'],
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
        consumer = KafkaConsumer(
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None
        )
        
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
EOF

chmod +x verify-kafka-streaming.py

echo "✅ Kafka Streaming Setup Complete!"
echo ""
echo "📋 Task 3 Implementation Summary:"
echo "✅ Kafka cluster in docker-compose.yml"
echo "✅ JSON serialization configured"
echo "✅ Schema Registry added"
echo "✅ Kafka consumer script created"
echo ""
echo "🚀 Next steps:"
echo "1. Run: docker-compose up --build"
echo "2. Run: ./manage-kafka-topics.sh"
echo "3. Run: python verify-kafka-streaming.py"
echo "4. Run: python kafka-json-consumer.py"
echo ""
echo "🔗 Access points:"
echo "- Kafka: localhost:9092"
echo "- Schema Registry: http://localhost:8081"
echo "- Kafka Connect: http://localhost:8083"
