#!/usr/bin/env python3
"""
Kafka JSON Consumer - Task 3 Implementation
Reads JSON messages from Kafka topics and validates them
"""

import json
import os
import time
import logging
import argparse
from kafka import KafkaConsumer
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class KafkaJSONConsumer:
    def __init__(self, bootstrap_servers=None):
        # Resolve bootstrap servers from env or default to localhost:9092
        env_bootstrap = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
        if bootstrap_servers is None:
            bootstrap_servers = env_bootstrap.split(',') if env_bootstrap else ['localhost:9092']
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
    parser = argparse.ArgumentParser(description="Kafka JSON Consumer")
    parser.add_argument("--non-interactive", "-n", action="store_true", help="Run without prompts")
    parser.add_argument("--topic", "-t", type=str, default=None, help="Topic name to consume (non-interactive)")
    parser.add_argument("--all-topics", "-a", action="store_true", help="Consume all business topics (non-interactive)")
    parser.add_argument("--timeout", "-T", type=int, default=30, help="Consume timeout in seconds")
    args = parser.parse_args()

    consumer = KafkaJSONConsumer()

    if not consumer.connect():
        return

    try:
        if args.non_interactive:
            # Parameter-driven mode, no menu printed
            if args.topic:
                consumer.consume_topic(args.topic, timeout=args.timeout)
            else:
                consumer.consume_all_business_topics(timeout=args.timeout)
            return

        # Interactive mode
        print("🎯 Kafka JSON Consumer - Task 3 Implementation")
        print("=" * 50)
        print("1. Consume from all business topics (30s timeout)")
        print("2. Consume from specific topic")
        print("3. Exit")
        print("")

        choice = input("Select option (1-3): ").strip()

        if choice == "1":
            consumer.consume_all_business_topics(timeout=args.timeout)
        elif choice == "2":
            topic = input("Enter topic name: ").strip()
            consumer.consume_topic(topic, timeout=args.timeout)
        elif choice == "3":
            print("👋 Goodbye!")
        else:
            print("❌ Invalid option")

    finally:
        consumer.close()

if __name__ == "__main__":
    main()
