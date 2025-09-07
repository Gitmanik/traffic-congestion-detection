#!/usr/bin/env python3
"""
Test script for Real-Time Traffic Congestion Detection System
"""

import subprocess
import time
import sys
import os
import json
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SystemTester:
    def __init__(self):
        self.kafka_servers = 'localhost:9092'
        self.test_topic = 'traffic-events-test'

    def check_kafka_connectivity(self):
        """Test Kafka connectivity"""
        logger.info("Testing Kafka connectivity...")
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=self.kafka_servers,
                client_id='test_client'
            )
            metadata = admin_client.list_topics()
            logger.info(f"✅ Kafka connection successful. Topics: {list(metadata)}")
            return True
        except Exception as e:
            logger.error(f"❌ Kafka connection failed: {str(e)}")
            return False

    def create_test_topic(self):
        """Create test topic for validation"""
        logger.info(f"Creating test topic: {self.test_topic}")
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=self.kafka_servers)
            topic = NewTopic(
                name=self.test_topic,
                num_partitions=3,
                replication_factor=1
            )
            admin_client.create_topics([topic])
            logger.info(f"✅ Test topic created: {self.test_topic}")
            return True
        except Exception as e:
            logger.info(f"Topic might already exist: {str(e)}")
            return True

    def test_producer(self, num_messages=10):
        """Test traffic data producer"""
        logger.info(f"Testing producer with {num_messages} messages...")
        try:
            producer = KafkaProducer(
                bootstrap_servers=self.kafka_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: str(k).encode('utf-8')
            )

            messages_sent = 0
            for i in range(num_messages):
                test_event = {
                    "timestamp": "2025-08-31T17:24:00.000Z",
                    "road_id": f"TEST_{i % 3:03d}",
                    "zone_id": "TestZone",
                    "vehicle_count": 20 + (i % 30),  # Varying vehicle counts
                    "latitude": 54.3520 + (i * 0.001),
                    "longitude": 18.6466 + (i * 0.001),
                    "speed_avg": 45.0 - (i % 20),
                    "sensor_id": f"test_sensor_{i}"
                }

                producer.send(self.test_topic, key=test_event['road_id'], value=test_event)
                messages_sent += 1

            producer.flush()
            producer.close()
            logger.info(f"✅ Producer test successful. Sent {messages_sent} messages")
            return True

        except Exception as e:
            logger.error(f"❌ Producer test failed: {str(e)}")
            return False

    def test_consumer(self, timeout_seconds=10):
        """Test consumer can read messages"""
        logger.info(f"Testing consumer for {timeout_seconds} seconds...")
        try:
            consumer = KafkaConsumer(
                self.test_topic,
                bootstrap_servers=self.kafka_servers,
                auto_offset_reset='earliest',
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                consumer_timeout_ms=timeout_seconds * 1000
            )

            messages_received = 0
            for message in consumer:
                messages_received += 1
                logger.info(f"Received: {message.value['road_id']} - {message.value['vehicle_count']} vehicles")
                if messages_received >= 5:  # Limit output for testing
                    break

            consumer.close()
            logger.info(f"✅ Consumer test successful. Received {messages_received} messages")
            return messages_received > 0

        except Exception as e:
            logger.error(f"❌ Consumer test failed: {str(e)}")
            return False

    def validate_system_components(self):
        """Validate all system files exist"""
        logger.info("Validating system components...")

        required_files = [
            'kafka_traffic_producer.py',
            'spark_traffic_detector.py', 
            'enhanced_traffic_detector.py',
            'docker-compose.yml',
            'requirements.txt',
            'README.md'
        ]

        missing_files = []
        for file_name in required_files:
            if not os.path.exists(file_name):
                missing_files.append(file_name)

        if missing_files:
            logger.error(f"❌ Missing files: {missing_files}")
            return False
        else:
            logger.info("✅ All system components present")
            return True

    def check_python_dependencies(self):
        """Check if required Python packages can be imported"""
        logger.info("Checking Python dependencies...")

        dependencies = [
            ('kafka', 'kafka-python'),
            ('pyspark', 'pyspark'),
            ('pandas', 'pandas'),
            ('numpy', 'numpy')
        ]

        missing_deps = []
        for module, package in dependencies:
            try:
                __import__(module)
                logger.info(f"✅ {package} available")
            except ImportError:
                missing_deps.append(package)
                logger.error(f"❌ {package} not available")

        if missing_deps:
            logger.error(f"Install missing dependencies: pip install {' '.join(missing_deps)}")
            return False
        return True

    def run_integration_test(self):
        """Run complete integration test"""
        logger.info("="*60)
        logger.info("STARTING SYSTEM INTEGRATION TEST")
        logger.info("="*60)

        # Step 1: Validate components
        if not self.validate_system_components():
            return False

        # Step 2: Check dependencies  
        if not self.check_python_dependencies():
            return False

        # Step 3: Test Kafka connectivity
        if not self.check_kafka_connectivity():
            logger.error("❌ Kafka is not running. Start with: docker-compose up -d")
            return False

        # Step 4: Create test topic
        if not self.create_test_topic():
            return False

        # Step 5: Test producer
        if not self.test_producer(20):
            return False

        # Wait a moment for messages to be available
        time.sleep(2)

        # Step 6: Test consumer
        if not self.test_consumer(15):
            return False

        logger.info("="*60)
        logger.info("✅ ALL TESTS PASSED - SYSTEM READY FOR USE")
        logger.info("="*60)
        logger.info("Next steps:")
        logger.info("1. Start producer: python kafka_traffic_producer.py")
        logger.info("2. Start detector: python spark_traffic_detector.py")
        logger.info("3. Monitor at: http://localhost:8080 (Kafka UI)")
        logger.info("="*60)

        return True

def main():
    """Main test execution"""
    if len(sys.argv) > 1 and sys.argv[1] == '--quick':
        logger.info("Running quick connectivity test only")
        tester = SystemTester()
        if tester.check_kafka_connectivity():
            logger.info("✅ Quick test passed - Kafka is running")
        else:
            logger.error("❌ Quick test failed - Check Kafka setup")
        return

    # Run full integration test
    tester = SystemTester()
    success = tester.run_integration_test()

    if not success:
        logger.error("❌ Integration test failed")
        sys.exit(1)
    else:
        logger.info("✅ Integration test completed successfully")
        sys.exit(0)

if __name__ == "__main__":
    main()
