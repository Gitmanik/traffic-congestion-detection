import json
import time
import random
import os
from datetime import datetime, timedelta
from kafka import KafkaProducer
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TrafficDataProducer:
    def __init__(self, bootstrap_servers=None, topic=None):
        """
        Initialize Kafka producer for traffic sensor data
        """
        # Read configuration from environment variables
        self.bootstrap_servers = bootstrap_servers or os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(',')
        self.topic = topic or os.getenv('KAFKA_TOPIC', 'traffic-events')

        # Streaming configuration from environment
        self.duration_minutes = int(os.getenv('STREAMING_DURATION_MINUTES', '60'))
        self.interval_seconds = int(os.getenv('STREAMING_INTERVAL_SECONDS', '3'))

        logger.info(f"Initializing producer with servers: {self.bootstrap_servers}")
        logger.info(f"Topic: {self.topic}, Duration: {self.duration_minutes}min, Interval: {self.interval_seconds}s")

        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8'),
            retries=5,
            retry_backoff_ms=1000,
            request_timeout_ms=30000
        )

        # Define road segments for simulation (Gdańsk area)
        self.road_segments = [
            {'road_id': 'A001', 'zone': 'Downtown', 'lat': 54.3520, 'lon': 18.6466},
            {'road_id': 'A002', 'zone': 'Old_Town', 'lat': 54.3480, 'lon': 18.6520},
            {'road_id': 'A003', 'zone': 'Shipyard', 'lat': 54.3600, 'lon': 18.6300},
            {'road_id': 'B001', 'zone': 'University', 'lat': 54.3700, 'lon': 18.6100},
            {'road_id': 'B002', 'zone': 'Airport', 'lat': 54.3800, 'lon': 18.4800},
            {'road_id': 'C001', 'zone': 'Port', 'lat': 54.3400, 'lon': 18.6700},
            {'road_id': 'C002', 'zone': 'Beach', 'lat': 54.4000, 'lon': 18.5500},
            {'road_id': 'D001', 'zone': 'Mall', 'lat': 54.3300, 'lon': 18.6200}
        ]

    def generate_traffic_event(self):
        """
        Generate a single traffic sensor event based on Uber Movement methodology
        """
        road = random.choice(self.road_segments)
        current_time = datetime.now()

        # Simulate realistic traffic patterns with time-based variations
        hour = current_time.hour

        # Rush hour traffic (7-9 AM, 5-7 PM)
        if (7 <= hour <= 9) or (17 <= hour <= 19):
            base_count = random.randint(15, 35)  # Higher traffic
            speed_factor = random.uniform(0.3, 0.7)  # Slower speeds
        # Regular hours
        elif 9 <= hour <= 17:
            base_count = random.randint(8, 20)  # Moderate traffic
            speed_factor = random.uniform(0.7, 1.0)  # Normal speeds
        # Night/early morning
        else:
            base_count = random.randint(1, 8)   # Light traffic
            speed_factor = random.uniform(0.8, 1.2)  # Faster speeds

        # Add random variation
        vehicle_count = max(0, base_count + random.randint(-5, 8))
        speed_limit = 50  # km/h
        avg_speed = speed_limit * speed_factor + random.uniform(-5, 5)
        avg_speed = max(5, min(avg_speed, speed_limit + 10))  # Realistic bounds

        traffic_event = {
            "timestamp": current_time.isoformat() + "Z",
            "road_id": road['road_id'],
            "zone_id": road['zone'],
            "vehicle_count": vehicle_count,
            "latitude": road['lat'] + random.uniform(-0.001, 0.001),
            "longitude": road['lon'] + random.uniform(-0.001, 0.001),
            "speed_avg": round(avg_speed, 2),
            "sensor_id": f"sensor_{road['road_id']}_01"
        }

        return traffic_event

    def start_streaming(self):
        """
        Start streaming traffic events to Kafka topic
        """
        end_time = datetime.now() + timedelta(minutes=self.duration_minutes)
        event_count = 0

        logger.info(f"Starting traffic data streaming to topic: {self.topic}")
        logger.info(f"Duration: {self.duration_minutes} minutes, Interval: {self.interval_seconds} seconds")
        logger.info("Press Ctrl+C to stop streaming...")

        try:
            while datetime.now() < end_time:
                # Generate multiple events per interval to simulate multiple sensors
                events_per_batch = random.randint(3, 8)

                for _ in range(events_per_batch):
                    event = self.generate_traffic_event()

                    # Send event to Kafka using road_id as key for partitioning
                    future = self.producer.send(
                        self.topic,
                        key=event['road_id'],
                        value=event
                    )

                    event_count += 1

                    if event_count % 50 == 0:
                        logger.info(f"Sent {event_count} events. Latest: {event['road_id']} - {event['vehicle_count']} vehicles")

                # Flush producer to ensure delivery
                self.producer.flush()

                # Wait for next batch
                time.sleep(self.interval_seconds)

        except KeyboardInterrupt:
            logger.info("Streaming interrupted by user")
        except Exception as e:
            logger.error(f"Error in streaming: {str(e)}")
            raise
        finally:
            self.producer.close()
            logger.info(f"Streaming completed. Total events sent: {event_count}")

if __name__ == "__main__":
    # Initialize and start the traffic data producer
    producer = TrafficDataProducer()
    producer.start_streaming()
