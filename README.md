# Real-Time Traffic Congestion Detection System

This project implements a real-time traffic congestion detection system using Apache Kafka and Apache Spark Structured Streaming, based on Uber Movement's GPS trace methodology.

## Architecture Overview

The system consists of three main components:

1. **Kafka Producer** (`kafka_traffic_producer.py`): Simulates traffic sensor data streaming
2. **Spark Structured Streaming** (`spark_traffic_detector.py`): Real-time congestion detection
3. **Infrastructure** (Docker Compose): Kafka, Zookeeper, and monitoring tools

## Data Model

Based on Uber Movement methodology, each traffic event contains:
- `timestamp`: Event time in ISO format
- `road_id`: Unique identifier for road segment  
- `zone_id`: Geographic zone (e.g., Downtown, Airport)
- `vehicle_count`: Number of vehicles detected per reading
- `latitude/longitude`: GPS coordinates
- `speed_avg`: Average speed of vehicles
- `sensor_id`: Unique sensor identifier

## Prerequisites

1. **Docker & Docker Compose** (for Kafka infrastructure)
2. **Python 3.8+**
3. **Java 8 or 11** (required by Spark)

## Quick Start

### 1. Start Kafka Infrastructure

```bash
# Start Kafka, Zookeeper, and monitoring tools
docker-compose up -d

# Verify services are running
docker-compose ps

# Check Kafka is ready
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

### 2. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 3. Create Kafka Topic

```bash
# Create traffic-events topic with 8 partitions for parallel processing
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic traffic-events --partitions 8 --replication-factor 1
```

### 4. Run the Traffic Data Producer

```bash
# Start streaming simulated traffic data
python kafka_traffic_producer.py
```

This will:
- Generate realistic traffic patterns based on time of day
- Stream events every 3 seconds
- Simulate rush hour congestion (7-9 AM, 5-7 PM)
- Use road segments in Gdańsk area

### 5. Run the Spark Congestion Detector

```bash
# Start real-time congestion detection (in separate terminal)
python spark_traffic_detector.py
```

This will:
- Process streaming data in 1-minute windows
- Detect congested segments (>25 vehicles/minute)
- Identify rising trends using sliding windows  
- Output alerts and summaries to console

## System Features

### Congestion Detection Logic

- **Moderate Congestion**: ≥25 vehicles per minute
- **High Congestion**: ≥40 vehicles per minute  
- **Slow Traffic**: Average speed <30 km/h
- **Congestion Score**: Composite metric based on vehicle count and speed

### Window Operations

1. **Tumbling Windows** (1 minute): Primary congestion detection
2. **Sliding Windows** (3 minutes, 1-minute slide): Trend analysis
3. **Watermarking**: 2-minute tolerance for late-arriving data

### Real-time Outputs

1. **Congestion Alerts**: Only congested segments
2. **Trend Analysis**: Rising congestion patterns
3. **Traffic Summary**: Overall road-level statistics

## Monitoring

Access Kafka UI at http://localhost:8080 to monitor:
- Topic partitions and message flow
- Consumer lag and processing rates
- Message throughput and retention

## Configuration

### Traffic Thresholds (in `spark_traffic_detector.py`)

```python
CONGESTION_THRESHOLD = 25        # vehicles per minute
HIGH_CONGESTION_THRESHOLD = 40   # vehicles per minute  
SPEED_THRESHOLD = 30.0           # km/h
```

### Streaming Parameters

```python
# Producer interval
interval_seconds = 3             # seconds between events

# Spark processing trigger  
processingTime = '30 seconds'    # processing interval

# Watermark tolerance
watermark = "2 minutes"          # late data tolerance
```

## Extending the System

### Add New Road Segments

Modify `road_segments` in `kafka_traffic_producer.py`:

```python
self.road_segments.append({
    'road_id': 'NEW001', 
    'zone': 'NewArea', 
    'lat': 54.xxx, 
    'lon': 18.xxx
})
```

### Custom Congestion Logic

Implement custom detection in `detect_congestion_windowed()`:

```python
.withColumn("custom_alert", 
    when((col("total_vehicles") > threshold) & 
         (col("avg_speed") < speed_limit), "ALERT")
    .otherwise("NORMAL")
)
```

### Output to External Systems

Replace console output with external sinks:

```python
# Write to Parquet files
.writeStream
.format("parquet")
.option("path", "/path/to/output")
.option("checkpointLocation", "/path/to/checkpoint")
.start()

# Write to Database
.writeStream
.foreach(lambda row: database.insert(row))
.start()
```

## Troubleshooting

### Common Issues

1. **Kafka Connection Errors**: Ensure Docker services are running
2. **Spark Dependencies**: Verify Java and Spark packages are installed
3. **Memory Issues**: Adjust Spark driver memory: `--driver-memory 2g`
4. **Late Data**: Increase watermark tolerance if needed

### Debugging

Enable detailed logging:

```python
logging.basicConfig(level=logging.DEBUG)
spark.sparkContext.setLogLevel("DEBUG")
```

## Performance Tuning

### Kafka Optimization

- Increase topic partitions for higher parallelism
- Adjust `batch.size` and `linger.ms` for throughput
- Monitor consumer lag in Kafka UI

### Spark Optimization

- Set optimal trigger intervals based on latency requirements
- Use appropriate watermark values for your use case
- Enable adaptive query execution for better performance

## Data Pipeline Flow

```
Traffic Sensors → Kafka Producer → Kafka Topic → Spark Streaming → Aggregation → Congestion Detection → Alerts/Dashboard
```

This implementation provides a complete real-time traffic monitoring solution suitable for smart city applications, traffic management systems, and urban planning analysis.
