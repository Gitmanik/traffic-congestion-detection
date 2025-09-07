import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import sys

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TrafficCongestionDetector:
    def __init__(self, app_name="TrafficCongestionDetector"):
        """
        Initialize Spark Structured Streaming application for traffic congestion detection
        """
        # Read configuration from environment variables
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.topic = os.getenv('KAFKA_TOPIC', 'traffic-events')

        logger.info(f"Kafka servers: {self.kafka_servers}")
        logger.info(f"Topic: {self.topic}")

        # Create Spark session with Kafka dependencies
        spark_builder = SparkSession.builder \
            .appName(app_name) \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.streaming.checkpointLocation", "/app/checkpoint") \
            .config("spark.sql.warehouse.dir", "/app/spark-warehouse")

        # Set master for local development vs cluster deployment
        spark_master = os.getenv('SPARK_MASTER', 'local[*]')
        if spark_master != 'local[*]':
            spark_builder = spark_builder.master(spark_master)

        self.spark = spark_builder.getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")

        # Define schema for traffic events based on our producer
        self.traffic_schema = StructType([
            StructField("timestamp", StringType(), True),
            StructField("road_id", StringType(), True),
            StructField("zone_id", StringType(), True),
            StructField("vehicle_count", IntegerType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("speed_avg", DoubleType(), True),
            StructField("sensor_id", StringType(), True)
        ])

        # Congestion thresholds (configurable via environment)
        self.CONGESTION_THRESHOLD = int(os.getenv('CONGESTION_THRESHOLD', '25'))
        self.HIGH_CONGESTION_THRESHOLD = int(os.getenv('HIGH_CONGESTION_THRESHOLD', '40'))
        self.SPEED_THRESHOLD = float(os.getenv('SPEED_THRESHOLD', '30.0'))

    def create_kafka_stream(self):
        """
        Create Kafka streaming DataFrame
        """
        logger.info(f"Connecting to Kafka: {self.kafka_servers}, topic: {self.topic}")

        kafka_stream = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_servers) \
            .option("subscribe", self.topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()

        # Parse JSON messages
        parsed_stream = kafka_stream \
            .select(from_json(col("value").cast("string"), self.traffic_schema).alias("data")) \
            .select("data.*") \
            .withColumn("event_timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))

        return parsed_stream

    def detect_congestion_windowed(self, stream_df):
        """
        Detect traffic congestion using window-based aggregation
        """
        logger.info("Setting up windowed aggregation for congestion detection")

        # Apply watermark for late data handling (2 minutes)
        watermarked_stream = stream_df \
            .withWatermark("event_timestamp", "2 minutes")

        # Window aggregation: 1-minute tumbling windows
        windowed_aggregation = watermarked_stream \
            .groupBy(
                window(col("event_timestamp"), "1 minute"),
                col("road_id"),
                col("zone_id")
            ) \
            .agg(
                sum("vehicle_count").alias("total_vehicles"),
                avg("vehicle_count").alias("avg_vehicles_per_reading"), 
                count("*").alias("reading_count"),
                avg("speed_avg").alias("avg_speed"),
                min("speed_avg").alias("min_speed"),
                max("speed_avg").alias("max_speed"),
                collect_list("sensor_id").alias("active_sensors")
            )

        # Detect congestion based on thresholds
        congestion_detected = windowed_aggregation \
            .withColumn("congestion_level", 
                when(col("total_vehicles") >= self.HIGH_CONGESTION_THRESHOLD, "HIGH")
                .when(col("total_vehicles") >= self.CONGESTION_THRESHOLD, "MODERATE") 
                .otherwise("LOW")
            ) \
            .withColumn("is_congested", col("total_vehicles") >= self.CONGESTION_THRESHOLD) \
            .withColumn("is_slow_traffic", col("avg_speed") < self.SPEED_THRESHOLD) \
            .withColumn("congestion_score", 
                least(lit(100), 
                    (col("total_vehicles") / self.HIGH_CONGESTION_THRESHOLD * 50) + 
                    ((self.SPEED_THRESHOLD + 20 - col("avg_speed")) / 50 * 50)
                )
            ) \
            .withColumn("alert_timestamp", current_timestamp())

        return congestion_detected

    def detect_rising_trends(self, congestion_stream):
        """
        Detect rising congestion trends using sliding windows
        """
        logger.info("Setting up trend detection with sliding windows")

        # 3-minute sliding window with 1-minute slide interval
        trending_analysis = congestion_stream \
            .groupBy(
                window(col("event_timestamp"), "3 minutes", "1 minute"),
                col("road_id"),
                col("zone_id")
            ) \
            .agg(
                sum("total_vehicles").alias("trend_total_vehicles"),
                avg("total_vehicles").alias("trend_avg_vehicles"),
                count("*").alias("trend_window_count"),
                avg("avg_speed").alias("trend_avg_speed"),
                stddev("total_vehicles").alias("traffic_volatility")
            ) \
            .withColumn("trend_status",
                when(col("trend_avg_vehicles") > self.CONGESTION_THRESHOLD * 0.8, "RISING")
                .when(col("trend_avg_vehicles") < self.CONGESTION_THRESHOLD * 0.4, "DECREASING")
                .otherwise("STABLE")
            ) \
            .withColumn("volatility_level",
                when(col("traffic_volatility") > 10, "HIGH_VOLATILITY")
                .when(col("traffic_volatility") > 5, "MODERATE_VOLATILITY")
                .otherwise("LOW_VOLATILITY")
            )

        return trending_analysis

    def start_congestion_monitoring(self):
        """
        Start real-time traffic congestion monitoring
        """
        try:
            # Create streaming DataFrame
            traffic_stream = self.create_kafka_stream()

            # Detect congestion with windowed aggregation
            congestion_stream = self.detect_congestion_windowed(traffic_stream)

            # Start congestion detection query
            congestion_query = congestion_stream \
                .filter(col("is_congested") == True) \
                .writeStream \
                .outputMode("update") \
                .format("console") \
                .option("truncate", False) \
                .option("numRows", 20) \
                .trigger(processingTime='30 seconds') \
                .queryName("congestion_detection") \
                .start()

            # Detect rising trends  
            trend_stream = self.detect_rising_trends(traffic_stream.withWatermark("event_timestamp", "2 minutes"))

            # Start trend detection query
            trend_query = trend_stream \
                .filter(col("trend_status") == "RISING") \
                .writeStream \
                .outputMode("update") \
                .format("console") \
                .option("truncate", False) \
                .option("numRows", 10) \
                .trigger(processingTime='1 minute') \
                .queryName("trend_detection") \
                .start()

            # Overall traffic summary query
            summary_query = traffic_stream \
                .withWatermark("event_timestamp", "2 minutes") \
                .groupBy(
                    window(col("event_timestamp"), "2 minutes", "1 minute"),
                    col("road_id")
                ) \
                .agg(
                    sum("vehicle_count").alias("total_vehicles"),
                    avg("speed_avg").alias("avg_speed"),
                    count("*").alias("sensor_readings")
                ) \
                .writeStream \
                .outputMode("update") \
                .format("console") \
                .option("truncate", False) \
                .trigger(processingTime='1 minute') \
                .queryName("traffic_summary") \
                .start()

            logger.info("Started traffic congestion monitoring...")
            logger.info("Congestion threshold: {} vehicles per minute".format(self.CONGESTION_THRESHOLD))
            logger.info("High congestion threshold: {} vehicles per minute".format(self.HIGH_CONGESTION_THRESHOLD))
            logger.info("Speed threshold: {} km/h".format(self.SPEED_THRESHOLD))

            # Wait for all queries to finish
            congestion_query.awaitTermination()

        except Exception as e:
            logger.error(f"Error in traffic monitoring: {str(e)}")
            raise
        finally:
            self.spark.stop()

if __name__ == "__main__":
    detector = TrafficCongestionDetector()
    detector.start_congestion_monitoring()
