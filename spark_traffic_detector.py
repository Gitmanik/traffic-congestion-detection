import os
import sys
import logging
import pyspark

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, sum as _sum, avg, count,
    min as _min, max as _max, collect_set, lit, when, current_timestamp,
    coalesce, least, greatest
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("TrafficCongestionDetector")


class TrafficCongestionDetector:
    def __init__(self, app_name="TrafficCongestionDetector"):
        # --- Config (env with safe defaults) ---
        self.kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.topic = os.getenv("KAFKA_TOPIC", "traffic-events")
        self.spark_master = os.getenv("SPARK_MASTER", "local[*]")
        self.checkpoint_dir = os.getenv("CHECKPOINT_DIR", "/tmp/spark-checkpoints/congestion")

        # Thresholds
        self.CONGESTION_THRESHOLD = int(os.getenv("CONGESTION_THRESHOLD", "25"))
        self.HIGH_CONGESTION_THRESHOLD = int(os.getenv("HIGH_CONGESTION_THRESHOLD", "40"))
        self.SPEED_THRESHOLD = float(os.getenv("SPEED_THRESHOLD", "30.0"))

        logger.info(f"Kafka: {self.kafka_servers} | Topic: {self.topic}")
        logger.info(
            "Thresholds → congestion:%s, high:%s, speed<%s",
            self.CONGESTION_THRESHOLD, self.HIGH_CONGESTION_THRESHOLD, self.SPEED_THRESHOLD
        )

        # --- Spark session (Kafka connector versioned to match installed PySpark) ---
        spark_ver = pyspark.__version__  # e.g., "3.5.1"
        kafka_pkg = os.getenv(
            "SPARK_KAFKA_PACKAGE",
            f"org.apache.spark:spark-sql-kafka-0-10_2.12:{spark_ver}"
        )

        builder = (
            SparkSession.builder
            .appName(app_name)
            .master(self.spark_master)
            .config("spark.jars.packages", kafka_pkg)
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "8"))
            .config("spark.ui.enabled", "false")
            # Make sure the module is visible in both driver & executors too
            .config("spark.driver.extraJavaOptions", "--add-modules=jdk.management")
            .config("spark.executor.extraJavaOptions", "--add-modules=jdk.management")
        )
        self.spark = builder.getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")

        # --- Schema for incoming JSON messages ---
        # Adjust field names/types here if your producer differs.
        self.traffic_schema = StructType([
            StructField("timestamp", StringType(), True),      # ISO8601 string
            StructField("road_id", StringType(), True),
            StructField("zone_id", StringType(), True),
            StructField("vehicle_count", IntegerType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("speed_avg", DoubleType(), True),
            StructField("sensor_id", StringType(), True)
        ])

    def create_kafka_stream(self):
        """
        Kafka -> JSON parse -> event_timestamp -> filter nulls
        """
        raw = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_servers)
            .option("subscribe", self.topic)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
        )

        parsed = (
            raw.select(from_json(col("value").cast("string"), self.traffic_schema).alias("data"))
               .select("data.*")
               # Be tolerant to milliseconds/no-milliseconds and generic to_timestamp fallback
               .withColumn(
                   "event_timestamp",
                   coalesce(
                       to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
                       to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
                       to_timestamp(col("timestamp"))
                   )
               )
               .filter(col("event_timestamp").isNotNull())
        )
        return parsed

    def detect_congestion_windowed(self, df):
        """
        1-minute tumbling window with 2-minute watermark.
        Aggregates per (road_id, zone_id).
        """
        agg = (
            df.withWatermark("event_timestamp", "2 minutes")
              .groupBy(
                  window(col("event_timestamp"), "1 minute"),
                  col("road_id"),
                  col("zone_id")
              )
              .agg(
                  _sum("vehicle_count").alias("total_vehicles"),
                  avg("vehicle_count").alias("avg_vehicles_per_reading"),
                  count(lit(1)).alias("reading_count"),
                  avg("speed_avg").alias("avg_speed"),
                  _min("speed_avg").alias("min_speed"),
                  _max("speed_avg").alias("max_speed"),
                  collect_set("sensor_id").alias("active_sensors")
              )
        )

        # Bounded congestion score (0..100): combines volume and inverse of speed
        score_expr = (
            (col("total_vehicles") / lit(self.HIGH_CONGESTION_THRESHOLD)) * lit(50.0)
            + ((lit(self.SPEED_THRESHOLD) + lit(20.0) - col("avg_speed")) / lit(50.0)) * lit(50.0)
        )

        result = (
            agg
            .withColumn(
                "congestion_level",
                when(col("total_vehicles") >= self.HIGH_CONGESTION_THRESHOLD, lit("HIGH"))
                .when(col("total_vehicles") >= self.CONGESTION_THRESHOLD, lit("MODERATE"))
                .otherwise(lit("LOW"))
            )
            .withColumn("is_congested", col("total_vehicles") >= self.CONGESTION_THRESHOLD)
            .withColumn("is_slow_traffic", col("avg_speed") < self.SPEED_THRESHOLD)
            .withColumn(
                "congestion_score",
                when(col("avg_speed").isNull(), lit(0.0))
                .otherwise(greatest(lit(0.0), least(lit(100.0), score_expr)))
            )
            .withColumn("alert_timestamp", current_timestamp())
            .select(
                col("window"),
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("road_id"), col("zone_id"),
                col("total_vehicles"), col("avg_speed"), col("min_speed"), col("max_speed"),
                col("reading_count"), col("active_sensors"),
                col("congestion_level"), col("is_congested"), col("is_slow_traffic"),
                col("congestion_score"), col("alert_timestamp")
            )
        )
        return result

    def start(self):
        traffic = self.create_kafka_stream()
        congestion = self.detect_congestion_windowed(traffic)

        query = (
            congestion.filter(col("is_congested"))
                      .writeStream
                      .outputMode("append")            # final results per watermarked window
                      .format("console")
                      .option("truncate", False)
                      .option("numRows", 50)
                      .option("checkpointLocation", self.checkpoint_dir)
                      .start()
        )

        logger.info("Started congestion monitoring (console sink).")
        query.awaitTermination()

    def stop(self):
        try:
            self.spark.stop()
        except Exception:
            pass


if __name__ == "__main__":
    detector = TrafficCongestionDetector()
    try:
        detector.start()
    finally:
        detector.stop()
