import logging
import os
import random
from math import radians, sin, cos, sqrt, atan2
from typing import List

import geopy
import geopy.distance
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from pyspark.sql import Column, SparkSession, functions as f
from pyspark.sql.types import FloatType

from pavol.constants import KAFKA_TOPICS
from pavol.types import KafkaTopic, Vehicle


logging.basicConfig(level=logging.WARN, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOGLEVEL", "INFO"))


def calculate_distance(lat1: float, long1: float, lat2: float, long2: float) -> float:
    """
    Returns the distance (km) of input GPS coordinates.
    """
    R = 6371.0  # radius of the Earth in km

    lat1_rad = radians(lat1)
    long1_rad = radians(long1)
    lat2_rad = radians(lat2)
    long2_rad = radians(long2)

    dlon = long2_rad - long1_rad
    dlat = lat2_rad - lat1_rad

    a = sin(dlat / 2)**2 + cos(lat1_rad) * cos(lat2_rad) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c
    return distance


def upsert_topic(admin: KafkaAdminClient, topic: KafkaTopic) -> None:
    """
    Upserts specific topic in a Kafka broker.
    """
    # List all remote topics
    remote_topics: List[str] = admin.list_topics()

    # Optionally create a new topic
    if topic.name not in remote_topics:
        topic = KAFKA_TOPICS["vehicle_location"]
        logger.info("Creating topic: {topic}")
        admin.create_topics([NewTopic(**topic)])


def move_vehicle(vehicle: Vehicle) -> None:
    """
    Simulates the single atomic movement of a vehicle with a resolution of 1 second

    Warning: Not a pure function!
    """
    # Adjust speed & bearing
    vehicle.speed = vehicle.speed + (1 if bool(random.getrandbits(1)) else -1)
    vehicle.speed = max(min(vehicle.speed, 100), 0)
    vehicle.bearing = vehicle.bearing + (3 if bool(random.getrandbits(1)) else -3)
    vehicle.bearing = max(min(vehicle.bearing, 360), 0)

    # Calculate new location
    lat, lon, _ = geopy.distance.geodesic(kilometers=vehicle.speed/3600).destination(geopy.Point(vehicle.lat, vehicle.lon), vehicle.bearing)
    vehicle.lat = lat
    vehicle.lon = lon


def build_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession
        .builder
        .master("local[4]")  # 4 concurrent tasks
        .appName(app_name)
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-core_2.12:2.4.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.access.key", "1234567890")
        .config("spark.hadoop.fs.s3a.secret.key", "1234567890")
        .config("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000")
        .config("spark.sql.shuffle.partitions", "32")
        .getOrCreate()
    )


def kmps_to_kmph(km_per_second: Column) -> Column:
    """
    Calculates km/h from km/s
    """
    return f.col("distance_km") * 3600


udf_calculate_distance = f.udf(calculate_distance, FloatType())
