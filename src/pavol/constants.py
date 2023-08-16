import os
from typing import Dict

from pavol.types import GpsPoint, KafkaTopic

CONN_KAFKA = "localhost:9092"
KAFKA_TOPICS: Dict[str, KafkaTopic] = {
    "vehicle_location": KafkaTopic(name="vehicle_location"),
    "vehicle_location_enriched": KafkaTopic(name="vehicle_location_enriched"),
}

CONN_REDIS_HOST = "localhost"
CONN_REDIS_PORT = 6379

S3_BUCKET_NAME = "valuticolake"
SPARK_CHECKPOINT_BASE_DIR = os.path.join(".", "volumes", "spark_checkpoints")

GPS_POINT_PARIS = GpsPoint(lat=48.8566, lon=2.3522)

PRODUCER_NUM_VEHICLES = 2
