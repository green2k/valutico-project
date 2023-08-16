import logging
import os
from kafka import KafkaAdminClient

from pyspark.sql import functions as f
from pavol.constants import CONN_KAFKA, KAFKA_TOPICS
from pavol.consumer_queries.avg_speed_5m import build_query_avg_speed_5m
from pavol.consumer_queries.fastest_vehicle_over_time import build_query_fastest_vehicle_over_time
from pavol.consumer_queries.radius_check import build_query_radius_check
from pavol.consumer_queries.total_distance import build_query_total_distance
from pavol.types import SCHEMA_KAFKA_INPUT_ENRICHED

from pavol.utils import build_spark_session, upsert_topic


logging.basicConfig(level=logging.WARN, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOGLEVEL", "INFO"))

if __name__ == "__main__":
    spark = build_spark_session(app_name="Valutico_Consumer")
    topic = KAFKA_TOPICS["vehicle_location_enriched"]

    # Make sure the topic exists
    upsert_topic(KafkaAdminClient(bootstrap_servers=CONN_KAFKA), topic)

    # Main input stream
    df_input = (
        spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", CONN_KAFKA)
        .option("subscribe", topic.name)
        .load()
        .select([f.from_json(f.col("value").cast("string"), SCHEMA_KAFKA_INPUT_ENRICHED).alias("parsed")])
        .select("parsed.*")
    )

    # Build streaming queries
    query_total_distance = build_query_total_distance(df_input)
    query_avg_speed_5m = build_query_avg_speed_5m(df_input)
    query_max_speed = build_query_fastest_vehicle_over_time(df_input)
    query_latest_position = build_query_radius_check(df_input)


    # Await
    query_total_distance.awaitTermination()
    query_avg_speed_5m.awaitTermination()
    query_max_speed.awaitTermination()
    query_latest_position.awaitTermination()
    logger.info("Done!")
