import logging
import os
import posixpath

from pyspark.sql import functions as f
from pavol.constants import CONN_KAFKA, KAFKA_TOPICS, S3_BUCKET_NAME, SPARK_CHECKPOINT_BASE_DIR
from pavol.types import SCHEMA_KAFKA_INPUT

from pavol.utils import build_spark_session, kmps_to_kmph, udf_calculate_distance


logging.basicConfig(level=logging.WARN, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOGLEVEL", "INFO"))

spark = build_spark_session(app_name="Valutico_Enricher")
topic_input = KAFKA_TOPICS["vehicle_location"]
topic_output = KAFKA_TOPICS["vehicle_location_enriched"]

if __name__ == "__main__":
    # Main input stream
    df_input = (
        spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", CONN_KAFKA)
        .option("subscribe", topic_input.name)
        .load()
        .select(f.from_json(f.col("value").cast("string"), SCHEMA_KAFKA_INPUT).alias("parsed"))
        .select([
            f.col("parsed.vehicle_id").alias("vehicle_id"),
            f.col("parsed.latitude").alias("latitude"),
            f.col("parsed.longitude").alias("longitude"),
            f.col("parsed.timestamp").alias("timestamp"),
        ])
        .filter(  # TODO: Write invalid records to a different Kafka topic & Delta table
            (f.col("vehicle_id").isNotNull())
            &
            (f.col("latitude").isNotNull())
            &
            (f.col("longitude").isNotNull())
            &
            (f.col("timestamp").isNotNull())
        )
        .withWatermark("timestamp", "10 seconds")
    )

    # Forked main input stream
    df_previous = (
        df_input
        .select([f.col(cname).alias(f"p_{cname}") for cname in df_input.columns])
        .withWatermark("p_timestamp", "10 seconds")
    )

    # Join the input stream to the forked input stream (with respect to watermarking).
    df_joined = (
        df_input.join(
            df_previous,
            (
                (f.col("vehicle_id") == f.col("p_vehicle_id"))
                &
                (f.col("timestamp") == (f.col("p_timestamp") + f.expr("INTERVAL 1 SECOND")))
            ),
            "inner"
        )
    )

    # Calculate the speed & travelled distance (enrichment)
    df_enriched = (
        df_joined
        .withColumn("distance_km", udf_calculate_distance(f.col("latitude"), f.col("longitude"), f.col("p_latitude"), f.col("p_longitude")))
        .withColumn("speed", kmps_to_kmph(f.col("distance_km")))
        .select("vehicle_id", "latitude", "longitude", "timestamp", "distance_km", "speed")
    )

    # Write to Databricks Delta table
    stream_s3 = (
        df_enriched
        .writeStream
        .format("delta")
        .option("checkpointLocation", os.path.join(SPARK_CHECKPOINT_BASE_DIR, "vehicles_enrichment_s3"))
        .option("path", posixpath.join("s3a://", S3_BUCKET_NAME, "stage/vehicles_gps"))
        .outputMode("append")
        .start()
    )

    # Write to Kafka
    stream_kafka = (
        df_enriched
        .withColumn("key", f.col("vehicle_id"))
        .withColumn("value", f.to_json(f.struct(
            f.col("vehicle_id"),
            f.col("latitude"),
            f.col("longitude"),
            f.col("timestamp"),
            f.col("distance_km"),
            f.col("speed"),
        )))
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", CONN_KAFKA)
        .option("topic", topic_output.name)
        .option("checkpointLocation", os.path.join(SPARK_CHECKPOINT_BASE_DIR, "vehicles_enrichment_kafka"))
        .outputMode("append")
        .start()
    )

    # Await
    stream_s3.awaitTermination()
    stream_kafka.awaitTermination()
    logger.info("Done!")
