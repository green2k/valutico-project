import logging
from typing import List

import redis
from pyspark.sql import DataFrame, functions as f, Window
from pyspark.sql.streaming.query import StreamingQuery
from pyspark.sql.types import Row

from pavol.constants import CONN_REDIS_HOST, CONN_REDIS_PORT


logger = logging.getLogger(__name__)


def __batch_max_speed(df: DataFrame, _: int):
    r = redis.Redis(host=CONN_REDIS_HOST, port=CONN_REDIS_PORT, db=1)  # TODO: Use DI here for better testability

    rows: List[Row] = (
        df
        .withColumn("rank", f.row_number().over(Window.partitionBy(f.col("timestamp")).orderBy(f.col("max_speed").desc())))
        .filter(f.col("rank") == 1)
        .collect()
    )
    for row in rows:
        timestamp = row["timestamp"]
        vehicle_id = row["vehicle_id"]
        max_speed = row["max_speed"]
        logger.info(f"[Redis] [{timestamp}] -> Fastest vehicle is {vehicle_id} ({max_speed} km/h)")
        r.hset(timestamp.isoformat(), mapping={
            "vehicle_id": vehicle_id,
            "max_speed": max_speed,
        })


def build_query_fastest_vehicle_over_time(df: DataFrame) -> StreamingQuery:
    """
    Determines the fastest vehicle with respect to all processed timestamps
    """
    return (
        df
        .withColumn("timestamp", f.date_trunc("minute", f.col("timestamp")))
        .withWatermark("timestamp", "5 minutes")
        .groupBy(f.col("vehicle_id"), f.col("timestamp")).agg(f.max(f.col("speed")).alias("max_speed"))
        .writeStream
        .foreachBatch(__batch_max_speed)
        .outputMode("complete")
        .start()
    )
