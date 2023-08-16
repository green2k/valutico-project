import logging
from typing import List

import redis
from pyspark.sql import DataFrame, functions as f
from pyspark.sql.streaming.query import StreamingQuery
from pyspark.sql.types import Row

from pavol.constants import CONN_REDIS_HOST, CONN_REDIS_PORT


logger = logging.getLogger(__name__)


def __batch_avg_speed_5m(df: DataFrame, _: int):
    r = redis.Redis(host=CONN_REDIS_HOST, port=CONN_REDIS_PORT, db=0)  # TODO: Use DI here for better testability

    rows: List[Row] = df.collect()
    for row in rows:
        vehicle_id = row["vehicle_id"]
        speed_5m_average = row["speed_5m_average"]
        logger.info(f"[Redis] Vehicle {vehicle_id} -> Average speed in past 5 minutes: {speed_5m_average}")
        r.hset(vehicle_id, mapping={
            "speed_5m_average": speed_5m_average,
        })


def build_query_avg_speed_5m(df: DataFrame) -> StreamingQuery:
    """
    Calculates the average speed (last 5 minutes) of all vehicles.
    """
    return (
        df
        .withWatermark("timestamp", "5 minutes")
        .groupBy(f.col("vehicle_id")).agg(f.avg(f.col("speed")).alias("speed_5m_average"))
        .writeStream
        .foreachBatch(__batch_avg_speed_5m)
        .outputMode("update")
        .start()
    )
