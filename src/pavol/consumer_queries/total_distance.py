import logging
from typing import List

import redis
from pyspark.sql import DataFrame, functions as f
from pyspark.sql.streaming.query import StreamingQuery
from pyspark.sql.types import Row

from pavol.constants import CONN_REDIS_HOST, CONN_REDIS_PORT


logger = logging.getLogger(__name__)


def __batch_total_distance(df: DataFrame, _: int):
    r = redis.Redis(host=CONN_REDIS_HOST, port=CONN_REDIS_PORT, db=0)  # TODO: Use DI here for better testability

    rows: List[Row] = df.collect()
    for row in rows:
        vehicle_id = row["vehicle_id"]
        total_distance_km = row["total_distance_km"]
        logger.info(f"[Redis] Vehicle {vehicle_id} -> Total distance km: {total_distance_km}")
        r.hset(vehicle_id, mapping={
            "total_distance_km": total_distance_km,
        })


def build_query_total_distance(df: DataFrame) -> StreamingQuery:
    """
    Calculates the total distance travelled, per vehicle
    """
    return (
        df
        .groupBy("vehicle_id").agg(f.sum(f.col("distance_km")).alias("total_distance_km"))
        .writeStream
        .foreachBatch(__batch_total_distance)
        .outputMode("complete")
        .start()
    )
