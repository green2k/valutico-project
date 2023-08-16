import logging

import redis
from pyspark.sql import DataFrame, functions as f, Window
from pyspark.sql.streaming.query import StreamingQuery
from pyspark.sql.types import Row
from pavol.constants import CONN_REDIS_HOST, CONN_REDIS_PORT

from pavol.utils import udf_calculate_distance


logger = logging.getLogger(__name__)


def __batch_latest_position(df: DataFrame, _: int):
    r = redis.Redis(host=CONN_REDIS_HOST, port=CONN_REDIS_PORT, db=2)  # TODO: Use DI here for better testability

    vehicles_count_in_radius = (
        df
        .withColumn("rank", f.row_number().over(Window.partitionBy(f.col("vehicle_id")).orderBy(f.col("timestamp").desc())))
        .filter(f.col("rank") == 1)
        .withColumn("distance_km_from_start", udf_calculate_distance(f.col("latitude"), f.col("longitude"), f.lit(48.8566), f.lit(2.3522)))
        .filter(f.col("distance_km_from_start") < 2)
        .count()
    )
    vehicles_count_in_radius
    r.hset("Paris", mapping={
        "vehicles_count": vehicles_count_in_radius,
    })


def build_query_radius_check(df: DataFrame) -> StreamingQuery:
    """
    Calculates the number of vehicles in a radius of 2km from the starting point.
    """
    return(
        df
        .withWatermark("timestamp", "5 minutes")
        .writeStream
        .foreachBatch(__batch_latest_position)
        .outputMode("update")
        .start()
    )
