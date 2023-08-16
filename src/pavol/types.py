from pydantic import conint, constr, BaseModel
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

class GpsPoint(BaseModel):
    lat: float
    lon: float

class KafkaTopic(BaseModel):
    name: constr(min_length=3)
    num_partitions: conint(ge=1) = 10
    replication_factor: conint(ge=1) = 1

class Vehicle(BaseModel):
    id: constr(min_length=3)
    lat: float
    lon: float
    speed: float
    bearing: float

SCHEMA_KAFKA_INPUT = StructType([
    StructField("vehicle_id", StringType()), 
    StructField("latitude", DoubleType()), 
    StructField("longitude", DoubleType()),
    StructField("timestamp", TimestampType())
])

SCHEMA_KAFKA_INPUT_ENRICHED = StructType([
    StructField("vehicle_id", StringType()), 
    StructField("latitude", DoubleType()), 
    StructField("longitude", DoubleType()),
    StructField("timestamp", TimestampType()),
    StructField("distance_km", DoubleType()),
    StructField("speed", DoubleType()),
])
