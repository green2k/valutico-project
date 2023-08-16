import json
import logging
import os
import time
from datetime import datetime, timedelta

from kafka import KafkaAdminClient, KafkaProducer

from pavol.constants import CONN_KAFKA, GPS_POINT_PARIS, KAFKA_TOPICS, PRODUCER_NUM_VEHICLES
from pavol.types import Vehicle
from pavol.utils import move_vehicle, upsert_topic


logging.basicConfig(level=logging.WARN, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOGLEVEL", "INFO"))

if __name__ == "__main__":
    # Bootstrap
    admin = KafkaAdminClient(bootstrap_servers=CONN_KAFKA)
    topic = KAFKA_TOPICS["vehicle_location"]

    # Make sure the topic exists
    upsert_topic(admin, topic)

    # Initialize KafkaProducer
    producer = KafkaProducer(
        client_id="client1",
        bootstrap_servers=CONN_KAFKA,
        key_serializer=str.encode,
        value_serializer=str.encode,
    )

    # Build a list of all simulated vehicles
    vehicles = [
        Vehicle(id=f"VEH{i}", lat=GPS_POINT_PARIS.lat, lon=GPS_POINT_PARIS.lon, bearing=180, speed=0)
        for i in range(0, PRODUCER_NUM_VEHICLES)
    ]

    current_time = datetime.now()
    while True:  # An infinite loop for simulating the movement of vehicles
        # Simulate next tick
        current_time = current_time + timedelta(seconds=1)
        logger.info(f"Simulating vehicles for timestamp: {current_time.isoformat()}")

        # Simulate the movement of all vehicles
        for vehicle in vehicles:
            move_vehicle(vehicle)

            # Build output message
            message = {
                "vehicle_id": vehicle.id,
                "latitude": vehicle.lat,
                "longitude": vehicle.lon,
                "timestamp": current_time.isoformat()
            }

            # Send message
            producer.send(topic=topic.name, key=vehicle.id, value=json.dumps(message))

        # Explicitly flush each "batch"
        producer.flush()

        # Simulate 1 second delay
        time.sleep(1)
