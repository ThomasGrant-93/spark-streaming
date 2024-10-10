#!/usr/bin/python3
import json
import logging
import threading
from time import (time, sleep)

import numpy as np  # pip install numpy
from kafka import KafkaProducer  # pip install kafka-python

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
tlogger = logging.getLogger("DeviceDataLogger")

# different device "profiles" with different
# distributions of values to make things interesting
# tuple --> (mean, std.dev)
DEVICE_PROFILES = {
    "boston": {"temp": (51.3, 17.7), "humd": (77.4, 18.7), "pres": (1019.9, 9.5)},
    "denver": {"temp": (49.5, 19.3), "humd": (33.0, 13.9), "pres": (1012.0, 41.3)},
    "losang": {"temp": (63.9, 11.7), "humd": (62.8, 21.8), "pres": (1015.9, 11.3)},
}

producer = KafkaProducer(bootstrap_servers="localhost:9093")


def send_message(profile_name: str, profile: dict):
    _count: int = 1
    while _count <= 100_000:
        # get random values within a normal distribution of the value
        temp = np.random.normal(profile["temp"][0], profile["temp"][1])
        humd = max(0, min(np.random.normal(profile["humd"][0], profile["humd"][1]), 100))
        pres = np.random.normal(profile["pres"][0], profile["pres"][1])

        # create CSV structure
        msg = {
            "timestamp": time(),
            "device": profile_name,
            "temp": temp,
            "humd": humd,
            "pres": pres
        }

        # send to Kafka
        producer.send(
            topic="weather",
            key=None,
            value=bytes(json.dumps(msg), encoding="utf8"),
            headers=[
                ("Content-Type", b"application/json")
            ],
        )

        tlogger.info(f"sending data to kafka, #{_count}")

        _count += 1
        sleep(.5)


stop_event = threading.Event()
threads = []
try:
    for profile_name, profile in DEVICE_PROFILES.items():
        thread = threading.Thread(target=send_message, args=(profile_name, profile), name=f"Tread-{profile_name}")
        threads.append(thread)
        thread.start()
        tlogger.info(f'Created and started thread: {thread.name} for device {profile_name}')

    [thread.join() for thread in threads]

except KeyboardInterrupt:
    tlogger.info("Shutdown initiated by user (Ctrl+C). Stopping all threads...")
    stop_event.set()
    [thread.join() for thread in threads]
    producer.close()  # Close Kafka producer
    tlogger.info("All threads stopped. Kafka producer closed. Exiting program.")
