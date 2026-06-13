import json
import time
import random
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='<KAFKA_VM_PUBLIC_IP>:9092')

while True:
    data = {
        "city": random.choice(["London", "Manchester"]),
        "demand": random.randint(50, 200),
        "supply": random.randint(20, 150),
        "event_time": time.strftime("%Y-%m-%d %H:%M:%S")
    }
    producer.send("pricing", json.dumps(data).encode())
    print("Sent:", data)
    time.sleep(2)
