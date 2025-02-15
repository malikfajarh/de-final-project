import json
import uuid
import os
import json
from dotenv import load_dotenv
from pathlib import Path
from kafka import KafkaProducer
# from faker import Faker
from time import sleep
from datetime import datetime, timedelta
import pandas as pd

dotenv_path = Path("/opt/app/.env")
load_dotenv(dotenv_path=dotenv_path)

kafka_host = os.getenv("KAFKA_HOST")
kafka_topic = os.getenv("KAFKA_TOPIC_NAME")

producer = KafkaProducer(bootstrap_servers=f"{kafka_host}:9092")
print("connected to kafka")

current_dir = os.path.dirname(os.path.abspath(__file__))
csv_path = os.path.join(current_dir, 'csv', 'click_activity.csv')

if not os.path.exists(csv_path):
    raise FileNotFoundError(f"CSV file not found at {csv_path}")
else:
    print(csv_path)

chunk_size = 10000  
max_messages = 5  
count = 0 

for chunk in pd.read_csv(csv_path, chunksize=chunk_size, on_bad_lines="skip"):
    for _, row in chunk.iterrows():
        if count >= max_messages:
            print("Reached message limit. Stopping producer...")
            break 

        data = row.to_dict()
        _payload = json.dumps(data).encode("utf-8")
        
        response = producer.send(topic=kafka_topic, value=_payload)
        print(f"msg:{count + 1} |", response.get())
        print("--" * 50, flush=True)

        count += 1
        sleep(5)  
        
    if count >= max_messages:
        break  # Stop processing more chunks
