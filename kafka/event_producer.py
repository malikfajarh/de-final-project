import json
import os
import json
from dotenv import load_dotenv
from pathlib import Path
from kafka import KafkaProducer
from time import sleep
import pandas as pd
import ast
import numpy as np


dotenv_path = Path("/opt/app/.env")
load_dotenv(dotenv_path=dotenv_path)

kafka_host = os.getenv("KAFKA_HOST")
kafka_topic = os.getenv("KAFKA_TOPIC_NAME")
producer = KafkaProducer(bootstrap_servers=f"{kafka_host}:9092")

current_dir = os.path.dirname(os.path.abspath(__file__))
csv_path = os.path.join(current_dir, 'csv', 'click_activity.csv')


def safe_parse(data):
    if pd.isna(data):
        return {}
    try:
        return ast.literal_eval(data)
    except:
        return {}


chunk_size = 10000  
max_messages = 100 
count = 0 

print(f'limiting to {max_messages} messages...')

for chunk in pd.read_csv(csv_path, chunksize=chunk_size):

    chunk["event_metadata"] = chunk["event_metadata"].apply(safe_parse)
    metadata_df = pd.json_normalize(chunk["event_metadata"])
    chunk = pd.concat([chunk.drop(columns=["event_metadata"]), metadata_df], axis=1)

    chunk["event_time"] = pd.to_datetime(chunk["event_time"])  
    chunk["event_time"] = chunk["event_time"].dt.tz_convert("Asia/Bangkok").dt.strftime("%Y-%m-%d %H:%M:%S.%f%z")
    chunk = chunk.replace({np.nan: None})

    for _, row in chunk.iterrows():
        if count >= max_messages:
            print("Reached message limit. Stopping producer...")
            break 

        data = row.to_dict()
        _payload = json.dumps(data, ensure_ascii=False).encode("utf-8")

        response = producer.send(topic=kafka_topic, value=_payload)
        print(f"msg:{count + 1} |", response.get())
        print("==" * 55, flush=True)

        count += 1
        sleep(2)  
        
    if count >= max_messages:
        break  
    
producer.flush()
producer.close()

