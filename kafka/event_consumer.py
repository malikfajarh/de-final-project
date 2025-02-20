from kafka import KafkaConsumer
import json
import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from pathlib import Path
from sqlalchemy import text

dotenv_path = Path("/opt/app/.env")
load_dotenv(dotenv_path=dotenv_path)

pg_host = os.getenv("POSTGRES_CONTAINER_NAME")
pg_port =  os.getenv("POSTGRES_DF_PORT") 
pg_db = os.getenv("POSTGRES_DW_DB")
pg_user = os.getenv("POSTGRES_USER")
pg_password = os.getenv("POSTGRES_PASSWORD")

pg_url = f"postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
engine = create_engine(pg_url)

kafka_topic = os.getenv("KAFKA_TOPIC_NAME")
kafka_host = os.getenv("KAFKA_HOST")

consumer = KafkaConsumer(
    kafka_topic,
    bootstrap_servers=f"{kafka_host}:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

def get_latest_event_time():
    query = "SELECT MAX(event_time) FROM stream.click_activity"
    result = pd.read_sql(query, engine)
    return pd.to_datetime(result.iloc[0, 0])


try:
    print("Streaming Kafka messages to PostgreSQL...")

    with engine.begin() as conn:  
        conn.execute(text("TRUNCATE TABLE stream.click_activity;"))

    while True:
        records = consumer.poll(timeout_ms=10000) 
        if not records:
            print("No more messages. Closing consumer...")
            break  

        for _, messages in records.items():
            for message in messages:
                data = message.value 

                df = pd.DataFrame([data])

                df.to_sql("click_activity", engine, schema="stream", if_exists="append", index=False)
                print(f"Inserted data into PostgreSQL: {data}")
                print('--'*55)

finally:
    consumer.close()
    print("Kafka Consumer Closed")
