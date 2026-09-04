import pandas as pd
import json
import os
import time
from confluent_kafka import Producer


broker = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = 'raw-events'

print(f"Connecting to Kafka broker at: {broker}...")

conf = {
    'bootstrap.servers': broker
}

producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")





def data_exploration(file_path: str) -> None:
    try:
        df = pd.read_csv(file_path)
        print("=====Summary=====\n"
                "What we found:")
        print(f"{df.shape[0]} rows,\n{df.shape[1]} columns")
        print(f"{df.head()}")
        print(f"{df.dtypes}")
        print(f"duplicates number is: {df.isna().sum()}")
        print(f"{df.duplicated().sum()}")
        print(f"not unique ResponseId num is: {df['ResponseId'].duplicated().sum()}")
        print(f"{df['Age'].value_counts()}")
        print(f"{df['AISelect'].value_counts(dropna=False)}")
        print(f"{df['LearnCode'].head()}")
        df = df.where(pd.notnull(df), None)
        for row in df.to_dict(orient="records"):
            payload = json.dumps(row).encode("utf-8")
            key = str(row(["ResponseId"])).encode("utf-8")
            producer.produce(topic=TOPIC, key=key, value=payload, callback=delivery_report)
            producer.poll(0)
        producer.flush()
    except FileNotFoundError:
        print("File Not Found Error")



if __name__ == "__main__":
    data_exploration("./developer_ai_learning_raw.csv")