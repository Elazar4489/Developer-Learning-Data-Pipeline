import pandas as pd
import os
import json
from confluent_kafka import Producer, Consumer

broker = os.getenv("KAFKA_BROKER", "localhost:9092")
topic1 = "raw-lines"
topic2 = "processed-rows"

print(f"Connecting to Kafka broker at: {broker}...")

conf = {
    'bootstrap.servers': broker
}
consumer = Consumer(conf)

producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")








def data_cleaning(source_file_path: str, destination_file_path: str)-> None:
    try:
        os.makedirs("./my_work", exist_ok=True)
        df = pd.read_csv(source_file_path)
        if df.duplicated().sum() > 0:
            df = df.drop_duplicates()
        df['YearsCode'] = df['YearsCode'].astype('Int64')
        df['LearnCode'] = df['LearnCode'].apply(split_multiselect)
        df['AILearnHow'] = df['AILearnHow'].apply(split_multiselect)
        print(df.shape)
        print(df['YearsCode'].dtype)
        print(df['LearnCode'].head())
        save = save_to_jsonl(df, destination_file_path)
        if save:
            print("The clean file was created and saved successfully.")
        else:
            print("Failed to save clean file.")
    except:
        print("An unexpected error occurred.")
    






def split_multiselect(value):
    if pd.isna(value):
        return None
    return value.split(";")

def save_to_jsonl(df: pd.DataFrame, file_path) -> bool:
    try:
        with open(f"./my_work/{file_path}", "w") as f:
            for _, row in df.iterrows():
                record = row.to_dict()
                
                # Convert Int64 to int or None
                if pd.notna(record['YearsCode']):
                    record['YearsCode'] = int(record['YearsCode'])
                else:
                    record['YearsCode'] = None
                
                # Convert NaN to None
                for key, value in record.items():
                    if not isinstance(value, list) and pd.isna(value):
                        record[key] = None
                
                f.write(json.dumps(record) + "\n")
        return True
    except:
        return False