import os
import json
import time
import pandas as pd
from confluent_kafka import Consumer, Producer, KafkaError

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BROKER", "localhost:9092")
INPUT_TOPIC = 'raw-events'
OUTPUT_TOPIC = 'processed-events'
consumer_conf = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'group.id': 'analytics-pipeline-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}
consumer = Consumer(consumer_conf)
consumer.subscribe([INPUT_TOPIC])
producer_conf = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'client.id': 'analytics-pipeline-producer'
}
producer = Producer(producer_conf)

def consum_raw_data(raw_data_file_name):
    idle_start = None
    MAX_IDLE_SECONDS = 10
    has_data = False
    try:
        with open(raw_data_file_name, "a") as f:
            
            while True:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    if has_data:
                        if idle_start is None:
                            idle_start = time.time()
                        elif time.time() - idle_start >= MAX_IDLE_SECONDS:
                            print("No new messages, ending.")
                            break
                    continue
                else:
                    idle_start = None
                if msg is not None:
                    if msg.error():
                        if msg.error().code() != KafkaError._PARTITION_EOF:
                            print(f"Consumer error: {msg.error()}")
                    else:
                        try:
                            has_data = True
                            raw_line = msg.value().decode('utf-8').strip()
                            f.write(raw_line + "\n")
                            # record = json.loads(msg.value().decode('utf-8'))
                            # record = str(record)
                            # f.write(record+"\n")
                        except json.JSONDecodeError:
                            print(f"Malformed JSON: {msg.value()}")
    except FileNotFoundError:
        print("File")
    except KafkaError:
        print("kafka")
    except BufferError:
        print("Buffer")

def split_multiselect(value):
    if pd.isna(value):
        return None
    return value.split(";")

def save_to_jsonl(df: pd.DataFrame, file_path) -> bool:
    try:
        with open(file_path, "a") as f:
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

def transform_data(raw_data, cleaned_data):
    if not os.path.exists(raw_data) or os.path.getsize(raw_data) == 0:
        print(f"Error: {raw_data} is empty or missing.")
        return
    df = pd.read_json(raw_data, lines=True)
    try:
        # os.makedirs("../my_work", exist_ok=True)
        if df.duplicated().sum() > 0:
            df = df.drop_duplicates()
        # 1. החלפת מחרוזות הקצה בערכים מספריים הגיוניים
        df['YearsCode'] = df['YearsCode'].replace({
        'Less than 1 year': 0,
        'More than 50 years': 51
        })

        # 2. המרה בטוחה למספר - errors='coerce' יהפוך כל טקסט לא צפוי אחר ל-NaN במקום לקרוס
        df['YearsCode'] = pd.to_numeric(df['YearsCode'], errors='coerce').astype('Int64')
        # df['YearsCode'] = df['YearsCode'].astype('Int64')
        df['LearnCode'] = df['LearnCode'].apply(split_multiselect)
        df['AILearnHow'] = df['AILearnHow'].apply(split_multiselect)
        # print(df.shape)
        # print(df['YearsCode'].dtype)
        # print(df['LearnCode'].head())
        save = save_to_jsonl(df, cleaned_data)
        if save:
            print("The clean file was created and saved successfully.")
        else:
            print("Failed to save clean file.")
    except:
        print("An unexpected error occurred.")
    # return df

def produce_processed_data(file_name) -> None:
    if not os.path.exists(file_name):
        print(f"Error: {file_name} does not exist.")
        return

    with open(file_name, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            
            row = json.loads(line)
            key = str(row.get('ResponseId', '')).encode('utf-8')
            payload = line.encode('utf-8') 

            producer.produce(
                topic=OUTPUT_TOPIC,
                key=key,
                value=payload
            )
            producer.poll(0)

    producer.flush()

def main(raw_data, cleaned_data):
    consum_raw_data(raw_data)
    transform_data(raw_data, cleaned_data)
    produce_processed_data(cleaned_data)

if __name__ == "__main__":
    main("raw_lines.jsonl", "cleaned_data.jsonl")
