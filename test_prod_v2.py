# producer_v2.py
import os
import time
import json
from kafka import KafkaProducer

KAFKA_TOPIC = "image_stream_tfrecords2"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
DATASET_PATH = "/home/abhishek/Projects/pypro/tfrecord_pipeline_project/sample_dataset"

producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)

def send_image(folder_path):
    for root, _, files in os.walk(folder_path):
        image_files = [f for f in files if f.lower().endswith(('.bmp', '.jpg', '.jpeg', '.png'))]
        for file in image_files:
            filepath = os.path.join(root, file)
            try:
                with open(filepath, 'rb') as f:
                    img_bytes = f.read()
                metadata = {
                    "filename": file,
                    "folder": os.path.basename(folder_path),  # top-level folder (e.g. "MARC11")
                    "full_path": os.path.relpath(root, DATASET_PATH)  # relative nested path
                }
                message = {
                    "image": list(img_bytes),
                    "metadata": metadata
                }
                producer.send(KAFKA_TOPIC, json.dumps(message).encode('utf-8'))
                print(f"[Producer] Sent: {filepath}")
                time.sleep(0.1)
            except Exception as e:
                print(f"[Producer Error] {filepath}: {e}")
        print(f"[Producer] End of folder: {folder_path}")

if __name__ == "__main__":
    for folder in os.listdir(DATASET_PATH):
        folder_path = os.path.join(DATASET_PATH, folder)
        if os.path.isdir(folder_path):
            send_image(folder_path)
    producer.flush()
