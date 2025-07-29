# test.py — send 100 images
from kafka import KafkaProducer
import json
import base64
import os
from PIL import Image
import io

KAFKA_TOPIC = "image_stream_tfrecords1"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
IMAGE_DIR = "/home/abhishek/Projects/pypro/tfrecord_pipeline_project/sample_dataset"

producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)

sent_count = 0

for root, _, files in os.walk(IMAGE_DIR):
    for file in files:
        if not file.lower().endswith(('.bmp', '.jpg', '.jpeg', '.png')):
            continue

        image_path = os.path.join(root, file)
        with open(image_path, "rb") as f:
            image_bytes = f.read()

        try:
            image = Image.open(io.BytesIO(image_bytes))
            width, height = image.size
        except Exception as e:
            print(f"Skipping invalid image {file}: {e}")
            continue

        image_base64 = base64.b64encode(image_bytes).decode("utf-8")
        label = os.path.basename(os.path.dirname(root))  # Folder name as label
        message = {
            "filename": file,
            "image_bytes": image_base64,
            "shape": [height, width],
            "label": label
        }

        producer.send(KAFKA_TOPIC, json.dumps(message).encode("utf-8"))
        sent_count += 1
        print(f"Sent: {file} ({sent_count})")

        if sent_count >= 100:
            break
    if sent_count >= 100:
        break

producer.flush()
print(f"Finished sending {sent_count} images.")
