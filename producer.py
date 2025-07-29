import os
import io
import base64
import json
from kafka import KafkaProducer
from dotenv import load_dotenv
from PIL import Image

load_dotenv()

producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

topic = os.getenv("KAFKA_IMAGE_TOPIC")
image_dir = os.getenv("IMAGE_DIR")

def encode_image(image_path):
    with open(image_path, "rb") as img_file:
        encoded_bytes = base64.b64encode(img_file.read()).decode("utf-8")
    img = Image.open(image_path)
    return encoded_bytes, img.size

count = 0
for root, _, files in os.walk(image_dir):
    for file in files:
        if file.endswith(".bmp"):
            file_path = os.path.join(root, file)
            img_bytes, shape = encode_image(file_path)
            label = os.path.basename(os.path.dirname(file_path))

            message = {
                "filename": file,
                "image_bytes": img_bytes,
                "shape": shape,
                "label": label
            }

            producer.send(topic, value=message)
            count += 1

producer.flush()
print(f"✅ Sent {count} images.")
