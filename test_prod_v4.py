import sys
import pickle
import time
from pathlib import Path
from kafka import KafkaProducer

KAFKA_TOPIC = "image_stream_tfrecordss"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

def send_images_from_folder(producer, current_folder, parent_name, rel_base):
    """Recursively send images (including subfolders), encoding relative path."""
    image_files = sorted([
        file for ext in ("*.bmp", "*.jpg", "*.jpeg")
        for file in current_folder.glob(ext)
    ])

    for image_path in image_files:
        with open(image_path, "rb") as f:
            image_bytes = f.read()

        message = {
            "type": "image",
            "filename": image_path.name,
            "relative_path": str(image_path.relative_to(rel_base)),  # full relative path
            "parent": parent_name,
            "image_bytes": image_bytes
        }

        producer.send(KAFKA_TOPIC, value=pickle.dumps(message))
        print(f"📤 Sent image: {image_path.name} from {image_path.parent}")
        time.sleep(0.01)  # avoid Kafka flooding

    # Recurse through subfolders
    for subfolder in current_folder.iterdir():
        if subfolder.is_dir():
            send_images_from_folder(producer, subfolder, parent_name, rel_base)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python producer.py <parent_folder_or_image_path>")
        sys.exit(1)

    input_path = Path(sys.argv[1]).resolve()

    if not input_path.exists():
        print(f"❌ Path not found: {input_path}")
        sys.exit(1)

    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)

    try:
        if input_path.is_file():
            with open(input_path, "rb") as f:
                image_bytes = f.read()

            parent_name = input_path.parent.name
            message = {
                "type": "image",
                "filename": input_path.name,
                "relative_path": input_path.name,
                "parent": parent_name,
                "image_bytes": image_bytes
            }
            producer.send(KAFKA_TOPIC, value=pickle.dumps(message))
            print(f"📤 Sent single image: {input_path.name}")

            # Send done
            done_msg = {
                "type": "parent_done",
                "parent": parent_name
            }
            producer.send(KAFKA_TOPIC, value=pickle.dumps(done_msg))
            print(f"✅ Sent DONE signal for parent: {parent_name}")
        elif input_path.is_dir():
            parent_name = input_path.name
            print(f"🔍 Traversing folder: {input_path}")
            send_images_from_folder(producer, input_path, parent_name, input_path)

            # Send done
            done_msg = {
                "type": "parent_done",
                "parent": parent_name
            }
            producer.send(KAFKA_TOPIC, value=pickle.dumps(done_msg))
            print(f"✅ Sent DONE signal for parent: {parent_name}")
        else:
            print("❌ Unsupported input type.")

        producer.flush()
    except Exception as e:
        print(f"❌ Error in producer: {e}")
    finally:
        producer.close()
        print("✅ Finished sending.")
