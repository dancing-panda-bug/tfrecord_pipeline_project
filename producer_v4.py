import sys
import pickle
import time
from pathlib import Path
from kafka import KafkaProducer

KAFKA_TOPIC = "image_stream_tfrecordss"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

def send_images_from_folder(producer, current_folder, parent_name, rel_base):
    """Recursively send images (including subfolders), encoding relative path."""
    # image_files = sorted(current_folder.glob("*.bmp"))
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
            "relative_path": str(image_path.relative_to(rel_base)),   #  full relative path
            "parent": parent_name,
            "image_bytes": image_bytes
        }
        producer.send(KAFKA_TOPIC, value=pickle.dumps(message))
        print(f"Sent image: {image_path.name} from {image_path.parent}")
        time.sleep(0.01)  # avoid flooding

    # (deep recursion)
    for subfolder in current_folder.iterdir():
        if subfolder.is_dir():
            send_images_from_folder(producer, subfolder, parent_name, rel_base)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python producer.py <parent_folder_path>")
        sys.exit(1)

    parent_folder = Path(sys.argv[1]).resolve()
    if not parent_folder.exists() or not parent_folder.is_dir():
        print(f"Folder not found: {parent_folder}")
        sys.exit(1)
    parent_name = parent_folder.name
    print(f"Traversing and sending images under: {parent_folder}")
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    try:
        send_images_from_folder(producer, parent_folder, parent_name, parent_folder)
        # Signal "parent_done"
        done_msg = {
            "type": "parent_done",
            "parent": parent_name
        }
        producer.send(KAFKA_TOPIC, value=pickle.dumps(done_msg))
        print(f"Sent PARENT_DONE for top folder: {parent_name}")
        producer.flush()
    except Exception as e:
        print(f"Error in producer: {e}")
    finally:
        producer.close()
        print("Finished traversal and sending images.")
