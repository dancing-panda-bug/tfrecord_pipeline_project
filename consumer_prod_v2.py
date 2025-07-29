# consumer_prod_v2.py
import time
import json
from kafka import KafkaConsumer
import tensorflow as tf
from hdfs import InsecureClient

KAFKA_TOPIC = "image_stream_tfrecords2"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
HDFS_URL = "http://localhost:9870"
HDFS_OUTPUT_DIR = "/tfrecords"

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

print("✅ Connected to Kafka")
hdfs_client = InsecureClient(HDFS_URL, user='abhishek')
print("✅ Connected to HDFS")

message_buffer = []
last_message_time = time.time()
current_folder = None

def write_tfrecord(folder, messages):
    if not messages:
        return

    tfrecord_path = f"{HDFS_OUTPUT_DIR}/{folder}.tfrecord"
    with tf.io.TFRecordWriter("/tmp/temp.tfrecord") as writer:
        for data in messages:
            img_bytes = bytes(data["image"])
            metadata = data["metadata"]
            feature = {
                "image": tf.train.Feature(bytes_list=tf.train.BytesList(value=[img_bytes])),
                "filename": tf.train.Feature(bytes_list=tf.train.BytesList(value=[metadata["filename"].encode()])),
                "folder": tf.train.Feature(bytes_list=tf.train.BytesList(value=[metadata["folder"].encode()])),
                "full_path": tf.train.Feature(bytes_list=tf.train.BytesList(value=[metadata["full_path"].encode()]))
            }
            example = tf.train.Example(features=tf.train.Features(feature=feature))
            writer.write(example.SerializeToString())

    hdfs_client.upload(tfrecord_path, "/tmp/temp.tfrecord", overwrite=True)
    print(f"[Consumer] ✅ Written TFRecord to HDFS: {tfrecord_path}")

print("🔄 Waiting for messages...")

try:
    while True:
        msg = next(consumer)
        try:
            message = json.loads(msg.value.decode('utf-8'))
            metadata = message.get("metadata")
            if not metadata:
                print("⚠️ Skipping message without metadata")
                continue

            folder = metadata["folder"]
            if current_folder is None:
                current_folder = folder

            # If new folder detected, write previous TFRecord
            if folder != current_folder:
                write_tfrecord(current_folder, message_buffer)
                message_buffer.clear()
                current_folder = folder

            message_buffer.append(message)
            last_message_time = time.time()

        except Exception as e:
            print(f"❌ Error decoding message: {e}")

        # Check for idle timeout
        if message_buffer and (time.time() - last_message_time) >= 5:
            write_tfrecord(current_folder, message_buffer)
            message_buffer.clear()
            current_folder = None

except KeyboardInterrupt:
    print("👋 Graceful shutdown")
    if message_buffer:
        write_tfrecord(current_folder, message_buffer)
