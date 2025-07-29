# consumer.py — batch 100 images into one TFRecord and save to HDFS

from kafka import KafkaConsumer
import json
import base64
import tensorflow as tf
import os
from hdfs import InsecureClient
import uuid
import io
from PIL import Image

# Kafka and HDFS settings
KAFKA_TOPIC = "image_stream_tfrecords1"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
HDFS_URL = "http://localhost:9870"
HDFS_USER = "abhishek"
HDFS_OUTPUT_DIR = "/data/demo"
BATCH_SIZE = 100

# Setup Kafka consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    group_id="tfrecord_pipeline",
    enable_auto_commit=True
)

# Setup HDFS client
hdfs_client = InsecureClient(HDFS_URL, user=HDFS_USER)

# Utility function to create TFRecord example
def create_example(filename, image_bytes, shape, label):
    return tf.train.Example(features=tf.train.Features(feature={
        'filename': tf.train.Feature(bytes_list=tf.train.BytesList(value=[filename.encode()])),
        'image_raw': tf.train.Feature(bytes_list=tf.train.BytesList(value=[image_bytes])),
        'shape': tf.train.Feature(int64_list=tf.train.Int64List(value=shape)),
        'label': tf.train.Feature(bytes_list=tf.train.BytesList(value=[label.encode()])),
    }))

# Main batch buffer
batch = []

print("Listening for messages...")

for message in consumer:
    try:
        data = json.loads(message.value.decode("utf-8"))
        filename = data["filename"]
        image_base64 = data["image_bytes"]
        image_bytes = base64.b64decode(image_base64)
        shape = data["shape"]
        label = data["label"]

        example = create_example(filename, image_bytes, shape, label)
        batch.append(example)

        print(f"Buffered: {filename} ({len(batch)}/{BATCH_SIZE})")

        if len(batch) >= BATCH_SIZE:
            # Write TFRecord
            tfrecord_filename = f"batch_{uuid.uuid4().hex}.tfrecord"
            local_path = f"/tmp/{tfrecord_filename}"

            with tf.io.TFRecordWriter(local_path) as writer:
                for ex in batch:
                    writer.write(ex.SerializeToString()) #serializatioooooooon

            # Upload to HDFS
            hdfs_path = f"{HDFS_OUTPUT_DIR}/{tfrecord_filename}"
            hdfs_client.upload(hdfs_path, local_path)
            os.remove(local_path)

            print(f"📤 Wrote batch of {BATCH_SIZE} images to {hdfs_path}")
            batch = []  # Clear buffer

    except Exception as e:
        print(f"❌ Error processing message: {e}")
