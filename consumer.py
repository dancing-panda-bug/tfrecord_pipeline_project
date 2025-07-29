# import os
# import json
# import base64
# import uuid
# import io
# from kafka import KafkaConsumer
# from dotenv import load_dotenv
# from tfrecord_writer import create_example
# import tensorflow as tf
# from hdfs import InsecureClient

# # Load environment
# load_dotenv()

# # Configs
# TOPIC = os.getenv("KAFKA_IMAGE_TOPIC")
# BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
# GROUP_ID = os.getenv("KAFKA_CONSUMER_GROUP")

# HDFS_HOST = os.getenv("HDFS_NAMENODE_HOST")
# HDFS_PORT = os.getenv("HDFS_NAMENODE_PORT")
# HDFS_USER = os.getenv("HDFS_USER")
# HDFS_OUTPUT_PATH = os.getenv("HDFS_OUTPUT_PATH")

# # HDFS Client
# hdfs_url = f"http://{HDFS_HOST}:{HDFS_PORT}"
# client = InsecureClient(hdfs_url, user=HDFS_USER)

# # Kafka Consumer
# consumer = KafkaConsumer(
#     TOPIC,
#     bootstrap_servers=BOOTSTRAP_SERVERS,
#     group_id=GROUP_ID,
#     auto_offset_reset='earliest',
#     enable_auto_commit=True
# )

# print(f"Listening to Kafka topic: {TOPIC}")

# for message in consumer:
#     try:
#         value = message.value.decode("utf-8")
#         print("Raw message:", value)

#         data = json.loads(value)
#         image_bytes = base64.b64decode(data["image_bytes"])
#         shape = data["shape"]
#         label = data["label"]
#         filename = data["filename"]

#         # Create TFRecord Example
#         example = create_example(filename, label, shape, image_bytes)
#         tfrecord_data = example.SerializeToString()

#         # Save to unique TFRecord file
#         tfrecord_name = f"{uuid.uuid4().hex}.tfrecord"
#         hdfs_file_path = os.path.join(HDFS_OUTPUT_PATH, tfrecord_name)

#         with client.write(hdfs_file_path, overwrite=True) as writer:
#             writer.write(tfrecord_data)

#         print(f"Saved: {hdfs_file_path} ({len(tfrecord_data)} bytes)")

#     except Exception as e:
#         print(f"Error processing message: {e}")
# kafka_tfrecord_consumer.py
import os
import io
import base64
from kafka import KafkaConsumer
from hdfs import InsecureClient
import tensorflow as tf
import json

# Kafka and HDFS config
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "image_stream_tfrecords1"
HDFS_URL = "http://localhost:9870"
HDFS_OUTPUT_DIR = "/data/tfrecords"
HDFS_USER = "abhishek"

# Initialize HDFS client
hdfs_client = InsecureClient(HDFS_URL, user=HDFS_USER)

# Initialize Kafka consumer with deserializer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    group_id="tfrecord_pipeline",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

print("👂 Waiting for messages from Kafka...")

for msg in consumer:
    data = msg.value  # Already deserialized
    try:
        image_bytes = base64.b64decode(data["image_bytes"])
        shape = data["shape"]
        label = data["label"]
        filename = data["filename"]

        # Convert to TFRecord
        def _bytes_feature(value):
            return tf.train.Feature(bytes_list=tf.train.BytesList(value=[value]))

        def _int64_list_feature(value):
            return tf.train.Feature(int64_list=tf.train.Int64List(value=value))

        feature = {
            "image_raw": _bytes_feature(image_bytes),
            "shape": _int64_list_feature(shape),
            "label": _bytes_feature(label.encode("utf-8")),
            "filename": _bytes_feature(filename.encode("utf-8"))
        }

        example = tf.train.Example(features=tf.train.Features(feature=feature))
        serialized_example = example.SerializeToString()

        # Create TFRecord file name
        tfrecord_filename = os.path.splitext(filename)[0] + ".tfrecord"
        hdfs_path = os.path.join(HDFS_OUTPUT_DIR, tfrecord_filename)

        # Write to HDFS
        with hdfs_client.write(hdfs_path, overwrite=True) as writer:
            writer.write(serialized_example)

        print(f"✅ TFRecord written to HDFS: {hdfs_path}")

    except Exception as e:
        print(f"❌ Error processing message: {e}")
