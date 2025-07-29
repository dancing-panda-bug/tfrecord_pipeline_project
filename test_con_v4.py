import os
import io
import pickle
import sys
from kafka import KafkaConsumer
from collections import defaultdict
import tensorflow as tf
from hdfs import InsecureClient

KAFKA_TOPIC = "image_stream_tfrecordss"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
HDFS_URL = "http://localhost:9870"
HDFS_USER = "abhishek"

client = InsecureClient(HDFS_URL, user="abhishek")

buffered_data = defaultdict(list)

def serialize_example(image_bytes, metadata):
    feature = {
        "image": tf.train.Feature(bytes_list=tf.train.BytesList(value=[image_bytes])),
        "filename": tf.train.Feature(bytes_list=tf.train.BytesList(value=[metadata["filename"].encode()])),
        "path": tf.train.Feature(bytes_list=tf.train.BytesList(value=[metadata["path"].encode()])),
        "shape": tf.train.Feature(int64_list=tf.train.Int64List(value=metadata["shape"])),
        "label": tf.train.Feature(bytes_list=tf.train.BytesList(value=[metadata["label"].encode()])),
    }
    example_proto = tf.train.Example(features=tf.train.Features(feature=feature))
    return example_proto.SerializeToString()

def write_to_hdfs(data_list, hdfs_path):
    with io.BytesIO() as buffer:
        with tf.io.TFRecordWriter(buffer) as writer:
            for item in data_list:
                tf_example = serialize_example(item["image_bytes"], item["metadata"])
                writer.write(tf_example)
        buffer.seek(0)
        client.write(hdfs_path, data=buffer, overwrite=True)

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=True
)

for msg in consumer:
    message = pickle.loads(msg.value)

    if message["type"] == "image":
        metadata = message["metadata"]
        image_bytes = message["image_bytes"]
        folder_key = metadata["path"]  # e.g., M60/0

        buffered_data[folder_key].append({
            "image_bytes": image_bytes,
            "metadata": metadata
        })

        print(f"📩 Buffered {metadata['filename']} for parent: {folder_key}")

    elif message["type"] == "parent_done":
        folder_key = message["path"]  # e.g., M60/0
        images = buffered_data.get(folder_key, [])

        if not images:
            print(f"⚠️ No images buffered for parent: {folder_key}")
            continue

        print(f"📝 Writing TFRecord for parent: {folder_key}")

        if len(images) == 1:
            img = images[0]
            image_name = os.path.splitext(img["metadata"]["filename"])[0]
            hdfs_path = os.path.join(HDFS_OUTPUT_DIR, folder_key, f"{image_name}.tfrecord")
            write_to_hdfs([img], hdfs_path)
            print(f"✅ TFRecord (1 image) uploaded to HDFS: {hdfs_path}")
        else:
            hdfs_path = os.path.join(HDFS_OUTPUT_DIR, f"{folder_key}.tfrecord")
            write_to_hdfs(images, hdfs_path)
            print(f"✅ TFRecord ({len(images)} images) uploaded to HDFS: {hdfs_path}")

        del buffered_data[folder_key]
