import sys
import pickle
import os
import tempfile
import tensorflow as tf
from kafka import KafkaConsumer
from hdfs import InsecureClient

KAFKA_TOPIC = "image_stream_tfrecords1"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
HDFS_URL = "http://localhost:9870"
HDFS_USER = "abhishek"  # Update if needed

def create_tf_example(image_bytes, filename, relative_path, parent):
    feature = {
        "image_raw": tf.train.Feature(bytes_list=tf.train.BytesList(value=[image_bytes])),
        "filename": tf.train.Feature(bytes_list=tf.train.BytesList(value=[filename.encode()])),
        "relative_path": tf.train.Feature(bytes_list=tf.train.BytesList(value=[relative_path.encode()])),
        "parent": tf.train.Feature(bytes_list=tf.train.BytesList(value=[parent.encode()]))
    }
    return tf.train.Example(features=tf.train.Features(feature=feature))

def write_tfrecord_to_hdfs(tf_examples, tfrecord_filename):
    local_temp_file = tempfile.NamedTemporaryFile(delete=False)
    tmp_name = local_temp_file.name
    try:
        with tf.io.TFRecordWriter(tmp_name) as writer:
            for example in tf_examples:
                writer.write(example.SerializeToString())
        hdfs_path = f"/user/{HDFS_USER}/{tfrecord_filename}"
        client = InsecureClient(HDFS_URL, user=HDFS_USER)
        client.upload(hdfs_path, tmp_name, overwrite=True)
        print(f"📁 Written TFRecord with {len(tf_examples)} images to HDFS at {hdfs_path}")
    finally:
        local_temp_file.close()
        if os.path.exists(tmp_name):
            os.remove(tmp_name)
            print(f"🧹 Deleted temp file {tmp_name}")

if __name__ == "__main__":
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True
    )

    print("📥 Kafka Consumer started...")
    image_buffers = {}  # key: parent folder, value: list of images

    try:
        for msg in consumer:
            data = pickle.loads(msg.value)
            if data["type"] == "image":
                parent = data["parent"]
                buf = image_buffers.setdefault(parent, [])
                buf.append({
                    "image_bytes": data["image_bytes"],
                    "filename": data["filename"],
                    "relative_path": data["relative_path"],
                    "parent": parent
                })
                print(f"📩 Received image: {data['filename']} (buffered for parent: {parent})")
            elif data["type"] == "parent_done":
                parent = data["parent"]
                buf = image_buffers.pop(parent, [])
                if buf:
                    tf_examples = [
                        create_tf_example(img["image_bytes"], img["filename"], img["relative_path"], img["parent"])
                        for img in buf
                    ]
                    tfrecord_filename = f"{parent.replace('/', '_')}.tfrecord"
                    write_tfrecord_to_hdfs(tf_examples, tfrecord_filename)
                    print(f"✅ Written {len(tf_examples)} images for parent '{parent}' to HDFS and cleared buffer")
                else:
                    print(f"⚠️ No images buffered for PARENT_DONE: {parent}, skipping TFRecord write.")
    except Exception as e:
        print(f"❌ Error in consumer: {e}")
    finally:
        consumer.close()
