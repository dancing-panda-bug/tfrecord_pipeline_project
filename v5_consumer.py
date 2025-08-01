import pickle
import os
import tempfile
import tensorflow as tf
from kafka import KafkaConsumer
from hdfs import InsecureClient

KAFKA_TOPIC = "image_stream_tfrecordss"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
HDFS_URL = "http://localhost:9870"
HDFS_USER = "abhishek"

def create_tf_example(image_bytes, filename, relative_path, parent):
    return tf.train.Example(features=tf.train.Features(feature={
        "image_raw": tf.train.Feature(bytes_list=tf.train.BytesList(value=[image_bytes])),
        "filename": tf.train.Feature(bytes_list=tf.train.BytesList(value=[filename.encode()])),
        "relative_path": tf.train.Feature(bytes_list=tf.train.BytesList(value=[relative_path.encode()])),
        "parent": tf.train.Feature(bytes_list=tf.train.BytesList(value=[parent.encode()]))
    }))

def write_tfrecord_to_hdfs(tf_examples, hdfs_rel_path):
    tmp = tempfile.NamedTemporaryFile(delete=False)
    try:
        with tf.io.TFRecordWriter(tmp.name) as writer:
            for ex in tf_examples:
                writer.write(ex.SerializeToString())

        full_hdfs_path = f"/user/test/{HDFS_USER}/{hdfs_rel_path}"
        client = InsecureClient(HDFS_URL, user=HDFS_USER)
        client.makedirs(os.path.dirname(full_hdfs_path))  
        client.upload(full_hdfs_path, tmp.name, overwrite=True)
        print(f"TFRecord ({len(tf_examples)} images) uploaded to HDFS: {full_hdfs_path}")
    finally:
        tmp.close()
        if os.path.exists(tmp.name): os.remove(tmp.name)

if __name__ == "__main__":
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True
    )
    print("Kafka Consumer started...")
    image_buffers = {}  # parent -> list of image dicts

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
                print(f"Buffered {data['filename']} for parent: {parent}")

            elif data["type"] == "parent_done":
                parent = data["parent"]
                buf = image_buffers.pop(parent, [])
                if not buf:
                    print(f"No images buffered for PARENT_DONE: {parent}, skipping TFRecord write.")
                    continue

                tf_examples = [
                    create_tf_example(img["image_bytes"], img["filename"], img["relative_path"], img["parent"])
                    for img in buf
                ]

                # Parse parent into folder structure
                parent_parts = parent.strip("/").split("/")  
                if len(tf_examples) == 1:
                    # One image → /parentfolder/subfolder/filename.tfrecord
                    subdir = "/".join(parent_parts)
                    filename = buf[0]["filename"].split(".")[0] + ".tfrecord"
                    hdfs_rel_path = f"{subdir}/{filename}"
                else:
                    # Multiple images → /parentfolder/subfolder.tfrecord
                    if len(parent_parts) >= 2:
                        hdfs_rel_path = f"{parent_parts[0]}/{parent_parts[1]}.tfrecord"
                    else:
                        hdfs_rel_path = f"{parent}.tfrecord"

                write_tfrecord_to_hdfs(tf_examples, hdfs_rel_path)

    except Exception as e:
        print(f"❌ Error in consumer: {e}")
    finally:
        consumer.close()
