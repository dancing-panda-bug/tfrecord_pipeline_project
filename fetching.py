import os
import tensorflow as tf
from hdfs import InsecureClient
from PIL import Image

# -----------------------
# HDFS configuration
# ---------------------
HDFS_URL = "http://localhost:9870"
HDFS_USER = "abhishek"
HDFS_FILE_PATH = "/data/demo/batch_de361043d0d84affb8b0e49a60a5151b.tfrecord"  
LOCAL_TMP_FILE = "/tmp/temp.tfrecord"

# -----------------------
# Local save path
# -----------------------
SAVE_DIR = "/home/abhishek/Projects/pypro/tfrecord_pipeline_project/received"
os.makedirs(SAVE_DIR, exist_ok=True)

# -----------------------
# Connect to HDFS and download TFRecord file
# -----------------------
client = InsecureClient(HDFS_URL, user=HDFS_USER)
print(f"Downloading from HDFS: {HDFS_FILE_PATH}")
client.download(HDFS_FILE_PATH, LOCAL_TMP_FILE, overwrite=True)
print("Download complete.")

# -----------------------
# TFRecord feature schema
# -----------------------
feature_description = {
    'filename': tf.io.FixedLenFeature([], tf.string),
    'image_raw': tf.io.FixedLenFeature([], tf.string),
    'shape': tf.io.FixedLenFeature([2], tf.int64),
    'label': tf.io.FixedLenFeature([], tf.string),
}

# -----------------------
# TFRecord parser
# -----------------------
def _parse_function(example_proto):
    return tf.io.parse_single_example(example_proto, feature_description)

# -----------------------
# Read and process TFRecord
# -----------------------
raw_dataset = tf.data.TFRecordDataset(LOCAL_TMP_FILE)
parsed_dataset = raw_dataset.map(_parse_function)

for i, parsed_record in enumerate(parsed_dataset):
    filename = parsed_record['filename'].numpy().decode()
    image_raw = parsed_record['image_raw'].numpy()
    shape = parsed_record['shape'].numpy()
    label = parsed_record['label'].numpy().decode()

    # Decode raw image bytes
    image_tensor = tf.io.decode_image(image_raw, channels=0)
    image_tensor = tf.image.resize(image_tensor, [shape[0], shape[1]])
    image_array = tf.squeeze(image_tensor).numpy().astype("uint8")

    # Save image locally
    local_filename = os.path.join(SAVE_DIR, f"{i}_{filename}")
    Image.fromarray(image_array).save(local_filename)
    print(f"Saved image: {local_filename}")
