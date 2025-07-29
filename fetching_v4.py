import sys
import os
import tensorflow as tf
from hdfs import InsecureClient
from PIL import Image

# ---------- CONFIGURATION ----------
HDFS_URL = "http://localhost:9870"
HDFS_USER = "abhishek"
SAVE_DIR = "/home/abhishek/Projects/tfrecord_pipeline_copy/tfrecord_pipeline_project/received_fulldataset"  
os.makedirs(SAVE_DIR, exist_ok=True)

# ---------- CLI ARGUMENT: HDFS TFRecord PATH ----------
if len(sys.argv) != 2:
    print("Usage: python fetch_and_reconstruct.py <hdfs_tfrecord_path>")
    print("Example: python fetch_and_reconstruct.py /user/abhishek/M60.tfrecord")
    sys.exit(1)

HDFS_FILE_PATH = sys.argv[1]
# Temporary file for downloaded TFRecord
LOCAL_TMP_FILE = f"/tmp/{os.path.basename(HDFS_FILE_PATH).replace('/', '_')}"

# ---------- DOWNLOAD FROM HDFS ----------
client = InsecureClient(HDFS_URL, user=HDFS_USER)
print(f"Downloading from HDFS: {HDFS_FILE_PATH}")
client.download(HDFS_FILE_PATH, LOCAL_TMP_FILE, overwrite=True)
print("Download complete.")

# ---------- TFRecord PARSING ----------
feature_description = {
    'filename': tf.io.FixedLenFeature([], tf.string),
    'image_raw': tf.io.FixedLenFeature([], tf.string),
    'relative_path': tf.io.FixedLenFeature([], tf.string),
    'parent': tf.io.FixedLenFeature([], tf.string)
}
def _parse_function(example_proto):
    return tf.io.parse_single_example(example_proto, feature_description)

raw_dataset = tf.data.TFRecordDataset(LOCAL_TMP_FILE)
parsed_dataset = raw_dataset.map(_parse_function)

# ---------- IMAGE EXTRACTION ----------
for i, parsed in enumerate(parsed_dataset):
    filename = parsed['filename'].numpy().decode()
    image_raw = parsed['image_raw'].numpy()
    rel_path = parsed['relative_path'].numpy().decode()

    image_tensor = tf.io.decode_image(image_raw, channels=0)
    image_array = tf.squeeze(image_tensor).numpy().astype("uint8")

    # Rebuild original directory structure
    img_dir = os.path.join(SAVE_DIR, os.path.dirname(rel_path))
    os.makedirs(img_dir, exist_ok=True)
    out_path = os.path.join(SAVE_DIR, rel_path)
    Image.fromarray(image_array).save(out_path)
    print(f"Saved image: {out_path}")

print("All images extracted and saved.")
