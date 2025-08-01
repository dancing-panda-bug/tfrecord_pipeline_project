import sys
import os
import tensorflow as tf
from hdfs import InsecureClient
from PIL import Image

# --- CONFIGURATIONS ---
HDFS_URL = "http://localhost:9870"
HDFS_USER = "abhishek"
SAVE_DIR = "/home/abhishek/Projects/pypro/airflow_tfrecord_spark/tfrecord_pipeline_project/test_received"
os.makedirs(SAVE_DIR, exist_ok=True)

BATCH_SIZE = 32
RESIZE_SHAPE = (128, 128)  # Change as needed

# --- CLI ARGUMENT CHECK ---
if len(sys.argv) != 2:
    print("Usage: python fetching.py <hdfs_tfrecord_path>")
    print("Example: python fetching.py /user/abhishek/M60.tfrecord")
    sys.exit(1)

HDFS_FILE_PATH = sys.argv[1]
LOCAL_TMP_FILE = f"/tmp/{os.path.basename(HDFS_FILE_PATH).replace('/', '_')}"

# --- DOWNLOAD ---
client = InsecureClient(HDFS_URL, user=HDFS_USER)
print(f"Downloading from HDFS: {HDFS_FILE_PATH}")
client.download(HDFS_FILE_PATH, LOCAL_TMP_FILE, overwrite=True)
print("Download complete.")

# --- TFRecord PARSER & PIPELINE ---
feature_description = {
    'filename': tf.io.FixedLenFeature([], tf.string),
    'image_raw': tf.io.FixedLenFeature([], tf.string),
    'relative_path': tf.io.FixedLenFeature([], tf.string),
    'parent': tf.io.FixedLenFeature([], tf.string)
}

def parse_and_resize(example_proto):
    features = tf.io.parse_single_example(example_proto, feature_description)
    image = tf.io.decode_image(features['image_raw'], channels=3, expand_animations=False)
    image = tf.image.resize(image, RESIZE_SHAPE)
    # Uncomment for optional rotation (180 deg): image = tf.image.rot90(image, k=2)
    image = tf.cast(image, tf.uint8)
    return image, features['filename'], features['relative_path']

raw_dataset = tf.data.TFRecordDataset(LOCAL_TMP_FILE)

# PARALLELIZE and prefetch; uses all available CPU threads.
dataset = (
    raw_dataset
    .map(parse_and_resize, num_parallel_calls=tf.data.AUTOTUNE)
    .batch(BATCH_SIZE)
    .prefetch(tf.data.AUTOTUNE)
)

# --- EXTRACTION LOOP ---
for batch_images, batch_filenames, batch_relpaths in dataset:
    imgs = batch_images.numpy()
    fnames = batch_filenames.numpy()
    relpaths = batch_relpaths.numpy()
    for img_np, fname_bytes, relpath_bytes in zip(imgs, fnames, relpaths):
        fname = fname_bytes.decode("utf-8")
        relpath = relpath_bytes.decode("utf-8")
        img_save_dir = os.path.join(SAVE_DIR, os.path.dirname(relpath))
        os.makedirs(img_save_dir, exist_ok=True)
        out_path = os.path.join(SAVE_DIR, relpath)
        Image.fromarray(img_np).save(out_path)
        print(f"Saved image: {out_path}")

print("Done Preprocessing.")
