# # import sys
# # import numpy as np
# # import os
# # import io         
# # from tfrecord.torch.dataset import TFRecordDataset
# # from PIL import Image
# # import torch
# # from hdfs import InsecureClient


# # # ---------------- CONFIGURATION ---------------
# # HDFS_URL = "http://localhost:9870"
# # HDFS_USER = "abhishek"
# # SAVE_DIR = "/home/abhishek/Projects/pypro/airflow_tfrecord_spark/tfrecord_pipeline_project/test_received_pt"
# # os.makedirs(SAVE_DIR, exist_ok=True)
# # BATCH_SIZE = 32
# # RESIZE_SHAPE = (128, 128)

# # # -------- CLI ARG: TFRecord Path on HDFS -----
# # if len(sys.argv) != 2:
# #     print("Usage: python fetching_pt.py <hdfs_tfrecord_path>")
# #     sys.exit(1)

# # HDFS_FILE_PATH = sys.argv[1]
# # LOCAL_TMP_FILE = f"/tmp/{os.path.basename(HDFS_FILE_PATH).replace('/', '_')}"

# # # --------- DOWNLOAD FILE FROM HDFS ---------
# # client = InsecureClient(HDFS_URL, user=HDFS_USER)
# # print(f"Downloading from HDFS: {HDFS_FILE_PATH}")
# # client.download(HDFS_FILE_PATH, LOCAL_TMP_FILE, overwrite=True)
# # print("Download complete.")

# # # -------- TFRecord Feature Schema ----------
# # description = {
# #     'filename': "byte",
# #     'image_raw': "byte",
# #     'relative_path': "byte",
# #     'parent': "byte"
# # }

# # # -------- CUSTOM TRANSFORM FUNCTION --------
# # def decode_and_resize(example):
# #     img_bytes = example['image_raw']
    
# #     # Force image to RGB
# #     img = Image.open(io.BytesIO(img_bytes)).convert("RGB")
# #     img = img.resize(RESIZE_SHAPE)

# #     # Convert to NumPy then to tensor (safer)
# #     img_np = np.array(img)  # (H, W, 3)
# #     img_tensor = torch.from_numpy(img_np).permute(2, 0, 1).byte()  # (C, H, W)

# #     return {
# #         "image": img_tensor,
# #         "filename": example["filename"].decode(),
# #         "relative_path": example["relative_path"].decode()
# #     }


# # # --------- PyTorch Dataset from TFRecord ---------
# # dataset = TFRecordDataset(
# #     LOCAL_TMP_FILE,
# #     None,             # Index path, provide if you generated .index
# #     description=description,
# #     transform=decode_and_resize,
# #     shuffle_queue_size=0
# # )

# # loader = torch.utils.data.DataLoader(dataset, batch_size=BATCH_SIZE)

# # # ------------ Extraction: Save Images Locally ------------
# # for batch in loader:
# #     for i in range(batch['image'].size(0)):
# #         img = batch['image'][i]  # Tensor shape (C, H, W)
# #         filename = batch['filename'][i]
# #         relpath = batch['relative_path'][i]
# #         img_np = img.permute(1, 2, 0).numpy()
# #         save_dir = os.path.join(SAVE_DIR, os.path.dirname(relpath))
# #         os.makedirs(save_dir, exist_ok=True)
# #         out_path = os.path.join(SAVE_DIR, relpath)
# #         Image.fromarray(img_np).save(out_path)
# #         print(f"Saved image: {out_path}")

# # print("Done PyTorch TFRecord fetching and preprocessing.")
# import sys
# import os
# import io
# import numpy as np
# from hdfs import InsecureClient
# from tfrecord.torch.dataset import TFRecordDataset
# from PIL import Image
# import torch
# from torch.utils.data import DataLoader

# # ------- CONFIGURATION -------
# HDFS_URL = "http://localhost:9870"
# HDFS_USER = "abhishek"
# SAVE_DIR = "/home/abhishek/Projects/pypro/airflow_tfrecord_spark/tfrecord_pipeline_project/v2_test_received"
# os.makedirs(SAVE_DIR, exist_ok=True)
# BATCH_SIZE = 32
# RESIZE_SHAPE = (128, 128)
# NUM_WORKERS = os.cpu_count        # Tune according to your CPU (use os.cpu_count() for max parallelism)
# PREFETCH_FACTOR = 4     # Default in PyTorch is 2; increase if you want deeper pipeline

# # ------- CLI ARG: TFRecord Path on HDFS --------
# if len(sys.argv) != 2:
#     print("Usage: python fetching_pt.py <hdfs_tfrecord_path>")
#     sys.exit(1)

# HDFS_FILE_PATH = sys.argv[1]
# LOCAL_TMP_FILE = f"/tmp/{os.path.basename(HDFS_FILE_PATH).replace('/', '_')}"

# # ------- DOWNLOAD FILE FROM HDFS -----------
# client = InsecureClient(HDFS_URL, user=HDFS_USER)
# print(f"Downloading from HDFS: {HDFS_FILE_PATH}")
# client.download(HDFS_FILE_PATH, LOCAL_TMP_FILE, overwrite=True)
# print("Download complete.")

# # ------- TFRecord Feature Schema ----------
# description = {
#     'filename': "byte",
#     'image_raw': "byte",
#     'relative_path': "byte",
#     'parent': "byte"
# }

# # ------- CUSTOM TRANSFORM FUNCTION --------
# def decode_and_resize(example):
#     img_bytes = example['image_raw']
#     img = Image.open(io.BytesIO(img_bytes)).convert("RGB")
#     img = img.resize(RESIZE_SHAPE, Image.BILINEAR)   # Match TensorFlow's default
#     img_np = np.array(img)
#     img_tensor = torch.from_numpy(img_np).permute(2, 0, 1).byte()
#     return {
#         "image": img_tensor,
#         "filename": example["filename"].decode(),
#         "relative_path": example["relative_path"].decode()
#     }

# # ------- PyTorch Dataset from TFRecord ----------
# dataset = TFRecordDataset(
#     LOCAL_TMP_FILE,
#     None,  # Index path, if available, can speed up random access
#     description=description,
#     transform=decode_and_resize,
#     shuffle_queue_size=0
# )

# # --------- DataLoader with Parallelism and Prefetch --------
# loader = DataLoader(
#     dataset,
#     batch_size=BATCH_SIZE,
#     num_workers=NUM_WORKERS,
#     prefetch_factor=PREFETCH_FACTOR,
#     pin_memory=True    # Useful for GPU; harmless for CPU
# )

# # --------- Extraction: Save Images Locally ---------
# for batch in loader:
#     for i in range(batch["image"].size(0)):
#         img = batch["image"][i]             # (C, H, W) tensor
#         relpath = batch["relative_path"][i]
#         img_np = img.permute(1, 2, 0).numpy()
#         # Reconstruct local folder structure
#         save_dir = os.path.join(SAVE_DIR, os.path.dirname(relpath))
#         os.makedirs(save_dir, exist_ok=True)
#         out_path = os.path.join(SAVE_DIR, relpath)
#         Image.fromarray(img_np).save(out_path)
#         print(f"Saved image: {out_path}")

# print("Done PyTorch TFRecord fetching and preprocessing.")

import sys
import os
import io
import numpy as np
from hdfs import InsecureClient
from tfrecord.torch.dataset import TFRecordDataset
from PIL import Image
import torch
from torch.utils.data import DataLoader

# ------- CONFIGURATION -------
HDFS_URL = "http://localhost:9870"
HDFS_USER = "abhishek"
SAVE_DIR = "/home/abhishek/Projects/pypro/airflow_tfrecord_spark/tfrecord_pipeline_project/v3_test_received_pth"
os.makedirs(SAVE_DIR, exist_ok=True)
BATCH_SIZE = 32
RESIZE_SHAPE = (128, 128)
NUM_WORKERS = os.cpu_count()       # Use all CPU cores
PREFETCH_FACTOR = 4                # Deeper prefetching

# ------- CLI ARG: TFRecord Path on HDFS --------
if len(sys.argv) != 2:
    print("Usage: python fetching_pt.py <hdfs_tfrecord_path>")
    sys.exit(1)

HDFS_FILE_PATH = sys.argv[1]
LOCAL_TMP_FILE = f"/tmp/{os.path.basename(HDFS_FILE_PATH).replace('/', '_')}"

# ------- DOWNLOAD FILE FROM HDFS -----------
client = InsecureClient(HDFS_URL, user=HDFS_USER)
print(f"Downloading from HDFS: {HDFS_FILE_PATH}")
client.download(HDFS_FILE_PATH, LOCAL_TMP_FILE, overwrite=True)
print("Download complete.")

# ------- TFRecord Feature Schema ----------
description = {
    'filename': "byte",
    'image_raw': "byte",
    'relative_path': "byte",
    'parent': "byte"
}

# ------- CUSTOM TRANSFORM FUNCTION --------
def decode_and_resize(example):
    img_bytes = example['image_raw']
    img = Image.open(io.BytesIO(img_bytes)).convert("RGB")
    img = img.resize(RESIZE_SHAPE, Image.BILINEAR)   # Match TensorFlow's default
    img_np = np.array(img)
    img_tensor = torch.from_numpy(img_np).permute(2, 0, 1).byte()
    return {
        "image": img_tensor,
        "filename": example["filename"].decode(),
        "relative_path": example["relative_path"].decode()
    }

# ------- PyTorch Dataset from TFRecord ----------
dataset = TFRecordDataset(
    LOCAL_TMP_FILE,
    None,  # Index path, if available
    description=description,
    transform=decode_and_resize,
    shuffle_queue_size=0
)

# --------- DataLoader with Parallelism, Prefetch, Persistent Workers --------
loader = DataLoader(
    dataset,
    batch_size=BATCH_SIZE,
    num_workers=NUM_WORKERS,
    prefetch_factor=PREFETCH_FACTOR,
    pin_memory=True,            # For fast GPU transfer (harmless for CPU)
    persistent_workers=True     # Keeps workers alive for repeated iterations
)

# --------- Extraction: Save Images Locally ---------
for batch in loader:
    for i in range(batch["image"].size(0)):
        img = batch["image"][i]             # (C, H, W) tensor
        relpath = batch["relative_path"][i]
        img_np = img.permute(1, 2, 0).numpy()
        save_dir = os.path.join(SAVE_DIR, os.path.dirname(relpath))
        os.makedirs(save_dir, exist_ok=True)
        out_path = os.path.join(SAVE_DIR, relpath)
        Image.fromarray(img_np).save(out_path)
        print(f"Saved image: {out_path}")

print("Done PyTorch TFRecord fetching and preprocessing.")
