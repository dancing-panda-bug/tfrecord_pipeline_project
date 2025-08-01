import sys
import os
import io
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
from PIL import Image

# ----------- CLI ARGUMENTS -----------
if len(sys.argv) < 2 or len(sys.argv) > 3:
    print("Usage: spark-submit spark_preprocess.py <hdfs_tfrecord_path> [<local_save_dir>]")
    print("Example: spark-submit spark_preprocess.py hdfs:///user/abhishek/M60.tfrecord ./images_out")
    sys.exit(1)

HDFS_TFRECORD_PATH = sys.argv[1]
LOCAL_SAVE_BASEDIR = "/home/abhishek/Projects/tfrecord_pipeline_copy/tfrecord_pipeline_project/test_received"
os.makedirs(LOCAL_SAVE_BASEDIR, exist_ok=True)

# ---------- SPARK SESSION ----------
spark = SparkSession.builder.appName("TFRecordPreprocessing").getOrCreate()

# ---------- LOAD TFRECORD ----------
df = spark.read.format("tfrecord") \
    .option("recordType", "Example") \
    .load(HDFS_TFRECORD_PATH)

# ---------- UDF: DECODE, RESIZE, SAVE ----------
def decode_resize_save_image(image_raw, filename, relative_path):
    try:
        image = Image.open(io.BytesIO(image_raw))
        image = image.resize((128, 128))  # Resize to desired dimensions
        save_path = os.path.join(LOCAL_SAVE_BASEDIR, relative_path)
        save_dir = os.path.dirname(save_path)
        os.makedirs(save_dir, exist_ok=True)
        image.save(save_path)
        return f"Saved: {save_path}"
    except Exception as e:
        return f"Failed: {filename} with error {str(e)}"

resize_save_udf = udf(decode_resize_save_image, StringType())

# ---------- APPLY UDF ----------
result_df = df.withColumn(
    "save_status",
    resize_save_udf(col("image_raw"), col("filename"), col("relative_path"))
)

# ---------- EXECUTE AND PRINT ----------
results = result_df.select("save_status").collect()
for row in results:
    print(row["save_status"])

spark.stop()
