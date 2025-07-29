    # test_hdfs_connect.py
from pyarrow import fs
hdfs = fs.HadoopFileSystem('default')
print("✅ Connected to HDFS")
