import tensorflow as tf

def _bytes_feature(value):
    return tf.train.Feature(bytes_list=tf.train.BytesList(value=[value]))

def _int64_list_feature(value):
    return tf.train.Feature(int64_list=tf.train.Int64List(value=value))

def create_example(filename, label, shape, image_bytes):
    feature = {
        "filename": _bytes_feature(filename.encode()),
        "label": _bytes_feature(label.encode()),
        "shape": _int64_list_feature(shape),
        "image_raw": _bytes_feature(image_bytes),
    }
    return tf.train.Example(features=tf.train.Features(feature=feature))
