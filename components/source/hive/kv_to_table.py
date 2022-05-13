import logging

from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, StringType

from common.spark_helper import build_spark_session


def _to_row(index_and_value_map, max_index):
    _map = {f"key_{x + 1}": None for x in range(max_index)}
    for idx, value in index_and_value_map.items():
        if idx > max_index:
            continue
        key = f"key_{idx}"
        _map[key] = value
    row = Row(**_map)
    return row


def _line_to_kv(line):
    vals = line.split(",")
    index_and_val_map = {}
    for k_v in vals:
        k, v = k_v.split(":")
        k = int(k)
        index_and_val_map[k] = float(v)
    return index_and_val_map


def _kv_to_line(index_and_value_map):
    line = ""
    for k, v in index_and_value_map.items():
        if v:
            line += f"{k}:{v},"
    line = line[:-1]
    return line


def kv_to_hive(kv_map_file, table_name, dataset):
    spark = build_spark_session("table_to_hive")
    line_rdd = spark.sparkContext().textFile(kv_map_file)
    idx_and_val_map_rdd = line_rdd.map(_line_to_kv)
    _max_index = idx_and_val_map_rdd.flatMap(lambda _map: [k for k, v in _map.items()]).max()
    row_rdd = idx_and_val_map_rdd.map(_to_row)
    schema = StructType([StructField(f"kv_{x + 1}", StringType(), True) for x in range(_max_index)])
    spark.createDataFrame(row_rdd, schema=schema).write.saveAsTable(f"{dataset}.{table_name}")
    spark.stop()


def hive_to_kv(table_name, dataset, kv_map_file):
    spark = build_spark_session("hive_to_kv")
    row_rdd = spark.sql(f"SELECT * FROM {dataset}.{table_name}").rdd()
    row_rdd.map(lambda row: row.asDict()) \
        .map(_kv_to_line).saveAsTextFile(kv_map_file)
    spark.stop()


