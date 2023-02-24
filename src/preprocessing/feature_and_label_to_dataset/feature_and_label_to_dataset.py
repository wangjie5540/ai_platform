#!/usr/bin/env python3
# encoding: utf-8

import logging


import digitforce.aip.common.utils.hdfs_helper as hdfs_helper
from digitforce.aip.common.utils.spark_helper import SparkClient
from pyspark.sql.functions import rand

hdfs_client = hdfs_helper.HdfsClient()


def feature_and_label_to_dataset(label_table_name, model_user_feature_table_name, model_item_feature_table_name,
                                 train_dataset_table_name, test_dataset_table_name, train_p=0.8):
    logging.info("begin join feature and label [FEATURE, LABEL] --> [DATASET]")
    spark_client = SparkClient.get()
    label_dataframe = spark_client.get_session().sql(f"select * from {label_table_name}")
    model_user_feature_dataframe = spark_client.get_session().sql(f"select * from {model_user_feature_table_name}")
    model_item_feature_dataframe = spark_client.get_session().sql(f"select * from {model_item_feature_table_name}")
    #
    dataset = label_dataframe
    dataset = dataset.join(model_user_feature_dataframe, "user_id", "left") \
        .join(model_item_feature_dataframe, "item_id", "left")
    dataset = dataset.withColumn("train_p", rand(2022))
    dataset.cache()
    train_dataset = dataset.filter(dataset.train_p < train_p).drop("train_p")
    train_dataset.write.format("hive").mode("overwrite").saveAsTable(train_dataset_table_name)
    test_dataset = dataset.filter(dataset.train_p >= train_p).drop("train_p")
    test_dataset.write.format("hive").mode("overwrite").saveAsTable(test_dataset_table_name)
    return train_dataset_table_name, test_dataset_table_name


def main():
    feature_and_label_to_dataset("algorithm.tmp_aip_model_sample",
                                 "algorithm.tmp_model_user_feature_table_name",
                                 "algorithm.tmp_model_item_feature_table_name",
                                 "algorithm.tmp_train_dataset_table_name",
                                 "algorithm.tmp_test_dataset_table_name")


if __name__ == '__main__':
    main()
