#!/usr/bin/env python3
# encoding: utf-8

import digitforce.aip.common.utils.spark_helper as spark_helper

def sample_comb(sample_table_name, user_feature_table_name, item_feature_table_name):
    spark_client = spark_helper.SparkClient()
    sample = spark_client.get_session().sql(f"select * from {sample_table_name}")
    user_feature = spark_client.get_session().sql(f"select * from {user_feature_table_name}")
    item_feature = spark_client.get_session().sql(f"select * from {item_feature_table_name}")

    col_sample = sample.columns
    user_id_sample = col_sample[0]
    item_id_sample = col_sample[1]

    col_user = user_feature.columns
    user_id = col_user[0]

    col_item = item_feature.columns
    item_id = col_item[0]

    user_feature = user_feature.withColumnRenamed(user_id, user_id_sample)
    item_feature = item_feature.withColumnRenamed(item_id, item_id_sample)

    train_data = sample.join(item_feature, item_id_sample)
    train_data = train_data.join(user_feature, user_id_sample)

    train_data_table_name = "algorithm.tmp_aip_train_data"
    train_data.write.format("hive").mode("overwrite").saveAsTable(train_data_table_name)

    columns = train_data.columns
    encoder_hdfs_path = "test1234"

    return train_data_table_name, columns, encoder_hdfs_path

if __name__ == '__main__':
    pass