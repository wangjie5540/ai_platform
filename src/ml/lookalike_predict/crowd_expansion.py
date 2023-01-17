#!/usr/bin/env python3
# encoding: utf-8
from digitforce.aip.common.utils import spark_helper
from digitforce.aip.common.utils.hdfs_helper import hdfs_client
from sklearn.cluster import KMeans
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
import pandas as pd
import os


def crowd_expansion(user_vec_table_name, seeds_crowd_table_name, predict_crowd_table_name, result_hdfs_path):
    spark_client = spark_helper.SparkClient()
    seed_crowd = spark_client.get_session().sql(
        f"""select t1.user_id, t2.user_vec from {seeds_crowd_table_name} t1 left join {user_vec_table_name} t2 on t1.user_id = t2.user_id""").toPandas()
    filter_crowd = spark_client.get_session().sql(
        f"""select t1.user_id, t2.user_vec from {predict_crowd_table_name} t1 left join {user_vec_table_name} t2 on t1.user_id = t2.user_id""").toPandas()

    filter_crowd = filter_crowd.append(seed_crowd)
    filter_crowd = filter_crowd.append(seed_crowd)
    filter_crowd = filter_crowd.drop_duplicates(subset=['user_id', 'user_vec'], keep=False)

    seed_crowd["user_vec"] = seed_crowd["user_vec"].str[1:-1]
    filter_crowd["user_vec"] = filter_crowd["user_vec"].str[1:-1]
    seed_crowd.set_index('user_id', inplace=True)
    filter_crowd.set_index('user_id', inplace=True)
    seed_crowd = seed_crowd['user_vec'].str.split(',', expand=True)
    filter_crowd = filter_crowd['user_vec'].str.split(',', expand=True)
    result = get_expansion_result(seed_crowd, filter_crowd)
    result.to_csv("result.csv",index=False)
    result_hdfs_path = os.path.join(result_hdfs_path, "result.csv")
    if hdfs_client.exists(result_hdfs_path):
        hdfs_client.delete(result_hdfs_path)
    hdfs_client.copy_from_local("result.csv", result_hdfs_path)
    # result_dataframe = spark_client.get_session().createDataFrame(result)
    # result_dataframe.write.format("hive").mode("overwrite").saveAsTable(result_hdfs_path)


def get_expansion_result(seed_crowd_vec, filter_crowd_vec):
    kmeans = KMeans(n_clusters=2)
    kmeans.fit(seed_crowd_vec)
    clusters = pd.DataFrame(kmeans.cluster_centers_)
    cal_result = pd.DataFrame(cosine_similarity(clusters, filter_crowd_vec).T, index=filter_crowd_vec.index)
    result = cal_result.apply(np.max, axis=1)
    result.sort_values(ascending=False, inplace=True)
    result = pd.DataFrame(result, columns=['score']).reset_index()
    return result
