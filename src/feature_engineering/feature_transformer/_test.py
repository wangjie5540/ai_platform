from digitforce.aip.common.utils.spark_helper import spark_client

# df = spark_client.get_session().sql(f"select * from algorithm.tmp_raw_user_feature_table_name_1")
# df.show()
# 参考：https://spark.apache.org/docs/3.3.1/api/python/reference/api/pyspark.ml.feature.StringIndexer.html
# from pyspark.ml.feature import StringIndexer, StringIndexerModel

# indexer = StringIndexer(inputCol="EDU", outputCol="EDU_index")
# fit = indexer.fit(df)
# fit.save("hdfs://HDFS8001206/user/ai/aip/wtg_test/EduIndexer")

# loaded_model = StringIndexerModel.load("hdfs://HDFS8001206/user/ai/aip/wtg_test/EduIndexer")
# print(loaded_model.labels)
# indexed = loaded_model.transform(df)
# indexed.select('EDU_index').show()


import transformer

rules = [
    {
        "type": "string_indexer",
        "input_col": "user_id",
        "output_col": "user_id_index",
        "need_keep": True,
    },
    {
        "type": "string_indexer",
        "input_col": "gender",
        "output_col": "gender_index",
        "need_keep": False,
    },
    {
        "type": "string_indexer",
        "input_col": "EDU",
        "output_col": "EDU_index",
        "need_keep": False,
    },
    {
        "type": "standard_scaler",
        "input_col": "u_amount_sum_30d",
        "output_col": "u_amount_sum_30d_scaled",
        "need_keep": True,
    },
    {
        "type": "standard_scaler",
        "input_col": "u_amount_avg_30d",
        "output_col": "u_amount_avg_30d_scaled",
        "need_keep": True,
    },
]
transformer.transform(table_name="algorithm.tmp_raw_user_feature_table_name_1", transform_rules=rules)
