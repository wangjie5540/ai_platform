# 参考：https://spark.apache.org/docs/3.3.1/api/python/reference/api/pyspark.ml.feature.StringIndexer.html


import transformer

rules = [
    {
        "type": "string_indexer",
        "input_col": "user_id",
        "output_col": "user_id_index",
    },
    {
        "type": "string_indexer",
        "input_col": "gender",
        "output_col": "gender_index",
    },
    {
        "type": "string_indexer",
        "input_col": "EDU",
        "output_col": "EDU_index",
    },
    {
        "type": "standard_scaler",
        "input_col": "u_amount_sum_30d",
        "output_col": "u_amount_sum_30d_scaled",
    },
    {
        "type": "standard_scaler",
        "input_col": "u_amount_avg_30d",
        "output_col": "u_amount_avg_30d_scaled",
        "need_keep": True,
    },
]
# transformer.transform(table_name="algorithm.tmp_raw_user_feature_table_name_1", transform_rules=rules)
transformer.create(table_name="algorithm.tmp_raw_user_feature_table_name_1", transform_rules=rules,
                   name='user_feature_transformers')
