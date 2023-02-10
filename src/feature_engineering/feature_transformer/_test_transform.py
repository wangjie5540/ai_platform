import transformer

transformers = [
    'user_id',
    # 'gender',
    # 'EDU',
    # 'u_amount_sum_30d',
    # 'u_amount_avg_30d',
]
transformer.do_transform('algorithm.wtg_test_feature', transformers=transformers,
                         pipeline_model_path='hdfs://HDFS8001206/user/ai/aip/feature_engineering/feature_transformer_4708259252090703873',
                         transformers_path='/user/ai/aip/feature_engineering/feature_transformer_4708259252090703873/transformers')
