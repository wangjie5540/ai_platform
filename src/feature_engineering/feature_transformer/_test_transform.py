import transformer

transformers = ['user_id', 'gender', 'EDU', 'u_amount_sum_30d', 'u_amount_avg_30d']
transformer.transform('algorithm.tmp_raw_user_feature_table_name_1', transform_rules=transformers,
                      name='user_feature_transformers')
