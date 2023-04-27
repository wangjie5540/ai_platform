from model_train import start_model_train

train_table_name = "algorithm.aip_zq_dixiaohu_custom_feature_train_dev"
test_table_name = "algorithm.aip_zq_dixiaohu_custom_feature_test_dev"
model_and_metrics_data_hdfs_path = "/user/ai/aip/model/222"

start_model_train(
    train_table_name,
    test_table_name,
    learning_rate=0.05,
    n_estimators=200,
    max_depth=5,
    scale_pos_weight=0.5,
    is_automl=False,
    model_and_metrics_data_hdfs_path=model_and_metrics_data_hdfs_path,
)
