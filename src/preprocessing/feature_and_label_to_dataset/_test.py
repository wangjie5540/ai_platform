#!/usr/bin/env python3
# encoding: utf-8

from feature_and_label_to_dataset import feature_and_label_to_dataset

label_table_name = "algorithm.tmp_aip_model_sample"
model_user_feature_table_name = "algorithm.tmp_model_user_feature_table_name"
model_item_feature_table_name = "algorithm.tmp_model_item_feature_table_name"
train_dataset_table_name = "algorithm.train_dataset_table_name"
test_dataset_table_name = "algorithm.test_dataset_table_name"

train_dataset_table_name, test_dataset_table_name = feature_and_label_to_dataset(label_table_name, model_user_feature_table_name, model_item_feature_table_name,
                                 train_dataset_table_name, test_dataset_table_name, train_p=0.8)
print(train_dataset_table_name, test_dataset_table_name)
