from train_deepfm import start_train

train_data_table_name = "algorithm.tmp_aip_train_data_gaoqian"
train_data_columns = ['user_id', 'label',
                  'u_event1_counts_30d',
                  'u_event1_amount_sum_30d',
                  'u_event1_amount_avg_30d',
                  'u_event1_amount_min_30d',
                  'u_event1_amount_max_30d',
                  'u_event1_days_30d',
                  'u_event1_avg_days_30d',
                  'u_last_event1_days_30d',
                  'gender', 'EDU', 'RSK_ENDR_CPY', 'NATN', 'OCCU', 'IS_VAIID_INVST'
                  ]
test_data_table_name = "algorithm.tmp_aip_test_data_gaoqian"
lr = 0.005
weight_decay = 0.001
hdfs_path = "/tmp/pycharm_project_19/src/preprocessing/sample_comb_gaoqian/"
start_train(train_data_table_name, train_data_columns, test_data_table_name, hdfs_path, lr, weight_decay)