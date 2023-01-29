#!/usr/bin/env python3
# encoding: utf-8
from crowd_expansion import crowd_expansion

seeds_table_name = 'algorithm.aip_zq_lookalike_seeds_crowd'
user_table_name = 'algorithm.aip_zq_lookalike_predict_crowd'
user_embedding_table_name = 'algorithm.aip_zq_lookalike_user_vec'
output_file_name = 'result.csv'
output_file_path = crowd_expansion(user_embedding_table_name, seeds_table_name, user_table_name, output_file_name)
print(output_file_path)