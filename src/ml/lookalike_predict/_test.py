#!/usr/bin/env python3
# encoding: utf-8
from crowd_expansion import crowd_expansion

seeds_table_name = 'algorithm.aip_zq_lookalike_seeds_crowd'
user_table_name = 'algorithm.aip_zq_lookalike_predict_crowd'
user_embedding_table_name = 'algorithm.aip_zq_lookalike_user_vec'
result_table_name = '/user/aip/aip/lookalike/result/'
crowd_expansion(user_embedding_table_name, seeds_table_name, user_table_name, result_table_name)