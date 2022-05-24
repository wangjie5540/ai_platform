#!/usr/bin/env python3
# encoding: utf-8

import lookalike

user_embedding_path = 'user_embedding_part.json'
seed_file_path = 'seed.csv'
crowd_file_path = 'crowd.csv'

result_path = lookalike.get_crowd_by_seed(user_embedding_path, seed_file_path, crowd_file_path)
print(result_path)