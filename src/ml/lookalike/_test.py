#!/usr/bin/env python3
# encoding: utf-8

import lookalike

user_embedding_path = 'user_embedding_part.json'
seed_file_path = 'seed.csv'
crowd_file_path = 'crowd.csv'
output_file_path = 'result.csv'
lookalike.get_crowd_by_seed(user_embedding_path, seed_file_path, crowd_file_path, output_file_path)
print(output_file_path)