#!/usr/bin/env python3
# encoding: utf-8

import pandas as pd
import json
from sklearn.cluster import KMeans
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity


def get_crowd_by_seed(user_embedding_path, seed_file_path, crowd_file_path, output_file_path):
    with open(user_embedding_path,'r',encoding='utf8')as fp:
        user_vec = json.load(fp)
    user_vec_df = pd.DataFrame.from_dict(user_vec,orient = 'index')
    user_vec_df.reset_index(inplace=True)
    user_vec_df.rename(columns={'index':'user_id'},inplace=True)
    user_vec_df['user_id'] = user_vec_df['user_id'].astype('string')
    seed_crowd = pd.read_csv(seed_file_path)
    seed_crowd.columns = ['user_id']
    seed_crowd['user_id'] = seed_crowd['user_id'].astype('string')
    expand_crowd = pd.read_csv(crowd_file_path)
    expand_crowd.columns = ['user_id']
    expand_crowd['user_id'] = expand_crowd['user_id'].astype('string')
    expand_crowd = pd.concat([expand_crowd, seed_crowd, seed_crowd]).drop_duplicates(keep=False)
    seed_crowd_vec = pd.merge(seed_crowd, user_vec_df, how='left', on='user_id')
    expand_crowd_vec = pd.merge(expand_crowd, user_vec_df, how='left', on='user_id')
    seed_crowd_vec.set_index('user_id',inplace=True)
    expand_crowd_vec.set_index('user_id',inplace=True)
    seed_crowd_vec = seed_crowd_vec.astype('float64')
    expand_crowd_vec = expand_crowd_vec.astype('float64')
    result = get_expansion_result(seed_crowd_vec, expand_crowd_vec)
    result.to_csv(output_file_path, index=0)


def get_expansion_result(seed_crowd_vec, expand_crowd_vec):
    kmeans = KMeans(n_clusters=5)
    kmeans.fit(seed_crowd_vec)
    clusters = pd.DataFrame(kmeans.cluster_centers_)
    cal_result = pd.DataFrame(cosine_similarity(clusters, expand_crowd_vec).T, index=expand_crowd_vec.index)
    result = cal_result.apply(np.max, axis=1)
    result.sort_values(ascending=False, inplace=True)
    return pd.DataFrame(result.index)
