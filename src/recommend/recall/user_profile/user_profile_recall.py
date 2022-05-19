import json
import random

import pandas as pd


def user_profile_random_recall(user_profiles, profile_id_and_hot_item_map, recall_cnt=200):
    '''
    根据用户的兴趣分布召回热门item，召回结果随机打撒
    :param user_profiles: 用户兴趣
    :param profile_id_and_hot_item_map: 兴趣和对应热门item
    :param recall_cnt: 召回数量
    :return:
    '''
    sum_score = sum(user_profiles.values())
    recall_item_ids = []
    for cid, score in user_profiles.items():
        cid = int(cid)
        sampling_cnt = int(score / sum_score * recall_cnt) + 1
        _products = profile_id_and_hot_item_map.get(cid, [])
        sampling_cnt = min((len(_products), sampling_cnt))
        if sampling_cnt:
            _result = _products[:sampling_cnt]
            for item_id in _result:
                recall_item_ids.append(item_id)
    recall_item_ids = recall_item_ids[:recall_cnt]
    random.shuffle(recall_item_ids)
    return recall_item_ids


def offline_recall(input_file, output_file, profile_and_hot_item_file):
    profile_and_hot_item_ids = {}
    with open(profile_and_hot_item_file) as fi:
        for line in fi:
            if line:
                json_obj = json.loads(line)
                profile_id = int(json_obj["profile_id"])
                hot_item_ids = json_obj["hot_item_ids"]
                profile_and_hot_item_ids[profile_id] = hot_item_ids
    fo = open(output_file, "w")
    with open(input_file) as fi:
        for line in fi:
            json_obj = json.loads(line)
            user_id = json_obj["user_id"]
            profiles = json_obj["profile_scores"]
            recall_item_ids = user_profile_random_recall(profiles, profile_and_hot_item_ids)
            fo.write(f"{json.dumps({'user_id': user_id, 'recall_item_ids': recall_item_ids})}\n")
    fo.close()


def agg_hot_product_by_profile(hot_csv_profile, item_and_profile_map_file, profile_and_hot_item_file):
    profile_df = pd.read_csv(item_and_profile_map_file, header=None, names=["profile_id", "item_id"])
    item_and_score_map = dict(zip(profile_df.item_id.astype(int), profile_df.profile_id.astype(int)))
    hot_df = pd.read_csv(hot_csv_profile, header=None, names=["item_id", "score"])
    hot_items = list(zip(hot_df.item_id, hot_df.score.astype(float)))
    hot_items.sort(key=lambda x: -x[1])
    profile_and_hot_item_map = {}
    for item_id, score in hot_items:
        profile_id = item_and_score_map.get(item_id, -1)
        if profile_id >= 0:
            hot_item_ids = profile_and_hot_item_map.get(profile_id, [])
            hot_item_ids.append(item_id)
            profile_and_hot_item_map[profile_id] = hot_item_ids
    with open(profile_and_hot_item_file, "w") as fo:
        for profile_id, hot_item_ids in profile_and_hot_item_map.items():
            fo.write(f"{json.dumps({'profile_id': profile_id, 'hot_item_ids': hot_item_ids})}\n")
