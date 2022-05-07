import json
import random
import sys


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
            json_obj = json.loads(line)
            profile_id = json_obj["profile_id"]
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


def main():
    from common.logging_config import setup_console_log
    setup_console_log()
    input_file, output_file, profile_and_hot_item_file = sys.argv[1], sys.argv[2], sys.argv[3]
    offline_recall(input_file, output_file, profile_and_hot_item_file)


if __name__ == '__main__':
    main()

