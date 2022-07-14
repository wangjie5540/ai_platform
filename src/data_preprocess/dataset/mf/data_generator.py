import logging
import random

import pandas as pd


def deal_user_id_and_item_id(df, min_user_action_cnt=5):
    a = df["user_id"].value_counts()
    user_and_id_map = {"?": 0}

    for user_id, cnt in zip(a.index, a.values):
        if cnt >= min_user_action_cnt:
            user_and_id_map[user_id] = len(user_and_id_map)

    item_and_id_map = {0: 0}
    item_ids = df["item_id"].value_counts().index
    for _ in item_ids:
        item_and_id_map[_] = len(item_and_id_map)

    df["user_id"] = df["user_id"].apply(lambda x: user_and_id_map.get(x, 0))
    df["item_id"] = df["item_id"].apply(lambda x: item_and_id_map.get(x, 0))
    return df, user_and_id_map, item_and_id_map


def generate_train_data(input_file, output_file, user_and_id_map_file, item_and_id_map_file, names=None):
    logging.info(f"input:{input_file} out:{output_file} names:{names}")
    if names:
        df = pd.read_csv(input_file, header=None, names=names)
    else:
        df = pd.read_csv(input_file)
    df, user_and_id_map, item_and_id_map = deal_user_id_and_item_id(df)
    df.click_cnt = df.click_cnt.astype(int)
    df.save_cnt = df.save_cnt.astype(int)
    df.order_cnt = df.order_cnt.astype(int)
    df["score"] = df.apply(lambda x: sum([x["click_cnt"], x["save_cnt"], x["order_cnt"]]), axis=1)
    fo = open(output_file, "w")
    all_item_ids = list(set(df["item_id"]))
    cnt = 0
    for user_id, group_df in df.groupby("user_id"):
        act_item_ids = set(group_df["item_id"])
        pos_cnt = group_df["score"].sum()
        neg_item_ids = random.choices(all_item_ids, k=2 * pos_cnt)
        neg_item_ids = [_ for _ in neg_item_ids if _ not in act_item_ids][:pos_cnt]
        for _ in neg_item_ids:
            fo.write(f"{user_id},{_},{0}\n")
        for _, row in group_df.iterrows():
            for i in range(int(row["score"])):
                fo.write(f"{user_id},{row['item_id']},{1}\n")
        cnt += 1
        if cnt % 100 == 0:
            logging.info(f"deal user cnt:{cnt}")
    fo.close()
    logging.info(f"user_and_id_map_file:{user_and_id_map_file}")
    with open(user_and_id_map_file, "w") as fo:
        for user_id, _id in user_and_id_map.items():
            fo.write(f"{user_id},{_id}\n")
    logging.info(f"item_and_id_map_file:{item_and_id_map_file}")
    with open(item_and_id_map_file, "w") as fo:
        for item_id, _id in item_and_id_map.items():
            fo.write(f"{item_id},{_id}\n")
