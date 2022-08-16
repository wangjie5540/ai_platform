import json
import logging
import os
import sys

import faiss
import numpy as np
from gensim.models import word2vec

from digitforce.aip.common.file_helper import create_dir


def item2vec(input_file, ouput_file, skip_gram, vec_size, recall_result_file=None, recall_cnt=2000, has_header=True):
    logging.info(f"run item2vec ...input:{input_file} out:{ouput_file} recall_result_file:{recall_result_file}")
    sentences = []
    all_items = set()
    with open(input_file) as fi:
        cur_user_id = None
        sentence = []
        cnt = 0
        for line in fi:
            cnt += 1
            if has_header and cnt == 1:
                continue
            vals = line.strip().split(",")
            user_id = vals[0]
            item_id = vals[1]
            all_items.add(item_id)
            click_cnt = vals[3]
            save_cnt = vals[4]
            order_cnt = vals[5]

            if user_id == "user_id":
                continue
            if cur_user_id is None:
                cur_user_id = user_id
            if user_id == cur_user_id:
                if int(click_cnt) + int(save_cnt) + int(order_cnt) > 0:
                    sentence.append(item_id)
            else:
                sentences.append(sentence)
                sentence = [item_id]
                cur_user_id = user_id

    wv_model = word2vec.Word2Vec(sentences, hs=1, sg=skip_gram, min_count=5, window=10, vector_size=vec_size,
                                 workers=4,
                                 negative=8)
    create_dir(ouput_file)
    fo = open(ouput_file, "w")
    item_and_vec_map = {}
    for item_id in all_items:
        try:
            vec = wv_model.wv.get_vector(str(item_id)).tolist()
            item_and_vec_map[item_id] = vec
            fo.write(f"{json.dumps({'item_id': item_id, 'item_vec': vec})}\n")
        except Exception as e:
            print(e)
    fo.close()
    if not recall_result_file:
        return
    logging.info("begin recall for user... ")
    if recall_result_file:
        os.makedirs(os.path.dirname(recall_result_file), exist_ok=True)

    item_ids = []
    vecs = []
    for item_id, vec in item_and_vec_map.items():
        item_ids.append(item_id)
        vecs.append(vec)
    vecs = np.array(vecs, dtype=np.float32)

    index = faiss.IndexFlatL2(vecs.shape[1])
    index.add(vecs)
    with open(recall_result_file, "w") as fo:
        with open(input_file) as fi:
            cur_user_id = None
            sentence = []
            cnt = 0
            for line in fi:
                cnt += 1
                if has_header and cnt == 1:
                    continue
                vals = line.strip().split(",")
                user_id = vals[0]
                item_id = vals[1]
                if user_id == cur_user_id:
                    if int(click_cnt) + int(save_cnt) + int(order_cnt) > 0:
                        sentence.append(item_id)
                else:
                    if not sentence:
                        continue
                    user_vec = [item_and_vec_map.get(_) for _ in sentence]
                    user_vec = np.array(user_vec).mean(axis=0)
                    batch_user_vec = [user_vec]
                    batch_user_id = [user_id]
                    x = index.search(np.array(batch_user_vec, np.float32), recall_cnt)
                    dis, batch_item_ids = x
                    batch_item_ids = batch_item_ids.tolist()
                    for i, user_id in enumerate(batch_user_id):
                        recall_item_ids = batch_item_ids[i]
                        recall_item_ids = [item_ids[_] for _ in recall_item_ids]
                        fo.write(f"{json.dumps({'user_id': user_id, 'recall_item_ids': recall_item_ids})}\n")
                    sentence = []
    fo.close()


def main():
    input_file = sys.argv[1]
    ouput_file = sys.argv[2]
    skip_gram = sys.argv[3]
    vec_size = int(sys.argv[4])
    recall_result_file = None
    if len(sys.argv) > 5:
        recall_result_file = sys.argv[5]
    item2vec(input_file, ouput_file, skip_gram, vec_size, recall_result_file)


if __name__ == '__main__':
    from digitforce.aip.common.logging_config import setup_console_log

    setup_console_log()
    main()
