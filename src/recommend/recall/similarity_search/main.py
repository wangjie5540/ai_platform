import json
import logging

import faiss
import numpy as np
import pandas as pd

from digitforce.aip.common.file_helper import create_dir


def find_neighbor(user_vec_file, item_vec_file, output_file, topk=100):
    logging.info(f"begin find neighor... "
                 f"user_vec_file:{user_vec_file} "
                 f"item_vec_file:{item_vec_file} "
                 f"output_file:{output_file} ")
    item_and_vec_map = {}
    with open(item_vec_file) as fi:
        for line in fi:
            if line:
                json_obj = json.loads(line)
                item_id = int(json_obj["item_id"])
                vec = json_obj["item_vec"]
                item_and_vec_map[item_id] = vec
    item_ids = []
    vecs = []
    for item_id, vec in item_and_vec_map.items():
        item_ids.append(item_id)
        vecs.append(vec)
    vecs = np.array(vecs, dtype=np.float32)

    index = faiss.IndexFlatL2(vecs.shape[1])
    index.add(vecs)
    with open(output_file, "w") as fo:
        with open(user_vec_file) as fi:
            batch_user_id = []
            batch_user_vec = []
            batch_size = 32
            for line in fi:
                if line:
                    json_obj = json.loads(line)
                    user_id = int(json_obj["user_id"])
                    user_vec = json_obj["user_vec"]
                    batch_user_id.append(user_id)
                    batch_user_vec.append(user_vec)
                    if len(batch_user_vec) == batch_size:
                        dis, batch_item_ids = index.search(np.array(batch_user_vec, np.float32), topk)
                        batch_item_ids = batch_item_ids.tolist()
                        for i, user_id in enumerate(batch_user_id):
                            recall_item_ids = batch_item_ids[i]
                            fo.write(f"{json.dumps({'user_id': user_id, 'recall_item_ids': recall_item_ids})}\n")


def main():
    from digitforce.aip.common.logging_config import setup_console_log
    setup_console_log()
    import sys

    user_vec_file = sys.argv[1]
    item_vec_file = sys.argv[2]
    output_file = sys.argv[3]
    topk = 100 if len(sys.argv) < 5 else int(sys.argv[4])
    user_and_id_map_file = None
    item_and_id_map_file = None
    if len(sys.argv) > 6:
        user_and_id_map_file = sys.argv[5]
        item_and_id_map_file = sys.argv[6]
    output_file_tmp = output_file + ".tmp"
    find_neighbor(user_vec_file, item_vec_file, output_file_tmp, topk)
    create_dir(output_file)
    fo = open(output_file, "w")
    if user_and_id_map_file:
        df = pd.read_csv(user_and_id_map_file, header=None, names=["user_id", "id"])
        user_and_id_map = dict(zip(df.id.astype(int), df.user_id))
        df = pd.read_csv(item_and_id_map_file, header=None, names=["item_id", "id"])
        item_and_id_map = dict(zip(df.id.astype(int), df.item_id))

    with open(output_file_tmp) as fi:
        for line in fi:
            json_obj = json.loads(line)
            json_obj["user_id"] = user_and_id_map.get(json_obj["user_id"])
            json_obj["recall_item_ids"] = [item_and_id_map.get(_) for _ in json_obj["recall_item_ids"]]
            fo.write(f"{json.dumps(json_obj)}\n")
    fo.close()


if __name__ == '__main__':
    main()
