import json

import faiss
import numpy as np
import pandas as pd


def find_neighbor(user_vec_file, item_vec_file, output_file, topk=100, user_and_id_map=None):
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
                            if user_and_id_map:
                                user_id = user_and_id_map.get(user_id, user_id)
                            fo.write(f"{json.dumps({'user_id': user_id, 'recall_item_ids': recall_item_ids})}\n")


def main():
    from digitforce.aip.common.logging_config import setup_console_log
    setup_console_log()
    import sys

    user_vec_file = sys.argv[1]
    item_vec_file = sys.argv[2]
    output_file = sys.argv[3]
    topk = 100 if len(sys.argv) < 5 else int(sys.argv[4])
    user_and_id_map = {}
    if len(sys.argv) > 5:
        user_and_id_map_file = sys.argv[5]
        # with open(user_and_id_map_file) as fi:
        #     for _ in fi:
        #         json_obj = json.loads(_)
        #         user_id = json_obj["user_id"]
        #         id = json_obj["id"]
        #         user_and_id_map[id] = user_id
        df = pd.read_csv(user_and_id_map_file, header=None, names=["user_id", "id"])
        user_and_id_map = dict(zip(df.user_id, df.id.astype(int)))
    find_neighbor(user_vec_file, item_vec_file, output_file, topk, user_and_id_map)


if __name__ == '__main__':
    main()
