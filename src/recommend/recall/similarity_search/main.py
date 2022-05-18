import faiss
import numpy as np

def find_neighbor(user_vec_file, item_vec_file, output_file, topk=100):
    item_and_vec_map = {}
    with open(item_vec_file) as fi:
        for line in fi:
            vals = line.split(",")
            if vals:
                item_id = vals[0]
                vec = eval(vals[1])
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
                vals = line.split(",")
                if vals:
                    user_id = vals[0]
                    user_vec = eval(vals[1])
                    batch_user_id.append(user_id)
                    batch_user_vec.append(user_vec)
                    if len(batch_user_vec) == batch_size:
                        dis, batch_item_ids = index.search(np.array(batch_user_vec, np.float32), topk)
                        batch_item_ids = batch_item_ids.tolist()
                        for i, user_id in enumerate(batch_user_id):
                            topk_item_ids = batch_item_ids[i]
                            fo.write(f"{user_id},{topk_item_ids}\n")


def main():
    from digitforce.aip.common.logging_config import setup_console_log
    setup_console_log()
    import sys

    user_vec_file = sys.argv[1]
    item_vec_file = sys.argv[2]
    output_file = sys.argv[3]
    topk = 100 if len(sys.argv) < 5 else int(sys.argv[4])
    find_neighbor(user_vec_file, item_vec_file, output_file, topk)


if __name__ == '__main__':
    main()
