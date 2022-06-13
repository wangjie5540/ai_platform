import json
import sys

from gensim.models import word2vec


def item2vec(input_file, ouput_file, skip_gram, vec_size):
    sentences = []
    all_items = set()
    with open(input_file) as fi:
        for line in fi:
            json_obj = json.loads(line)
            items = json_obj["items"]
            items = [str(_) for _ in items]
            sentences.append(items)
            all_items.update(items)
    wv_model = word2vec.Word2Vec(sentences, hs=1, sg=skip_gram, min_count=5, window=10, vector_size=vec_size,
                                 workers=4,
                                 negative=8)
    fo = open(ouput_file, "w")
    for item_id in all_items:
        try:
            vec = wv_model.wv.get_vector(str(item_id)).tolist()
            fo.write(f"{json.dumps({'item_id': item_id, 'item_vec': vec})}\n")
        except Exception as e:
            print(e)
    fo.close()


def main():
    input_file = sys.argv[1]
    ouput_file = sys.argv[2]
    skip_gram = sys.argv[3]
    vec_size = int(sys.argv[4])
    item2vec(input_file, ouput_file, skip_gram, vec_size)


if __name__ == '__main__':
    from digitforce.aip.common.logging_config import setup_console_log

    setup_console_log()
    main()
