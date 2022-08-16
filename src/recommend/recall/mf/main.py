import json
import logging

from deep_mf import train
from digitforce.aip.common.file_helper import create_dir


def main():
    from digitforce.aip.common.logging_config import setup_console_log
    setup_console_log()
    import sys

    input_file = sys.argv[1]
    item_embeding_file = sys.argv[2]
    user_embeding_file = sys.argv[3]

    mf_model = train(input_file, 3)

    create_dir(item_embeding_file)
    create_dir(user_embeding_file)
    with open(item_embeding_file, "w") as fo:
        item_emb = mf_model.item_emb.weight.detach().to('cpu').numpy().tolist()
        for i, item_vec in enumerate(item_emb):
            line = json.dumps({'item_id': i, 'item_vec': item_vec})
            fo.write(f"{line}\n")
    with open(user_embeding_file, "w") as fo:
        user_emb = mf_model.user_emb.weight.detach().to('cpu').numpy().tolist()
        for i, user_vec in enumerate(user_emb):
            line = json.dumps({'user_id': i, 'user_vec': user_vec})
            fo.write(f"{line}\n")
    logging.info(f"finish....")


if __name__ == '__main__':
    main()
