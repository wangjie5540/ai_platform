import logging

from deep_mf import train


def main():
    from digitforce.aip.common.logging_config import setup_console_log
    setup_console_log()
    input_file = "C:/Users/zhangxueren/Desktop/mf_train_dataset.csv"
    item_embeding_file = "C:/Users/zhangxueren/Desktop/recommend/emb/recall/item_embding.csv"
    user_embeding_file = "C:/Users/zhangxueren/Desktop/recommend/emb/recall/user_embeding.csv"
    import sys
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    if len(sys.argv) > 3:
        item_embeding_file = sys.argv[2]
        user_embeding_file = sys.argv[3]
    mf_model = train(input_file, 3)
    with open(item_embeding_file, "w") as fo:
        item_emb = mf_model.item_emb.weight.detach().to('cpu').numpy().tolist()
        for i, item_vec in enumerate(item_emb):
            fo.write(f"{i},{item_vec}\n")
    with open(user_embeding_file, "w") as fo:
        user_emb = mf_model.user_emb.weight.detach().to('cpu').numpy().tolist()
        for i, user_vec in enumerate(user_emb):
            fo.write(f"{i},{user_vec}\n")
    logging.info(f"finish....")


if __name__ == '__main__':
    main()
