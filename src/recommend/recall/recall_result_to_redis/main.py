import json
import logging
import time

from digitforce.aip.common.logging_config import setup_console_log
from digitforce.aip.common.redis_client import df_redis_cli


def upload_recall_result(redis_key_header, recall_result_file):
    with open(recall_result_file) as fi:
        batch = []
        cnt = 0
        t0 = time.time()
        for line in fi:
            json_obj = json.loads(line)
            user_id = json_obj.get("user_id", None)
            item_ids = json_obj.get("recall_item_ids", [])
            item_ids = [str(_) for _ in item_ids]
            if user_id and item_ids:
                batch.append((redis_key_header + user_id, item_ids))
                cnt += 1
            if len(batch) % 100 == 0:
                kv_map = dict(batch)
                df_redis_cli.batch_update_redis_list(kv_map)
                batch = []
                logging.info(f"update user recall result cnt:{cnt} time spend:{time.time() - t0}")


def main():
    import sys

    recall_result_file = sys.argv[1]
    redis_key_header = sys.argv[2]
    logging.info(f"begin upload recall result to redis.."
                 f"recall result file:{recall_result_file}")
    upload_recall_result(redis_key_header, recall_result_file)


if __name__ == '__main__':
    main()
