import os
import sys
import uuid
from user_profile_recall import offline_recall, agg_hot_product_by_profile


def main():
    from digitforce.aip.common.logging_config import setup_console_log
    setup_console_log()
    input_file, output_file, hot_csv_file, item_and_profile_map_file = \
        sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
    profile_and_hot_item_file = f"/data/tmp/tmp-{uuid.uuid4()}.jsonl"
    agg_hot_product_by_profile(hot_csv_file, item_and_profile_map_file, profile_and_hot_item_file)
    offline_recall(input_file, output_file, profile_and_hot_item_file)
    os.system("rm " + profile_and_hot_item_file)


if __name__ == '__main__':
    main()
