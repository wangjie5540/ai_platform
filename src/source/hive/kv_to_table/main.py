import logging

from kv_to_table import hive_to_kv, kv_to_hive


def main():
    import sys
    _type = sys.argv[1]
    kv_map_file = sys.argv[2]
    table_name = sys.argv[3]
    dataset = sys.argv[4]
    if _type == "kv_to_table":
        kv_to_hive(kv_map_file, table_name, dataset)
    elif _type == "hive_to_kv":
        hive_to_kv(table_name, dataset, kv_map_file)
    else:
        logging.error(" _type is error")
        raise Exception()


if __name__ == '__main__':
    main()
