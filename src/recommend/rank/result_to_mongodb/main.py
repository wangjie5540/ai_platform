import logging
import json
import argparse
import pandas as pd

from digitforce.aip.common.mongodb_helper import mongodb_client_cli


def insert_to_mango_db(db_name, collection, file_name):
    logging.info('---------------------insert to mongo-----------------')
    client = mongodb_client_cli
    client.set_db(db_name)
    client.set_collection(collection)
    with open(file_name, 'r') as f:
        data = json.load(f)
        if isinstance(data, list):
            logging.info('data is instance of list')
            length = len(data)
            logging.info(f"data length: {length}")
            n = length // 1000
            logging.info(f"batch num: {n+1}")
            for i in range(n+1):
                data_i = data[i * 1000: (i+1) * 1000]
            client.insert_many(data_i)
        elif isinstance(data, dict):
            logging.info('data is instance of dict')
            client.insert_one(data)
        else:
            logging.error('file content not support')
            raise TypeError('file content not support')
    client.close()


def replace_to_mongo_db(db_name, collection, file_name, cond_key):
    logging.info('---------------------replace to mongo-----------------')
    client = mongodb_client_cli
    client.set_db(db_name)
    client.set_collection(collection)
    with open(file_name, 'r') as f:
        data = json.load(f)
        if isinstance(data, list):
            logging.info('data is instance of list')
            for record in data:
                value = record[cond_key]
                client.replace_one({cond_key: value}, record, upsert=True)
        elif isinstance(data, dict):
            logging.info('data is instance of dict')
            client.replace_one({cond_key: {'$exists': True}}, data, upsert=True)
        else:
            logging.error('file content not support')
            raise TypeError('file content not support')
    client.close()


def main():
    from digitforce.aip.common.logging_config import setup_logging

    parse = argparse.ArgumentParser()
    parse.add_argument('--mode', type=int, required=True)
    parse.add_argument('--db_name', type=str, required=True)
    parse.add_argument('--collection', type=str, required=True)
    parse.add_argument('--file_name', type=str, required=True)
    parse.add_argument('--cond_key', type=str)
    parse.add_argument('--info_log_file', type=str, required=True)
    parse.add_argument('--error_log_file', type=str, required=True)

    args = parse.parse_args()
    mode = args.mode
    db_name = args.db_name
    collection = args.collection
    file_name = args.file_name
    cond_key = args.cond_key
    info_log_file = args.info_log_file
    error_log_file = args.error_log_file

    setup_logging(info_log_file, error_log_file)
    logging.info(args)

    if mode == 1:
        insert_to_mango_db(db_name, collection, file_name)
    if mode == 2:
        replace_to_mongo_db(db_name, collection, file_name, cond_key)


if __name__ == '__main__':
    main()
