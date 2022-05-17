# coding: utf-8
from kafka import KafkaProducer
import argparse
import json


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--bootstrap_servers', type=str, required=True, help='kafka连接地址，使用","分隔')
    parser.add_argument('-t', '--topic', type=str, required=True, help='目标kafka的主题')
    parser.add_argument('-f', '--file_path', type=str, required=True, help='输入文件路径')
    return parser.parse_args()


def send_data(bootstrap_servers: list, topic: str, iterator,
              value_serializer=lambda m: json.dumps(m, ensure_ascii=False).encode('utf-8')):
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=value_serializer)
    for item in iterator:
        producer.send(topic, json.loads(item))


if __name__ == '__main__':
    args = parse_arguments()
    with open(file=args.file_path, mode='r') as f:
        send_data(
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
            iterator=f.readlines()
        )
