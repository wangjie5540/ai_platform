import argparse
import logging
from generate_statistics_table import generate_item_statistics_table
from digitforce.aip.common.logging_config import setup_console_log


def main():
    setup_console_log()
    parser = argparse.ArgumentParser()
    parser.add_argument('statistics_table', type=str)
    parser.add_argument('--event_table', type=str, default='push_traffic_behavior')
    parser.add_argument('--event_code_column_name', type=str, default='event_code')
    parser.add_argument('--duration', type=int)
    parser.add_argument('--partition_name', type=str, default='dt')
    parser.add_argument('--date_str', type=str)
    parser.add_argument('--item_column', type=str, default='sku')

    args = parser.parse_args()

    logging.info(args)
    statistics_table = args.statistics_table
    event_table = args.event_table
    event_code_column_name = args.event_code_column_name
    duration = args.duration
    partition_name = args.partition_name
    date_str = args.date_str
    item_columns = args.item_column

    generate_item_statistics_table(statistics_table, table_name=event_table, duration=duration,
                                   event_code_column_name=event_code_column_name,
                                   partition_name=partition_name, date_str=date_str, item=item_columns)


if __name__ == '__main__':
    main()
