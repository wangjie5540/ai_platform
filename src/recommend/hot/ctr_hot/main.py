import sys

from ctr_hot import calculate_hot_item


def main():
    from digitforce.aip.common.logging_config import setup_console_log
    setup_console_log()
    input_file, output_file = sys.argv[1], sys.argv[2]
    calculate_hot_item(input_file, output_file)


if __name__ == '__main__':
    main()