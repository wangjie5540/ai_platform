import sys
import logging
from get_data import get_data


def main():
    sql = sys.argv[1]
    output_file = sys.argv[2]
    info_log_file = sys.argv[3]
    error_log_file = sys.argv[4]
    from digitforce.aip.common.logging_config import setup_logging
    setup_logging(info_log_file, error_log_file)

    logging.info(sql)
    get_data(sql, output_file)


if __name__ == '__main__':
    main()
