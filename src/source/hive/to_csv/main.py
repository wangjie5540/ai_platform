import logging
import sys

from digitforce.aip.common.file_helper import create_dir
from digitforce.aip.common.hive_helper import df_hive_helper


def main():
    from digitforce.aip.common.logging_config import setup_console_log
    setup_console_log()
    sql = sys.argv[1]
    file_path = sys.argv[2]
    logging.info(sql)
    df = df_hive_helper.query_to_df(sql)
    create_dir(file_path)
    df.to_csv(file_path, index=False)


if __name__ == '__main__':
    main()
