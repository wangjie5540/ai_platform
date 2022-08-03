import logging
import sys

from digitforce.aip.common.hive_helper import df_hive_helper


def main():
    from digitforce.aip.common.logging_config import setup_console_log
    setup_console_log()
    sql = sys.argv[1]
    table_name = sys.argv[2]
    logging.info(sql)
    delete_tb = False
    if len(sys.argv) > 3:
        delete_tb = sys.argv[3].lower() == "true"
    df_hive_helper.query_to_table(sql, table_name, delete_tb=delete_tb)


if __name__ == '__main__':
    main()
