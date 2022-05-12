import logging
import os.path

from common.hdfs_helper import dg_hdfs_client


def main():
    from common.logging_config import setup_console_log
    import sys
    setup_console_log()
    _type = sys.argv[1]
    local_file = sys.argv[2]
    hdfs_path = sys.argv[3]
    if _type == "local_to_hdfs":
        dg_hdfs_client.mkdir_dirs(os.path.dirname(hdfs_path))
        dg_hdfs_client.copy_from_local(local_file, hdfs_path)
    elif _type == "hdfs_to_local":
        dg_hdfs_client.copy_to_local(hdfs_path, local_file)
    else:
        logging.error(" _type is error")
        raise Exception()


if __name__ == '__main__':
    main()
