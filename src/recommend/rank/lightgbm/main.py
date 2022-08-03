import sys

from train import train


def main():
    dataset_file_path = sys.argv[1]
    output_file = sys.argv[2]
    info_log_file = sys.argv[3]
    error_log_file = sys.argv[4]
    from digitforce.aip.common.logging_config import setup_logging
    setup_logging(info_log_file, error_log_file)
    train(dataset_file_path, output_file)


if __name__ == '__main__':
    main()
