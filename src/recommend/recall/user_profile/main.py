from user_profile_recall import offline_recall


def main():
    from digitforce.aip.common.logging_config import setup_console_log
    setup_console_log()
    input_file, output_file, profile_and_hot_item_file = sys.argv[1], sys.argv[2], sys.argv[3]
    offline_recall(input_file, output_file, profile_and_hot_item_file)


if __name__ == '__main__':
    main()
