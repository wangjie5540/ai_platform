from data_generator import generate_train_data


def main():
    from common.logging_config import setup_console_log
    setup_console_log()
    import sys
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    user_and_id_map_file = sys.argv[3]
    item_and_id_map_file = sys.argv[4]

    generate_train_data(input_file, output_file, user_and_id_map_file, item_and_id_map_file)


if __name__ == '__main__':
    main()
