from data_generator import generate_train_data


def main():
    from digitforce.aip.common.logging_config import setup_console_log
    setup_console_log()
    import sys
    input_table = sys.argv[1]
    output_file = sys.argv[2]
    profile_name = sys.argv[3]
    generate_train_data(input_table, output_file, profile_name)


if __name__ == '__main__':
    main()
