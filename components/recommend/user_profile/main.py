import sys

from user_profile_caculator import calculate_user_profile


def main():
    from common.logging_config import setup_console_log
    setup_console_log()
    input_file, output_file = sys.argv[1], sys.argv[2]
    calculate_user_profile(input_file, output_file)


if __name__ == '__main__':
    main()
