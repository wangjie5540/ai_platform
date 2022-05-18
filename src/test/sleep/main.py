import time


def main():
    import sys
    sleep_time = int(sys.argv[1])
    for _ in range(sleep_time):
        time.sleep(1)
        print(f"sleep {_}s sum time:{sleep_time}")
    print("goodbye")


if __name__ == '__main__':
    main()
