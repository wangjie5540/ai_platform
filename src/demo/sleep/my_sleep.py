import time


def do_sleep(minutes):
    print('start to sleep...')
    time.sleep(minutes * 60)
    print('wake up!')
