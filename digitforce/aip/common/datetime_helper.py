import datetime

DATE_FORMAT = "%Y%m%d"
PATH_DATE_FORMAT = "%Y/%m/%d"
PATH_DATE_HOUR_FORMAT = "%Y/%m/%d/%H"


def get_yesterday_str():
    return add_days(datetime.datetime.today(), -1).strftime(DATE_FORMAT)


def get_today_str():
    return datetime.datetime.today().strftime(DATE_FORMAT)


def n_days_ago(days=1):
    return add_days(datetime.datetime.today(), -days)


def n_days_ago_str(days=1):
    return n_days_ago(days).strftime(DATE_FORMAT)


def add_days(one_date, days=1):
    return one_date + datetime.timedelta(days=days)


def add_hours(one_date, hours=1):
    return one_date + datetime.timedelta(hours=hours)


def parse_date_str(date_str):
    return datetime.datetime.strptime(date_str, DATE_FORMAT)


def to_date_str(date_time):
    return date_time.strftime(DATE_FORMAT)


def get_path(date_time):
    return date_time.strftime(PATH_DATE_FORMAT)


def get_path_hour(date_time):
    return date_time.strftime(PATH_DATE_HOUR_FORMAT)


