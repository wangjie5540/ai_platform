import datetime

DATE_FORMAT = "%Y-%m-%d"
PATH_DATE_FORMAT = "%Y/%m/%d"
PATH_DATE_HOUR_FORMAT = "%Y/%m/%d/%H"


def get_today_str(data_format=DATE_FORMAT):
    return datetime.datetime.today().strftime(data_format)


def get_day_str(date: datetime, date_format=DATE_FORMAT):
    return date.strftime(date_format)


def n_days_ago(days):
    return datetime.datetime.today() + datetime.timedelta(days=-days)


def n_days_ago_str(days, data_format=DATE_FORMAT):
    return get_day_str(n_days_ago(days), data_format)


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
