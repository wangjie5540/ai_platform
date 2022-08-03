import datetime
from dateutil.relativedelta import relativedelta

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

def date_add_str(date_str,step_len,time_type='day'):
    """
    新的日期=当前日期+step_len*(day/周/月)
    :param date_str:当前日期
    :param step_len:多长时间
    :param time_type:day/week/month
    :return:新的日期
    """
    if time_type=='day':#日
        date_str=datetime.datetime.strptime(date_str,"%Y%m%d")
        date_str_add=date_str+datetime.timedelta(days=+step_len)
        date_str_add=str(date_str_add).replace('-','')[0:8]
    elif time_type=='week':#周
        date_str=datetime.datetime.strptime(date_str, "%Y%m%d")
        date_str_add=date_str + datetime.timedelta(weeks=+step_len)
        date_str_add=str(date_str_add).replace('-','')[0:8]
    else:#月
        date_str=datetime.datetime.strptime(date_str,'%Y%m%d')
        date_str_add=date_str+relativedelta(months=+step_len)
        date_str_add=str(date_str_add).replace('-','')[0:8]
    return date_str_add

def month_add_str(month_str,step_len):
    """
    月份加上len后的月份时间
    :param month_str: 月份，如：202205
    :param step_len: 长度
    :return: 加上len后的长度
    """
    if len(month_str)>6:
        month_str=month_str[0:6]
    elif len(month_str)<6:
        return None
    month_str=month_str+'01'
    month_str=date_add_str(month_str,step_len,time_type='month')
    month_str=month_str[0:6]
    return month_str

