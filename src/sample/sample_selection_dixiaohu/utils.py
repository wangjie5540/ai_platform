# encoding: utf-8
import datetime
from chinese_calendar import is_holiday
from pyspark.sql import Row
import re


def get_zc_jf(feature_list: list, start_date: str, end_date: str,
              dixiao_before_days: int, dixiao_after_days: int, noexchangedate_list: list):
    """

    Args:
        feature_list (list): 输入某个客户的每日资产
        ，顺序排列 [("20230101",123456),("20230102",1234567),("20230103",12345)]
        start_date (str): 需要计算样本起始时间,样式形如"20230203"
        end_date (str): 需要计算样本结束时间,样式形如"20230203"
        dixiao_before_days (int): 过去n个交易日,要求大于0
        dixiao_after_days (int): 在未来m个交易日,要求大于0
        noexchangedate_list (list[str]): 非交易日列表，['2023-02-04','2023-02-05']

    Returns:
        _type_: 某个客户在start_date到end_date之间，每一天，相较过去n个交易日的日均资产
        ，未来m个交易日的日均资产，例如：[('20220103',12113，123)]
    """

    # date类型
    # 比如当前是20200315，该值的取值范围是(start_date, end_date)
    # 过去5天：  (left_date1, right_date1), (20200311 - 20200315)
    # 未来10天： (left_date2, right_date2), (20200316 - 20200325)
    right1, left1, right2, left2 = None, None, None, None
    start_index, end_index = None, None

    # 确定起始日期
    for index, value in enumerate(feature_list):
        if start_date == value[0]:
            start_index = index
            break
    # 确定终止日期
    for index, value in enumerate(feature_list):
        if end_date == value[0]:
            end_index = index
            break

    res = []
    for ix in range(start_index, end_index + 1):  # TODO： 超出范围的情况

        right1 = ix  # right1
        #
        left1 = right1
        i = 0
        left_avg_asset = 0
        while i < dixiao_before_days:
            left1_date = (
                datetime.datetime
                .strptime(feature_list[left1][0], '%Y%m%d')
                .strftime('%Y-%m-%d')
            )  # 转化日期字符串格式
            if left1_date not in noexchangedate_list:
                left_avg_asset += feature_list[left1][1]
                i += 1  # 慢指针，同时记录天数

            if left1 > 0:
                left1 -= 1  # 往左的快指针,最小指到0
            else:
                break
        left_avg_asset = left_avg_asset / i  # 除以实际天数

        #
        left2 = right1 + 1
        right2 = left2
        j = 0
        right_max_asset = 0
        while j < dixiao_after_days:
            right2_date = (
                datetime.datetime
                .strptime(feature_list[right2][0], '%Y%m%d')
                .strftime('%Y-%m-%d')
            )  # 转化日期字符串格式
            if right2_date not in noexchangedate_list:
                right_max_asset = max(feature_list[right2]
                                      [1], right_max_asset)
                j += 1  # 慢指针，同时记录天数

            if right2 < len(feature_list) - 1:
                right2 += 1  # 往右的快指针,最小指到末尾
            else:
                break
        right_max_asset = max(right_max_asset, 0)

        right1_date = feature_list[right1][0]
        right1_zc = feature_list[right1][1]  # 当前日期和资产

        res.append((right1_date, right1_zc, left_avg_asset, right_max_asset))

    return res


def get_login_days(feature_list: list, start_date: str, end_date: str,
                   dixiao_before_days: int, noexchangedate_list: list):
    """_summary_

    Args:
        feature_list (list): 输入某个客户的每日是否登录，顺序排列 [("20230101",1),("20230102",1),("20230103",1)]
        start_date (str): 需要计算样本起始时间,样式形如"20230203"
        end_date (str): 需要计算样本结束时间,样式形如"20230203"
        dixiao_before_days (int): 过去n个交易日,要求大于0
        noexchangedate_list (list): 非交易日列表，['2023-02-04','2023-02-05']

    Returns:
        _type_: 某个客户在start_date到end_date之间，每一天，相较过去n个交易日的登陆天数，例如：[('20220103',12)]
    """
    # date类型
    # 比如当前是20200315，该值的取值范围是(start_date, end_date)
    # 过去5天：  (left_date, right_date), (20200311 - 20200315)
    right, left, = None, None,
    start_index, end_index = None, None

    # 确定起始日期
    for index, value in enumerate(feature_list):
        if start_date == value[0]:
            start_index = index
            break
    # 确定终止日期
    for index, value in enumerate(feature_list):
        if end_date == value[0]:
            end_index = index
            break

    res = []
    for ix in range(start_index, end_index + 1):

        right = ix  # right1
        #
        left = right
        i = 0
        left_login_days = 0
        while i < dixiao_before_days:
            left_date = (
                datetime.datetime
                .strptime(feature_list[left][0], '%Y%m%d')
                .strftime('%Y-%m-%d')
            )  # 转化日期字符串格式
            if left_date not in noexchangedate_list:
                left_login_days += feature_list[left][1]
                i += 1  # 慢指针，同时记录天数

            if left > 0:
                left -= 1  # 往左的快指针,最小指到0
            else:
                break

        right_date = feature_list[right][0]
        res.append((right_date, left_login_days))
    return res


def get_exchange_days(feature_list: list, start_date: str, end_date: str,
                      dixiao_before_days: int, noexchangedate_list: list):
    """_summary_

    Args:
        feature_list (list): 输入某个客户的每日交易次数，顺序排列 [("20230101",2),("20230102",3),("20230103",4)]
        start_date (str): 需要计算样本起始时间
        end_date (str): 需要计算样本结束时间
        dixiao_before_days (int): 过去n个交易日,要求大于0
        noexchangedate_list (list): 非交易日列表，['2023-02-04','2023-02-05']

    Returns:
        _type_: 某个客户在start_date到end_date之间，每一天，相较过去n个交易日的交易天数，例如：[('20220103',12)]
    """
    # date类型
    # 比如当前是20200315，该值的取值范围是(start_date, end_date)
    # 过去5天：  (left_date, right_date), (20200311 - 20200315)
    right, left, = None, None,
    start_index, end_index = None, None

    # 确定起始日期
    for index, value in enumerate(feature_list):
        if start_date == value[0]:
            start_index = index
            break
    # 确定终止日期
    for index, value in enumerate(feature_list):
        if end_date == value[0]:
            end_index = index
            break

    res = []
    for ix in range(start_index, end_index + 1):

        right = ix  # right1
        #
        left = right
        i = 0
        left_exchange_days = 0
        while i < dixiao_before_days:
            left_date = (
                datetime.datetime
                .strptime(feature_list[left][0], '%Y%m%d')
                .strftime('%Y-%m-%d')
            )  # 转化日期字符串格式
            if left_date not in noexchangedate_list:
                # 当天的交易次数和1做min，大于1的为1，没有交易为0
                left_exchange_days += min(feature_list[left][1], 1)
                i += 1  # 慢指针，同时记录天数

            if left > 0:
                left -= 1  # 往左的快指针,最小指到0
            else:
                break

        right_date = feature_list[right][0]
        res.append((right_date, left_exchange_days))
    return res


def get_latest_dt(spark, table_name, method='max'):
    """
    获取一个表的最新分区日期
    Args:
        spark:
        table_name:
        method:

    Returns:

    """
    all_dt = (
        spark.sql('show partitions ' + table_name)
        .rdd.flatMap(lambda x: x)
        .map(lambda x: x.replace("dt=", ""))
        # 防止日期数据中存在异常，此处仅通过长度识别，可优化
        .filter(lambda x: len(x) == 8)
    )
    if method == 'max':
        lastest_dt = all_dt.max()
    elif method == 'min':
        lastest_dt = all_dt.min()
    else:
        lastest_dt = None

    return lastest_dt


def future_exchange_date(day: str, delta: int):
    """返回delta个交易日前/后的日期

    Args:
        day (str): "2023-01-02"类型
        delta (int): _description_

    Returns:
        _type_: _description_
    """
    # 定义变量
    count = 0
    future_date = datetime.datetime.strptime(day, '%Y-%m-%d')

    # 计算未来/过去delta个交易日
    while count < abs(delta):
        days = None
        if delta == 0:
            break
        elif delta < 0:
            days = -1
        elif delta > 0:
            days = 1

        # 往前/后推一天
        future_date += datetime.timedelta(days=days)

        # 判断是否是周六或周日
        if future_date.weekday() >= 5:
            continue

        # 判断是否是节假日,不包括调休
        if is_holiday(future_date):
            continue

        count += 1

    # 输出未来20个交易日的日期
    return future_date.strftime('%Y-%m-%d')


def noexchange_days(start_date: str, end_date: str):
    """_summary_

    Args:
        start_date (str): "2023-02-23"
        end_date (str): _description_

    Returns:
        _type_: 返回包含所有节假日的列表
    """
    future_day = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    end_day = datetime.datetime.strptime(end_date, '%Y-%m-%d')

    res = []
    for i in range((end_day - future_day).days + 1):
        future_day += datetime.timedelta(days=1)
        # 判断future_day是否不是周六或周日且不是节假日
        if future_day.weekday() >= 5 or is_holiday(future_day):
            res.append(future_day.strftime('%Y-%m-%d'))
    return res


def generate_rows_from_df(df, cast_int=None):
    columns = df.columns.tolist()
    row_list = list()
    for row in df.iterrows():
        k_v = dict()
        for key in columns:
            if cast_int is not None and key in cast_int:
                k_v[key] = int(row[1][key])
            else:
                k_v[key] = row[1][key]
        row_list.append(Row(**k_v))
    return row_list


def list2str(list_: list):
    """
    输入['a','b','c']，输出”'a','b','c'“
    """
    return re.sub("[\[\]]", "", str(list_))
