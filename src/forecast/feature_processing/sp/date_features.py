# -*- coding: utf-8 -*-
# @Time : 2021/12/25
# @Author : Arvin

from forecast.common.reference_package import *
from digitforce.aip.common.spark_helper import *

def get_holiday(dt):
    if type(dt) == str:
        dt = pd.to_datetime(dt)

    holidays = {
        '2023-01-01': "New Year's Day",
        '2023-01-02': "New Year's Day",
        '2023-01-21': "Spring Festival",
        '2023-01-22': "Spring Festival",
        '2023-01-23': "Spring Festival",
        '2023-01-24': "Spring Festival",
        '2023-01-25': "Spring Festival",
        '2023-01-26': "Spring Festival",
        '2023-01-27': "Spring Festival",
        '2023-04-05': "Tomb-sweeping Day",
        '2023-04-29': "Labour Day",
        '2023-04-30': "Labour Day",
        '2023-05-01': "Labour Day",
        '2023-05-02': "Labour Day",
        '2023-06-22': "Dragon Boat Festival",
        '2023-06-23': "Dragon Boat Festival",
        '2023-06-24': "Dragon Boat Festival",
        '2023-09-29': "Mid-autumn Festival",
        '2023-09-30': "Mid-autumn Festival",
        '2023-10-01': "National Day",
        '2023-10-02': "National Day",
        '2023-10-03': "National Day",
        '2023-10-04': "National Day",
        '2023-10-05': "National Day",
        '2023-10-06': "National Day",
        '2023-10-07': "National Day",
        '2024-01-01': "New Year's Day",
        '2024-02-10': "Spring Festival",
        '2024-02-11': "Spring Festival",
        '2024-02-12': "Spring Festival",
        '2024-02-13': "Spring Festival",
        '2024-02-14': "Spring Festival",
        '2024-02-15': "Spring Festival",
        '2024-02-16': "Spring Festival",
        '2024-02-17': "Spring Festival",
        '2024-04-04': "Tomb-sweeping Day",
        '2024-04-05': "Tomb-sweeping Day",
        '2024-04-06': "Tomb-sweeping Day",
        '2024-05-01': "Labour Day",
        '2024-05-02': "Labour Day",
        '2024-05-03': "Labour Day",
        '2024-05-04': "Labour Day",
        '2024-05-05': "Labour Day",
        '2024-06-08': "Dragon Boat Festival",
        '2024-06-09': "Dragon Boat Festival",
        '2024-06-10': "Dragon Boat Festival",
        '2024-09-15': "Mid-autumn Festival",
        '2024-09-16': "Mid-autumn Festival",
        '2024-09-17': "Mid-autumn Festival",
        '2024-10-01': "National Day",
        '2024-10-02': "National Day",
        '2024-10-03': "National Day",
        '2024-10-04': "National Day",
        '2024-10-05': "National Day",
        '2024-10-06': "National Day",
        '2024-10-07': "National Day",
        '2025-01-01': "New Year's Day",
        '2025-01-02': "New Year's Day",
        '2025-01-03': "New Year's Day",
        '2025-01-29': "Spring Festival",
        '2025-01-30': "Spring Festival",
        '2025-01-31': "Spring Festival",
        '2025-02-01': "Spring Festival",
        '2025-02-02': "Spring Festival",
        '2025-02-03': "Spring Festival",
        '2025-02-04': "Spring Festival",
        '2025-02-05': "Spring Festival",
        '2025-04-04': "Tomb-sweeping Day",
        '2025-04-05': "Tomb-sweeping Day",
        '2025-04-06': "Tomb-sweeping Day",
        '2025-05-01': "Labour Day",
        '2025-05-02': "Labour Day",
        '2025-05-03': "Labour Day",
        '2025-05-04': "Labour Day",
        '2025-05-05': "Labour Day",
        '2025-05-30': "Dragon Boat Festival",
        '2025-05-31': "Dragon Boat Festival",
        '2025-06-01': "Dragon Boat Festival",
        '2025-10-06': "Mid-autumn Festival",
        '2025-10-07': "Mid-autumn Festival",
        '2025-10-08': "Mid-autumn Festival",
        '2025-10-01': "National Day",
        '2025-10-02': "National Day",
        '2025-10-03': "National Day",
        '2025-10-04': "National Day",
        '2025-10-05': "National Day",
        '2025-10-06': "National Day",
        '2025-10-07': "National Day",
    }

    if dt.year <= 2022:
        return calendar.get_holiday_detail(dt)
    else:
        day = str(dt.date())
        if day in holidays:
            return (True, holidays[day])
        else:
            return (False, None)


def build_date_features(sdate, edate, col_dt='dt'):
    short_holiday = ["New Year's Day", 'Tomb-sweeping Day', 'Dragon Boat Festival', 'Mid-autumn Festival']
    long_holiday = ['National Day', 'Labour Day', 'Spring Festival']
    special_holiday = ['Spring Festival']
    holiday_type = {}
    for holiday in short_holiday:
        holiday_type[holiday] = 'Short'
    for holiday in long_holiday:
        holiday_type[holiday] = 'Long'
    for holiday in special_holiday:
        holiday_type[holiday] = 'Special'
    holiday_type['None'] = ''

    feats = pd.DataFrame(pd.date_range(start=sdate, end=edate), columns=[col_dt])
    feats['week'] = feats.dt.dt.isocalendar().week.astype(int)
    feats['year'] = feats.dt.dt.year.astype(int)
    feats['year'] = feats.apply(lambda x: x.year - 1 if (x.week >= 50) & (x['dt'].month < 2) else x.year, axis=1)
    feats['year'] = feats.apply(lambda x: x.year + 1 if (x.week < 2) & (x['dt'].month > 11) else x.year, axis=1)
    feats['week_day'] = feats.dt.dt.weekday
    feats['month_dt'] = feats.dt.apply(lambda x: (datetime.date(year=x.year, month=x.month, day=1)).strftime("%Y%m%d"))
    feats['week_dt'] = feats.dt.apply(lambda x: (x - datetime.timedelta(days=x.weekday())).strftime("%Y%m%d"))
    feats['is_weekend'] = feats.week_day > 4
    feats['is_holiday'] = feats.dt.apply(lambda x: get_holiday(x)[0])
    feats['is_workday'] = ~feats.is_holiday
    feats['holiday'] = feats.dt.apply(lambda x: get_holiday(x)[1]).astype(str)
    feats['is_holiday'] = feats.apply(lambda x: (~x.is_workday) & (x.holiday != 'None'), axis=1).astype(int)
    feats['holiday_first_day'] = feats.is_holiday.diff().apply(lambda x: x == 1)
    feats['holiday_last_day'] = feats.is_holiday.diff(-1).apply(lambda x: x == 1)
    feats['holiday_pre_day'] = feats.is_holiday.diff(-1).apply(lambda x: x == -1)
    feats['holiday_next_day'] = feats.holiday.shift(-1)
    feats['holiday'] = feats.apply(lambda x: x.holiday_next_day if x.holiday_pre_day else x.holiday, axis=1)
    feats['holiday_short'] = feats.holiday.apply(lambda x: x in short_holiday)
    feats['holiday_short'] = feats.apply(lambda x: x.is_holiday & x.holiday_short, axis=1)
    feats['holiday_long'] = feats.holiday.apply(lambda x: x in long_holiday)
    feats['holiday_long'] = feats.apply(lambda x: x.is_holiday & x.holiday_long, axis=1)
    feats['holiday_special'] = feats.holiday.apply(lambda x: x in special_holiday)
    feats['is_holiday'] = feats['is_holiday'].astype(bool)
    feats['holiday_short'] = feats['holiday_short'].astype(bool)
    feats['holiday_long'] = feats['holiday_long'].astype(bool)
    return feats.drop(['holiday', 'holiday_next_day'], axis=1)


def bulid_date_daily_feature(spark, param):
    """日粒度日期特征"""

    col_list = param['col_list']
    ctype = param['mode_type']
    sdate = param['sdate']
    edate = param['edate']
    col_time = param['col_time']
    feat_func = eval(param['date_feature_daily_func'])
    output_table = param['date_features_daily_table']

    df = build_date_features(sdate, edate, col_time)
    for key in feat_func:
        time_aggs = feat_func[key][0]
        ws = feat_func[key][1]
        df[key] = df[key].apply(lambda x: 1 if x is True else 0)
        for w in ws:
            for time_agg in time_aggs:
                if time_agg == 'last':
                    df[key + '_last_' + str(w)] = df[key].rolling(window=w, min_periods=0).agg(np.sum)
                    col_list.append(key + '_last_' + str(w))
                if time_agg == 'future':
                    indexer = pd.api.indexers.FixedForwardWindowIndexer(window_size=w)
                    df[key + '_future_' + str(w)] = df[key].shift(-1).rolling(window=indexer, min_periods=0).agg(np.sum)
                    col_list.append(key + '_future_' + str(w))
    df[col_time] = df[col_time].apply(lambda x: x.strftime('%Y%m%d'))
    df = df[col_list]
    for u in df.columns:
        if df[u].dtype == bool:
            df[u] = df[u].astype('int')
    if ctype == 'sp':
        df_values = df.values.tolist()
        df_columns = df.columns.tolist()
        sparkdf = spark.createDataFrame(df_values, df_columns)
        save_table(spark, sparkdf, output_table, partition=["dt"])
        return sparkdf
    else:
        return df


def build_date_weekly_feature(spark, param):
    """周粒度日期特征"""

    col_key = param['col_key']
    ctype = param['model_type']
    sdate = param['sdate']
    edate = param['edate']
    col_time = param['col_time']
    agg_func = eval(param['date_feature_weekly_func'])
    output_table = param['date_features_weekly_table']
    df = build_date_features(sdate, edate, col_time)
    df = df.groupby(col_key).agg(agg_func).reset_index()
    for u in df.columns:
        if df[u].dtype == bool:
            df[u] = df[u].astype('int')
    df[col_time] = df[col_time].apply(lambda x: x.strftime('%Y%m%d'))
    if ctype == 'sp':
        df_values = df.values.tolist()
        df_columns = df.columns.tolist()
        sparkdf = spark.createDataFrame(df_values, df_columns)
        save_table(spark, sparkdf, output_table, partition=["dt"])
        return sparkdf
    else:
        return df


def build_date_monthly_feature(spark, param):
    """周粒度日期特征"""

    col_key = param['col_key']
    ctype = param['mode_type']
    sdate = param['sdate']
    edate = param['edate']
    col_time = param['col_time']
    agg_func = eval(param['date_feature_monthly_func'])
    output_table = param['date_features_monthly_table']
    df = build_date_features(sdate, edate, col_time)
    df = df.groupby(col_key).agg(agg_func).reset_index()
    for u in df.columns:
        if df[u].dtype == bool:
            df[u] = df[u].astype('int')
    df[col_time] = df[col_time].apply(lambda x: x.strftime('%Y%m%d'))
    if ctype == 'sp':
        df_values = df.values.tolist()
        df_columns = df.columns.tolist()
        sparkdf = spark.createDataFrame(df_values, df_columns)
        save_table(sparkdf, output_table, partition=["dt"])
        return sparkdf
    else:
        return df

# build_date_weekly_feature({'dt':'min','is_holiday':'sum', 'is_workday':'sum','holiday_first_day':'sum', 'holiday_short':'max',
#                            'holiday_long':'max', 'holiday_special':'max'},'20220101','20221231',col_dt='dt',ctype='pd').show()

# bulid_date_daily_feature({'holiday_short':(['last','future'],[5]),'holiday_long':(['last','future'],[14]),'is_workday':(['last'],[14])},'20220101','20221231',col_dt='dt',ctype='pd')