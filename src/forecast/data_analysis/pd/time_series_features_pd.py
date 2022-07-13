import numpy as np
import pandas as pd
from kats.tsfeatures.tsfeatures import TsFeatures
from kats.consts import TimeSeriesData


def get_analysis_index(df, model, ts_col, y):
    #     if df.shape[0] < 10:
    #         return pd.DataFrame()
    df = df[ts_col]
    df[y] = df[y].apply(lambda x: x if x > 0 else 1)
    df.columns = ['time', 'value']
    ts = TimeSeriesData(df)
    output_feature = model.transform(ts)
    return pd.DataFrame(output_feature, index=[0])


def comp_time_series_features(df, f_cols=None, keys=None, ts_col=None, y='qty'):
    if f_cols is None:
        f_cols = ['seasonality_strength', 'trend_strength', 'seasonal_period', 'trend_mag', 'hw_alpha', 'hw_beta',
                  'hw_gamma', 'var', 'mean', 'entropy', 'hurst', 'linearity', 'crossing_points']
    if keys is None:
        keys = ['shop_id', 'goods_id']
    if ts_col is None:
        ts_col = ['dt', 'qty']

    model = TsFeatures(selected_features=f_cols)

    ts_features = df.groupby(keys).apply(get_analysis_index, model, ts_col, y).reset_index()
    return ts_features
