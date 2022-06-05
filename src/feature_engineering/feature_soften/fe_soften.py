import pandas as pd


def soften(df, soften_method='z-score', cols=None, thresh_max=None, thresh_min=None, percent_max=None, percent_min=None):
    """
    特征异常平滑
    @param df: 输入数据
    @param soften_method: 平滑方式(z-score, thresh, percent)
    @param cols: 平滑列名
    @param thresh_max: 阈值上限，当平滑方式为阈值时，需配置改参数
    @param thresh_min: 阈值下限，当平滑方式为阈值时，需配置改参数
    @param percent_max: 百分数上限， 当平滑方式为百分位时，需配置改参数
    @param percent_min: 百分数下限， 当平滑方式为百分位时，需配置改参数
    """

    if soften_method == 'z-score':
        df[cols] = df[cols].apply(z_score_soften, axis=0)
    elif soften_method == 'thresh':
        df[cols].clip(thresh_min, thresh_max, inplace=True)
    elif soften_method == 'percent':
        df[cols] = df[cols].apply(percent_soften, axis=0)
        return df


def z_score_soften(series):
    std = series.std()
    mean = series.mean()
    min_value = mean - std * 3
    max_value = mean + std * 3
    return series.clip(min_value, max_value)


def percent_soften(series):
    q1 = series.quantile(0.25)
    q3 = series.quantile(0.75)
    iqr = q3 - q1
    return series.clip(q1 - 1.5 * iqr, q3 + 1.5 * iqr)
