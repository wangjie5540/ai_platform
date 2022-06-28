import pandas as pd


def soften(input_file, output_file, sep=',', soften_method='z-score', cols=None, thresh_max=None, thresh_min=None, percent_max=None,
           percent_min=None):
    """
    特征异常平滑
    @param input_file: 输入数据
    @param output_file: 输出文件路径
    @param sep: 分隔符
    @param soften_method: 平滑方式(z-score, thresh, percent, box_plot)
    @param cols: list, 平滑列名
    @param thresh_max: 阈值上限，当平滑方式为阈值时，需配置改参数
    @param thresh_min: 阈值下限，当平滑方式为阈值时，需配置改参数
    @param percent_max: 百分数上限， 当平滑方式为百分位时，需配置改参数
    @param percent_min: 百分数下限， 当平滑方式为百分位时，需配置改参数
    """
    df = pd.read_csv(input_file, sep=sep)
    if soften_method == 'z-score':
        df[cols] = df[cols].apply(z_score_soften, axis=0)
    elif soften_method == 'thresh':
        df[cols] = df[cols].clip(thresh_min, thresh_max)
    elif soften_method == 'box_plot':
        df[cols] = df[cols].apply(boxplot_soften, axis=0)
    elif soften_method == 'percent':
        df[cols] = df[cols].apply(percent_soften, args=(percent_max, percent_min), axis=0)
    else:
        raise ValueError('请选择平滑方式')
    df.to_csv(output_file, sep=sep, index=False)


def z_score_soften(series):
    std = series.std()
    mean = series.mean()
    min_value = mean - std * 3
    max_value = mean + std * 3
    return series.clip(min_value, max_value)


def boxplot_soften(series):
    q1 = series.quantile(0.25)
    q3 = series.quantile(0.75)
    iqr = q3 - q1
    return series.clip(q1 - 1.5 * iqr, q3 + 1.5 * iqr)


def percent_soften(series, percent_max, percent_min):
    min_value = series.quantile(percent_min)
    max_value = series.quantile(percent_max)
    return series.clip(min_value, max_value)