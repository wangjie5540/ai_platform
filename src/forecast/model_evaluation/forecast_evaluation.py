# -*- coding: utf-8 -*-
# @Time : 2022/07/11
# @Author : Arvin
from forecast.common.common_helper import *


def forecast_evaluation_wmape(df, col_true, col_pred, col_key=[], df_type='pd'):
    """
    计算 wmape
    """
    if df_type == 'sp':
        if len(col_key) == 0:
            wmape_df = df.groupBy(lit("total")).agg(
                psf.when(psf.sum(df.col_true) == 0, psf.sum(psf.abs(df.col_true - df.col_pred)) / 1).otherwise(
                    psf.sum(psf.abs(df.col_true - df.col_pred)) / psf.sum(df.col_true)).alias('wmape'))

        else:
            wmape_df = df.groupBy(col_key).agg(
                psf.when(psf.sum(df.col_true) == 0, psf.sum(psf.abs(df.col_true - df.col_pred)) / 1).otherwise(
                    psf.sum(psf.abs(df.col_true - df.col_pred)) / psf.sum(df.col_true)).alias('wmape'))
        return wmape_df

    else:
        df['error'] = abs(df[col_true] - df[col_pred])
        if len(col_key) == 0:
            df['total'] = 'total'
            agg_df = df.groupby(['total']).agg({col_true: sum, "error": sum})
        else:
            agg_df = df.groupby(col_key).agg({col_true: sum, "error": sum})
        agg_df[col_true] = agg_df[col_true].apply(lambda x: 1 if x == 0 else x)
        agg_df['wmape'] = agg_df['error'] / agg_df[col_true]
        agg_df = agg_df.reset_index()
        return agg_df
