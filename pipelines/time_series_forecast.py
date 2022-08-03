import kfp
import kfp.dsl as dsl

from digitforce.aip.components.forecast.data_prepare import data_prepare
from digitforce.aip.components.forecast.series_predict import time_series_predict
from digitforce.aip.components.forecast.data_processing import big_order_filter, sales_aggregation, no_sales_restore, \
    sales_fill_zero, sales_boxcox_filter

name = "time_series_predict"
description = 'firstPrepareDataThenTrainTimeSeriesModel'


@dsl.pipeline(
    name=name,
    description=description,
)
def time_series_forecast(forecast_start_date, purpose, time_type, sdate, edate, col_qty, col_qty_1, col_qty_2,
                         col_qty_3, col_openinv, join_key,
                         fill_value, col_endinv, col_category, col_time, w, input_table, output_table, agg_func):
    # 大单过滤
    big_order_filter_ = big_order_filter.forecast_big_order_filter(sdate, edate)
    big_order_filter_.container.set_image_pull_policy("Always")

    # 天维度聚合
    sales_agg = sales_aggregation.sales_aggregation(sdate, edate, input_table, output_table, agg_func, col_qty,
                                                    time_type).after(big_order_filter_)
    sales_agg.container.set_image_pull_policy("Always")

    # 0库存销填补
    no_sales = no_sales_restore.no_sales_restore(sdate, edate, col_openinv, col_endinv, col_category, col_time, w,
                                                 col_qty_1, join_key).after(sales_agg)
    no_sales.container.set_image_pull_policy("Always")

    # 无销量填0
    sales_fill = sales_fill_zero.sales_fill_zero(sdate, edate, col_openinv, col_qty_2, join_key, fill_value).after(
        no_sales)
    sales_fill.container.set_image_pull_policy("Always")

    # boxcox
    boxcox_file = sales_boxcox_filter.sales_boxcox_filter(sdate, edate, col_qty_3).after(sales_fill)
    boxcox_file.container.set_image_pull_policy("Always")

    # 周维度聚合
    sales_agg_week = sales_aggregation.sales_aggregation(sdate, edate, input_table, output_table, agg_func, col_qty,
                                                         time_type).after(boxcox_file)
    sales_agg_week.container.set_image_pull_policy("Always")

    # data_prepare
    prepare_data_day = data_prepare.data_prepare(sdate, edate).after(boxcox_file)
    prepare_data_day.container.set_image_pull_policy("Always")

    prepare_data_week = data_prepare.data_prepare(sdate, edate).after(sales_agg_week)
    prepare_data_week.container.set_image_pull_policy("Always")

    # time series forecast
    time_series_forecast_day = time_series_predict.time_series_predict(forecast_start_date, purpose, time_type).after(
        prepare_data_day)
    time_series_forecast_day.container.set_image_pull_policy("Always")

    time_series_forecast_week = time_series_predict.time_series_predict(forecast_start_date, purpose, time_type).after(
        prepare_data_week)
    time_series_forecast_week.container.set_image_pull_policy("Always")


def upload_pipeline():
    pipeline_path = "/data/aip/pipelines/" + name + ".yaml"
    kfp.compiler.Compiler().compile(time_series_forecast, pipeline_path)
    client = kfp.Client()
    client.upload_pipeline(pipeline_path, name, description)


def main():
    upload_pipeline()


if __name__ == "__main__":
    main()
