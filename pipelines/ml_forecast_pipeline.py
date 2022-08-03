import kfp
import kfp.dsl as dsl

from digitforce.aip.components.forecast.data_prepare import data_prepare
from digitforce.aip.components.forecast.series_predict import time_series_predict
from digitforce.aip.components.forecast.data_processing import big_order_filter, sales_aggregation, no_sales_restore, \
    sales_fill_zero, sales_boxcox_filter
from digitforce.aip.components.forecast.feature_processing import build_sales_feature_weekly, build_sales_feature_daily, \
    build_date_feature_daily, build_date_feature_monthly, build_weather_feature_monthly, build_weather_feature_daily, \
    build_weather_feature_weekly, build_sales_feature_monthly
from digitforce.aip.components.forecast.ml_model import ml_train

name = "ml_forecast_pipeline"
description = 'ml_forecast_pipeline'


@dsl.pipeline(
    name=name,
    description=description,
)
def ml_forecast_pipeline(forecast_start_date, purpose, time_type, sdate, edate, col_qty, col_openinv, join_key,
                         fill_value, col_endinv, col_category, col_time, w, input_table, output_table, agg_func,
                         agg_type, col_key, col_weather_list, dict_agg_func):
    # 大单过滤
    big_order_filter_ = big_order_filter.forecast_big_order_filter(sdate, edate)
    big_order_filter_.container.set_image_pull_policy("Always")

    # 天维度聚合
    sales_agg = sales_aggregation.sales_aggregation(sdate, edate, input_table, output_table, agg_func, col_qty,
                                                    agg_type).after(big_order_filter_)
    sales_agg.container.set_image_pull_policy("Always")

    # 0库存销填补
    no_sales = no_sales_restore.no_sales_restore(sdate, edate, col_openinv, col_endinv, col_category, col_time, w,
                                                 col_qty, join_key).after(sales_agg)
    no_sales.container.set_image_pull_policy("Always")

    # 无销量填0
    sales_fill = sales_fill_zero.sales_fill_zero(sdate, edate, col_openinv, col_qty, join_key, fill_value).after(
        no_sales)
    sales_fill.container.set_image_pull_policy("Always")

    # boxcox
    boxcox_file = sales_boxcox_filter.sales_boxcox_filter(sdate, edate, col_qty).after(sales_fill)
    boxcox_file.container.set_image_pull_policy("Always")

    # 周维度聚合
    sales_agg_week = sales_aggregation.sales_aggregation(sdate, edate, input_table, output_table, agg_func, col_qty,
                                                         agg_type).after(boxcox_file)
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

    # 天维度的特征生成
    sales_feature_daily = build_sales_feature_daily.build_sales_feature_daily(sdate, edate, col_qty).after(boxcox_file)
    sales_feature_daily.container.set_image_pull_policy("Always")
    weather_feature_daily = build_weather_feature_daily.build_weather_feature_daily(sdate, edate, col_key,
                                                                                    col_weather_list, dict_agg_func)
    weather_feature_daily.container.set_image_pull_policy("Always")
    # 周维度的特征生成
    sales_feature_weekly = build_sales_feature_weekly.build_sales_feature_weekly(sdate, edate, col_time, col_qty).after(
        sales_agg_week)
    sales_feature_weekly.container.set_image_pull_policy("Always")

    weather_feature_weekly = build_weather_feature_weekly.build_weather_feature_weekly(sdate, edate, col_weather_list,
                                                                                       col_key, join_key)
    weather_feature_weekly.container.set_image_pull_policy("Always")

    # 汇总到模型训练

    ml_train_task = ml_train.ml_model_train(sdate, edate).after(sales_feature_daily, weather_feature_daily,
                                                                time_series_forecast_day, sales_feature_weekly,
                                                                weather_feature_weekly)

    ml_train_task.container.set_image_pull_policy("Always")



def upload_pipeline():
    pipeline_path = "/data/aip/pipelines/" + name + ".yaml"
    kfp.compiler.Compiler().compile(ml_forecast_pipeline, pipeline_path)
    client = kfp.Client()
    client.upload_pipeline(pipeline_path, name, description)


def main():
    upload_pipeline()


if __name__ == "__main__":
    main()
