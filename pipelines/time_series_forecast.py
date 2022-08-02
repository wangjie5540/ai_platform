import kfp
import kfp.dsl as dsl

from digitforce.aip.components.forecast.data_prepare import data_prepare
from digitforce.aip.components.forecast.series_predict import time_series_predict

name = "time series predict"
description = 'first prepare data then train time series model'


@dsl.pipeline(
    name=name,
    description=description,
)
def time_series_forecast(forecast_start_date, purpose, time_type, sdate, edate):
    # time series forecast
    time_series_forecast = time_series_predict.time_series_predict(forecast_start_date, purpose, time_type)
    time_series_forecast.container.set_image_pull_policy("Always")

    # data_prepare
    prepare_data = data_prepare.data_prepare(sdate, edate).after(time_series_forecast)
    prepare_data.container.set_image_pull_policy("Always")


def upload_pipeline():
    pipeline_path = "/data/aip/pipelines/" + name + ".yaml"
    kfp.compiler.Compiler().compile(time_series_forecast, pipeline_path)
    client = kfp.Client()
    client.upload_pipeline(pipeline_path, name, description)


def main():
    upload_pipeline()


if __name__ == "__main__":
    main()
