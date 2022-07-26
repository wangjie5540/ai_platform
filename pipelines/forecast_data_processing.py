import kfp
import kfp.dsl as dsl

from digitforce.aip.components.preprocess import dataset
from digitforce.aip.components.preprocess import sql
from digitforce.aip.components.recommend import hot
from digitforce.aip.components.recommend import recall
from digitforce.aip.components.recommend.tmp import rank_data_process_op, lightgbm_train_op
from digitforce.aip.components.source import df_model_manager
from digitforce.aip.components.source import hive
from digitforce.aip.components.forecast.data_processing import big_order_filter
name = "ForecastDataProcessing"
description = ''


@dsl.pipeline(
    name=name,
    description=description,
)
def forecast_test_pipeline(train_data_start_date_str, train_data_end_date_str, run_datetime_str,
                                             solution_id, instance_id):
    # run_datetime_str = to_date_str(parse_date_str(run_datetime_str))

    show_and_action_table_maker = big_order_filter.forecast_big_order_filter(train_data_start_date_str, train_data_end_date_str)
    show_and_action_table_maker.container.set_image_pull_policy("Always")


def upload_pipeline():
    pipeline_path = "/data/aip/pipelines/" + name + ".yaml"
    kfp.compiler.Compiler().compile(forecast_test_pipeline, pipeline_path)
    client = kfp.Client()

    client.upload_pipeline(pipeline_path, name, description)


def main():

    upload_pipeline()


if __name__ == '__main__':
    main()

