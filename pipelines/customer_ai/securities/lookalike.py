import os
import kfp

import digitforce.aip.common.utils.kubeflow_helper as kubeflow_helper
from digitforce.aip.common.utils import config_helper
from digitforce.aip.components.ml import LookalikeModel, LookalikeModelPredict
from digitforce.aip.components.preprocessing import ModelFeature2Dataset
from digitforce.aip.components.sample import *
from digitforce.aip.components.source.cos import Cos
from kfp.compiler import Compiler

pipeline_name = 'lookalike_dev'
pipeline_path = f'/tmp/{pipeline_name}.yaml'

from digitforce.aip.components.feature_engineering import *


@dsl.pipeline(name=pipeline_name)
def ml_lookalike(global_params: str, flag='TRAIN'):
    RUN_ENV = "dev-wh"
    with dsl.Condition(flag != "PREDICT", name="is_not_predict"):
        raw_user_feature_op = \
            RawUserFeatureOp(name='raw_user_feature', global_params=global_params, tag=RUN_ENV)
        raw_user_feature_op.container.set_image_pull_policy("Always")
        raw_item_feature_op = RawItemFeatureOp(name='raw_item_feature', global_params=global_params, tag=RUN_ENV)
        raw_item_feature_op.container.set_image_pull_policy("Always")

        zq_feature_op = ZqFeatureEncoderCalculator(name="zq_feature_calculator", global_params=global_params,
                                                   raw_user_feature_table=raw_user_feature_op.outputs[
                                                       RawUserFeatureOp.OUTPUT_KEY_RAW_USER_FEATURE],
                                                   raw_item_feature_table=raw_item_feature_op.outputs[
                                                       RawItemFeatureOp.OUTPUT_KEY_RAW_ITEM_FEATURE
                                                   ], tag=RUN_ENV)

        zq_feature_op.container.set_image_pull_policy("Always")
        model_user_feature_op = ModelUserFeatureOp(name='model_user_feature', global_params=global_params,
                                                   raw_user_feature_table=raw_user_feature_op.outputs[
                                                       RawUserFeatureOp.OUTPUT_KEY_RAW_USER_FEATURE],
                                                   raw_item_feature_table=raw_item_feature_op.outputs[
                                                       RawItemFeatureOp.OUTPUT_KEY_RAW_ITEM_FEATURE
                                                   ], tag=RUN_ENV)
        model_user_feature_op.after(zq_feature_op)
        model_user_feature_op.container.set_image_pull_policy("Always")
        model_item_feature_op = ModelItemFeatureOp(name="model_item_feature", global_params=global_params,
                                                   raw_item_feature_table=raw_item_feature_op.outputs[
                                                       RawItemFeatureOp.OUTPUT_KEY_RAW_ITEM_FEATURE
                                                   ], tag=RUN_ENV)
        model_item_feature_op.after(zq_feature_op)
        model_item_feature_op.container.set_image_pull_policy("Always")
        op_sample_selection = SampleSelectionLookalike(name='sample_select', global_params=global_params, tag=RUN_ENV)
        op_sample_selection.container.set_image_pull_policy("Always")
        model_sample_op = RawSample2ModelSample(name="raw_sample2model_sample", global_params=global_params,
                                                raw_sample_table_name=op_sample_selection.outputs[
                                                    SampleSelectionLookalike.output_name
                                                ], tag=RUN_ENV)
        model_sample_op.after(op_sample_selection)
        model_sample_op.after(zq_feature_op)
        model_sample_op.container.set_image_pull_policy("Always")
        to_dataset_op = ModelFeature2Dataset(name="feature_create", global_params=global_params,
                                             label_table_name=model_sample_op.outputs[
                                                 RawSample2ModelSample.OUTPUT_KEY_MODEL_SAMPLE],
                                             model_user_feature_table_name=model_user_feature_op.outputs[
                                                 ModelUserFeatureOp.OUTPUT_KEY_MODEL_USER_FEATURE],
                                             model_item_feature_table_name=model_item_feature_op.outputs[
                                                 ModelItemFeatureOp.OUTPUT_KEY_RAW_ITEM_FEATURE
                                             ], tag=RUN_ENV
                                             )
        to_dataset_op.container.set_image_pull_policy("Always")
        with dsl.Condition(flag == 'TRAIN', name="is_train"):
            lookalike_model_op = LookalikeModel("model", global_params,
                                                train_dataset_table_name=to_dataset_op.outputs[
                                                    ModelFeature2Dataset.OUTPUT_KEY_TRAIN_DATASET],
                                                test_dataset_table_name=to_dataset_op.outputs[
                                                    ModelFeature2Dataset.OUTPUT_KEY_TEST_DATASET
                                                ], tag=RUN_ENV)
            lookalike_model_op.container.set_image_pull_policy("Always")

    with dsl.Condition(flag == "PREDICT", name="is_predict"):
        seeds_table_op = Cos("seeds_cos_url", global_params)
        seeds_table_op.container.set_image_pull_policy("Always")
        predict_table_op = Cos("predict_cos_url",global_params)
        predict_table_op.container.set_image_pull_policy("Always")
        lookalike_model_predict_op = LookalikeModelPredict("model_predict", global_params,
                                                           seeds_crowd_table_name=seeds_table_op.outputs[
                                                               Cos.OUTPUT_1
                                                           ],
                                                           predict_crowd_table_name=predict_table_op.outputs[
                                                               Cos.OUTPUT_1
                                                           ], tag=RUN_ENV)
        lookalike_model_predict_op.container.set_image_pull_policy("Always")


kubeflow_config = config_helper.get_module_config("kubeflow")
client = kfp.Client(host="http://172.22.20.9:30000/pipeline", cookies=kubeflow_helper.get_istio_auth_session(
    url=kubeflow_config['url'], username=kubeflow_config['username'],
    password=kubeflow_config['password'])['session_cookie'])
import json

global_params = json.dumps({
    "sample_select":
    {
    },
    "raw_user_feature":
    {
        "raw_user_feature_table_name": "algorithm.tmp_test_raw_user_feature"
    },
    "raw_item_feature":
    {
        "raw_item_feature_table_name": "algorithm.tmp_test_raw_item_feature"
    },
    "zq_feature_calculator":
    {
        "raw_user_feature_table_name": "algorithm.tmp_test_raw_user_feature",
        "raw_item_feature_table_name": "algorithm.tmp_test_raw_item_feature"
    },
    "raw_sample2model_sample":
    {
        "model_sample_table_name": "algorithm.tmp_aip_model_sample"
    },
    "model_item_feature":
    {
        "model_item_feature_table_name": "algorithm.tmp_model_item_feature_table_name"
    },
    "model_user_feature":
    {
        "model_user_feature_table_name": "algorithm.tmp_model_user_feature_table_name"
    },
    "feature_create":
    {},
    "model":
    {
        "lr": 0.01,
        "dnn_dropout": 0.5,
        "batch_size": 256,
        "model_user_feature_table_name": "algorithm.tmp_model_user_feature_table_name",
        "user_vec_table_name": "algorithm.tmp_user_vec_table_name",
        "model_and_metrics_data_hdfs_path": "/user/ai/aip/zq/lookalike/model/112233"
    },
    "seeds_cos_url":
    {
        "url": "https://algorithm-1308011215.cos.ap-beijing.myqcloud.com/aip_test_lookalike_seeds.csv",
        "columns": "user_id"
    },
    "predict_cos_url":
    {
        "url": "https://algorithm-1308011215.cos.ap-beijing.myqcloud.com/aip_test_lookalike_predict.csv",
        "columns": "user_id"
    },
    "model_predict":
    {
        "output_file_name": "result.csv",
        "user_vec_table_name": "algorithm.tmp_user_vec_table_name"
    }
})

# kubeflow_helper.upload_pipeline(ml_lookalike, pipeline_name)
# kubeflow_helper.upload_pipeline_version(ml_lookalike, kubeflow_helper.get_pipeline_id(pipeline_name),pipeline_name)
client.create_run_from_pipeline_func(ml_lookalike, arguments={"global_params": global_params, "flag": "TRAIN"},
                                     experiment_name="recommend",
                                     namespace='kubeflow-user-example-com')
# client.create_run_from_pipeline_func(ml_lookalike, arguments={"global_params": global_params, "flag": "AUTOML"},
#                                      experiment_name="recommend",
#                                      namespace='kubeflow-user-example-com')

#
# client.create_run_from_pipeline_func(ml_lookalike, arguments={"global_params": global_params, "flag": "PREDICT"},
#                                      experiment_name="recommend",
#                                      namespace='kubeflow-user-example-com')

# kubeflow_config = config_helper.get_module_config("kubeflow")
# pipeline_path = f"/tmp/{pipeline_name}.yaml"
# pipeline_conf = kfp.dsl.PipelineConf()
# pipeline_conf.set_image_pull_policy("Always")
# Compiler().compile(pipeline_func=ml_lookalike, package_path=pipeline_path, pipeline_conf=pipeline_conf)