import os

os.environ["RUN_ENV"] = "dev"
import kfp
import digitforce.aip.common.utils.kubeflow_helper as kubeflow_helper
from digitforce.aip.common.utils import config_helper
from digitforce.aip.components.ml import LookalikeModel
from digitforce.aip.components.preprocessing import ModelFeature2Dataset
from digitforce.aip.components.sample import *

pipeline_name = 'lookalike_zxr'
pipeline_path = f'/tmp/{pipeline_name}.yaml'

from digitforce.aip.components.feature_engineering import *


@dsl.pipeline(name=pipeline_name)
def ml_lookalike(global_params: str, flag='TRAIN'):
    with dsl.Condition(flag != "PREDICT", name="is_not_predict"):
        raw_user_feature_op = \
            RawUserFeatureOp(name='raw_user_feature', global_params=global_params)  # todo 第一个组件生成的表名
        raw_user_feature_op.container.set_image_pull_policy("Always")
        raw_item_feature_op = RawItemFeatureOp(name='raw_item_feature', global_params=global_params)  # todo 第一个组件生成的表名
        raw_item_feature_op.container.set_image_pull_policy("Always")

        zq_feature_op = ZqFeatureEncoderCalculator(name="zq_feature_calculator", global_params=global_params,
                                                   raw_user_feature_table=raw_user_feature_op.outputs[
                                                       RawUserFeatureOp.OUTPUT_KEY_RAW_USER_FEATURE],
                                                   raw_item_feature_table=raw_item_feature_op.outputs[
                                                       RawItemFeatureOp.OUTPUT_KEY_RAW_ITEM_FEATURE
                                                   ])

        zq_feature_op.container.set_image_pull_policy("Always")
        model_user_feature_op = ModelUserFeatureOp(name='model_user_feature', global_params=global_params,
                                                   raw_user_feature_table=raw_user_feature_op.outputs[
                                                       RawUserFeatureOp.OUTPUT_KEY_RAW_USER_FEATURE],
                                                   raw_item_feature_table=raw_item_feature_op.outputs[
                                                       RawItemFeatureOp.OUTPUT_KEY_RAW_ITEM_FEATURE
                                                   ])
        model_user_feature_op.after(zq_feature_op)
        model_user_feature_op.container.set_image_pull_policy("Always")
        model_item_feature_op = ModelItemFeatureOp(name="model_item_feature", global_params=global_params,
                                                   raw_item_feature_table=raw_item_feature_op.outputs[
                                                       RawItemFeatureOp.OUTPUT_KEY_RAW_ITEM_FEATURE
                                                   ])
        model_item_feature_op.after(zq_feature_op)
        model_item_feature_op.container.set_image_pull_policy("Always")
        # todo remove table
        op_sample_selection = SampleSelectionLookalike(name='sample_select', global_params=global_params)
        op_sample_selection.container.set_image_pull_policy("Always")
        model_sample_op = RawSample2ModelSample(name="raw_sample2model_sample", global_params=global_params,
                                                raw_sample_table_name="algorithm.tmp_aip_sample")  # todo
        model_sample_op.after(op_sample_selection)
        model_sample_op.container.set_image_pull_policy("Always")
        to_dataset_op = ModelFeature2Dataset(name="feature_and_label_to_dataset", global_params=global_params,
                                             label_table_name=model_sample_op.outputs[
                                                 RawSample2ModelSample.OUTPUT_KEY_MODEL_SAMPLE],
                                             model_user_feature_table_name=model_user_feature_op.outputs[
                                                 ModelUserFeatureOp.OUTPUT_KEY_MODEL_USER_FEATURE],
                                             model_item_feature_table_name=model_item_feature_op.outputs[
                                                 ModelItemFeatureOp.OUTPUT_KEY_RAW_ITEM_FEATURE
                                             ]
                                             )
        to_dataset_op.container.set_image_pull_policy("Always")
        with dsl.Condition(flag == 'TRAIN', name="is_train"):
            lookalike_model_op = LookalikeModel("model", global_params,
                                                train_dataset_table_name=to_dataset_op.outputs[
                                                    ModelFeature2Dataset.OUTPUT_KEY_TRAIN_DATASET],
                                                test_dataset_table_name=to_dataset_op.outputs[
                                                    ModelFeature2Dataset.OUTPUT_KEY_TEST_DATASET
                                                ])
            lookalike_model_op.container.set_image_pull_policy("Always")

    # with dsl.Condition(flag == "PREDICT", name="is_predict"):
    #     # todo
    #     pass


kubeflow_config = config_helper.get_module_config("kubeflow")
client = kfp.Client(host="http://172.22.20.9:30000/pipeline", cookies=kubeflow_helper.get_istio_auth_session(
    url=kubeflow_config['url'], username=kubeflow_config['username'],
    password=kubeflow_config['password'])['session_cookie'])
import json

global_params = json.dumps({
    "model_item_feature": {"model_item_feature_table_name": "algorithm.tmp_model_item_feature_table_name"},
    "model_user_feature": {"model_user_feature_table_name": "algorithm.tmp_model_user_feature_table_name"},
    "sample_select": {},
    "zq_feature_calculator": {"raw_user_feature_table_name": "algorithm.tmp_raw_user_feature_table_name",
                              "raw_item_feature_table_name": "algorithm.tmp_raw_item_feature_table_name"},
    "raw_user_feature": {"raw_user_feature_table_name": "algorithm.tmp_raw_user_feature_table_name_1"},
    "raw-item-feature": {"raw_item_feature_table_name": "algorithm.tmp_raw_item_feature_table_name_1"},
    "model-item-feature": {"model_item_feature_table_name": "algorithm.tmp_model_item_feature_table_name"},
    "raw_sample2model_sample": {"model_sample_table_name": "algorithm.tmp_aip_model_sample"},
    "feature_and_label_to_dataset": {},
    "model": {"lr": 0.01, "dnn_dropout": 0.5, "batch_size": 1024,  # "is_automl": False,
              "model_user_feature_table_name": "algorithm.tmp_model_user_feature_table_name",
              "user_vec_table_name": "algorithm.tmp_user_vec_table_name"},
    "user_feature_trans": {
        "create": {
            "transform_rules": [
                {
                    "type": "string_indexer",
                    "input_col": "user_id",
                    "output_col": "user_id_index"
                },
                {
                    "type": "string_indexer",
                    "input_col": "gender",
                    "output_col": "gender_index"
                }
            ]
        }
    }
})
client.create_run_from_pipeline_func(ml_lookalike, arguments={"global_params": global_params},
                                     experiment_name="recommend",
                                     namespace='kubeflow-user-example-com')
client.create_run_from_pipeline_func(ml_lookalike, arguments={"global_params": global_params, "flag": "AUTOML"},
                                     experiment_name="recommend",
                                     namespace='kubeflow-user-example-com')
