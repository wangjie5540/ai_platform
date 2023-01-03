import kfp.dsl as dsl
from kfp.dsl import Condition
from digitforce.aip.components.sample import SampleSelectionLookalike
from digitforce.aip.components.feature_engineering import FeatureCreateLookalike
from digitforce.aip.components.preprocessing import SampleCombLookalike, ModelFeature2Dataset
from digitforce.aip.components.ml import Lookalike
import digitforce.aip.common.utils.kubeflow_helper as kubeflow_helper
from digitforce.aip.components.sample import *

pipeline_name = 'lookalike_zxr'
pipeline_path = f'/tmp/{pipeline_name}.yaml'

from digitforce.aip.components.feature_engineering import *


@dsl.pipeline(name=pipeline_name)
def ml_lookalike(global_params: str, flag='TRAIN'):
    raw_user_feature_op = \
        RawUserFeatureOp(name='raw_user_feature', global_params=global_params)  # todo 第一个组件生成的表名
    raw_item_feature_op = RawItemFeatureOp(name='raw_item_feature', global_params=global_params)  # todo 第一个组件生成的表名

    model_user_feature_op = ModelUserFeatureOp(name='raw_user_feature', global_params=global_params,
                                               raw_user_feature_table=raw_user_feature_op.outputs[
                                                   RawUserFeatureOp.OUTPUT_KEY_RAW_USER_FEATURE], )
    model_user_feature_op.after(raw_user_feature_op)
    model_item_feature_op = ModelItemFeatureOp(name="model_item_feature", global_params=global_params,
                                               raw_item_feature_table=raw_item_feature_op.outputs[
                                                   RawItemFeatureOp.OUTPUT_KEY_RAW_ITEM_FEATURE
                                               ])
    model_item_feature_op.after(raw_item_feature_op)

    op_sample_selection = SampleSelectionLookalike(name='sample_select', global_params=global_params)
    model_sample_op = RawSample2ModelSample(name="raw_sample2model_sample", global_params=global_params,
                                            raw_sample_table_name="algorithm.tmp_aip_sample")  # todo
    model_sample_op.after(op_sample_selection)

    to_dataset_op = ModelFeature2Dataset(name="feature_and_label_to_dataset", global_params=global_params,
                                         label_table_name=model_sample_op.outputs[
                                             RawSample2ModelSample.OUTPUT_KEY_MODEL_SAMPLE],
                                         model_user_feature_table_name=model_user_feature_op.outputs[
                                             ModelUserFeatureOp.OUTPUT_KEY_MODEL_USER_FEATURE],
                                         model_item_feature_table_name=model_item_feature_op.outputs[
                                             ModelItemFeatureOp.OUTPUT_KEY_RAW_ITEM_FEATURE
                                         ]
                                         )
    to_dataset_op.after(model_item_feature_op)
    to_dataset_op.after(model_user_feature_op)
    to_dataset_op.after(model_sample_op)



kubeflow_helper.upload_pipeline(ml_lookalike, pipeline_name)
