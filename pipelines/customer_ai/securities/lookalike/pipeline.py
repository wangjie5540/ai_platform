from digitforce.aip.components.ml import LookalikeModel, LookalikeModelPredict
from digitforce.aip.components.preprocessing import ModelFeature2Dataset
from digitforce.aip.components.sample import *
from digitforce.aip.components.source.cos import Cos
from digitforce.aip.components.feature_engineering import *

pipeline_name = 'lookalike_dev'


@dsl.pipeline(name=pipeline_name)
def pipeline_func(global_params: str, flag='TRAIN'):
    RUN_ENV = "dev-wh"
    with dsl.Condition(flag != "PREDICT", name="is_not_predict"):
        raw_user_feature_op = RawUserFeatureOp(name='raw_user_feature', global_params=global_params, tag=RUN_ENV)
        raw_item_feature_op = RawItemFeatureOp(name='raw_item_feature', global_params=global_params, tag=RUN_ENV)
        zq_feature_op = ZqFeatureEncoderCalculator(
            name="zq_feature_calculator", global_params=global_params,
            raw_user_feature_table=raw_user_feature_op.outputs[
                RawUserFeatureOp.OUTPUT_KEY_RAW_USER_FEATURE],
            raw_item_feature_table=raw_item_feature_op.outputs[
                RawItemFeatureOp.OUTPUT_KEY_RAW_ITEM_FEATURE
            ],
            tag=RUN_ENV
        )
        model_user_feature_op = ModelUserFeatureOp(
            name='model_user_feature', global_params=global_params,
            raw_user_feature_table=raw_user_feature_op.outputs[RawUserFeatureOp.OUTPUT_KEY_RAW_USER_FEATURE],
            raw_item_feature_table=raw_item_feature_op.outputs[RawItemFeatureOp.OUTPUT_KEY_RAW_ITEM_FEATURE],
            tag=RUN_ENV
        )
        model_user_feature_op.after(zq_feature_op)
        model_item_feature_op = ModelItemFeatureOp(
            name="model_item_feature", global_params=global_params,
            raw_item_feature_table=raw_item_feature_op.outputs[RawItemFeatureOp.OUTPUT_KEY_RAW_ITEM_FEATURE],
            tag=RUN_ENV
        )
        model_item_feature_op.after(zq_feature_op)
        op_sample_selection = SampleSelectionLookalike(name='sample_select', global_params=global_params, tag=RUN_ENV)
        model_sample_op = RawSample2ModelSample(
            name="raw_sample2model_sample", global_params=global_params,
            raw_sample_table_name=op_sample_selection.outputs[SampleSelectionLookalike.output_name],
            tag=RUN_ENV
        )
        model_sample_op.after(op_sample_selection)
        model_sample_op.after(zq_feature_op)
        to_dataset_op = ModelFeature2Dataset(
            name="feature_create", global_params=global_params,
            label_table_name=model_sample_op.outputs[RawSample2ModelSample.OUTPUT_KEY_MODEL_SAMPLE],
            model_user_feature_table_name=model_user_feature_op.outputs[
                ModelUserFeatureOp.OUTPUT_KEY_MODEL_USER_FEATURE],
            model_item_feature_table_name=model_item_feature_op.outputs[ModelItemFeatureOp.OUTPUT_KEY_RAW_ITEM_FEATURE],
            tag=RUN_ENV
        )
        with dsl.Condition(flag == 'TRAIN', name="is_train"):
            lookalike_model_op = LookalikeModel(
                "model", global_params,
                train_dataset_table_name=to_dataset_op.outputs[ModelFeature2Dataset.OUTPUT_KEY_TRAIN_DATASET],
                test_dataset_table_name=to_dataset_op.outputs[ModelFeature2Dataset.OUTPUT_KEY_TEST_DATASET],
                tag=RUN_ENV
            )
    with dsl.Condition(flag == "PREDICT", name="is_predict"):
        seeds_table_op = Cos("seeds_cos_url", global_params)
        predict_table_op = Cos("predict_cos_url", global_params)
        lookalike_model_predict_op = LookalikeModelPredict(
            "model_predict", global_params,
            seeds_crowd_table_name=seeds_table_op.outputs[Cos.OUTPUT_1],
            predict_crowd_table_name=predict_table_op.outputs[Cos.OUTPUT_1],
            tag=RUN_ENV
        )
