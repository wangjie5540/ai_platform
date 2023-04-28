from digitforce.aip.components.feature_engineering import (
    FeatureCreateDixiaohu,
    FeatureCreateDixiaohuPredict,
)
from digitforce.aip.components.ml import DixiaohuModel, DixiaohuPredict
from digitforce.aip.components.sample import SampleSelectionDixiaohu
from digitforce.aip.components.source.cos import Cos
from kfp.dsl import Condition
import kfp.dsl as dsl

pipeline_name = "dixiaohu_dev"


@dsl.pipeline(name=pipeline_name)
def pipeline_func(global_params: str, flag="TRAIN"):
    RUN_ENV = "dev"
    with Condition(flag != "PREDICT", name="is_not_predict"):
        op_sample_selection = SampleSelectionDixiaohu(
            name="sample_select", global_params=global_params, tag=RUN_ENV
        )

        op_feature_create = FeatureCreateDixiaohu(
            name="feature_create",
            global_params=global_params,
            tag=RUN_ENV,
            sample=op_sample_selection.outputs["sample_table_name"],
        )

        with Condition(flag == "TRAIN", name="is_train"):
            op_sample_comb = DixiaohuModel(
                name="model",
                global_params=global_params,
                tag=RUN_ENV,
                train_table_name=op_feature_create.outputs[
                    op_feature_create.OUTPUT_TRAIN_FEATURE
                ],
                test_table_name=op_feature_create.outputs[
                    op_feature_create.OUTPUT_TEST_FEATURE
                ],
            )
    with Condition(flag == "PREDICT", name="is_predict"):
        predict_table_op = Cos("predict_cos_url", global_params)

        predict_feature_op = FeatureCreateDixiaohuPredict(
            name="feature_create_predict",
            global_params=global_params,
            sample=predict_table_op.outputs[Cos.OUTPUT_1],
            tag=RUN_ENV,
        )
        dixiaohu_predict_op = DixiaohuPredict(
            name="model_predict",
            global_params=global_params,
            tag=RUN_ENV,
            predict_feature_table_name=predict_feature_op.outputs[
                predict_feature_op.OUTPUT_PREDICT_FEATURE
            ],
        )
