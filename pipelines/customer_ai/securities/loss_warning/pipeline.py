import kfp
import kfp.dsl as dsl
from kfp.dsl import Condition

import digitforce.aip.common.utils.kubeflow_helper as kubeflow_helper
from digitforce.aip.common.utils import config_helper
from digitforce.aip.components.feature_engineering import FeatureCreateLiushi
from digitforce.aip.components.feature_engineering import FeatureCreateLiushiPredict
from digitforce.aip.components.ml import LiushiModel
from digitforce.aip.components.ml import LiushiPredict
from digitforce.aip.components.sample import SampleSelectionLiushi
from digitforce.aip.components.source.cos import Cos
from kfp.compiler import Compiler

pipeline_name = 'loss_warning'
pipeline_path = f'/tmp/{pipeline_name}.yaml'


@dsl.pipeline(name=pipeline_name)
def ml_loss_warning(global_params: str, flag='TRAIN'):
    RUN_ENV = "dev"
    with Condition(flag != "PREDICT", name="is_not_predict"):
        op_sample_selection = SampleSelectionLiushi(name='sample_select', global_params=global_params, tag=RUN_ENV)
        op_sample_selection.container.set_image_pull_policy("Always")

        op_feature_create = FeatureCreateLiushi(name='feature_create', global_params=global_params, tag=RUN_ENV,
                                                sample=op_sample_selection.outputs['sample_table_name'])
        op_feature_create.container.set_image_pull_policy("Always")

        with Condition(flag == 'TRAIN', name="is_train"):
            op_sample_comb = LiushiModel(name="model", global_params=global_params, tag=RUN_ENV,
                                         train_data=op_feature_create.outputs[
                                             op_feature_create.OUTPUT_TRAIN_FEATURE
                                         ],
                                         test_data=op_feature_create.outputs[
                                             op_feature_create.OUTPUT_TEST_FEATURE
                                         ])
            op_sample_comb.container.set_image_pull_policy("Always")
    with Condition(flag == "PREDICT", name="is_predict"):
        predict_table_op = Cos("predict_cos_url",global_params)
        predict_table_op.container.set_image_pull_policy("Always")
        predict_feature_op = FeatureCreateLiushiPredict(name="feature_create_predict", global_params=global_params,
                                                        sample=predict_table_op.outputs[Cos.OUTPUT_1],
                                                        tag=RUN_ENV)
        predict_feature_op.container.set_image_pull_policy("Always")
        liushi_predict_op = LiushiPredict(name="model_predict", global_params=global_params, tag=RUN_ENV,
                                          predict_table_name=predict_feature_op.outputs[
                                              predict_feature_op.OUTPUT_PREDICT_FEATURE
                                          ])
        liushi_predict_op.container.set_image_pull_policy("Always")