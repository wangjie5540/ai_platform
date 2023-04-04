from digitforce.aip.components.feature_engineering import FeatureCreateGaoqian, FeatureCreateGaoqianPredict
from digitforce.aip.components.ml import GaoqianModel, GaoqianPredict
from digitforce.aip.components.sample import SampleSelectionGaoqian
from digitforce.aip.components.source.cos import Cos
import kfp.dsl as dsl

pipeline_name = 'gaoqian'

@dsl.pipeline(name=pipeline_name)
def pipeline_func(global_params: str, flag='TRAIN'):
    RUN_ENV = "3.0.0"
    with dsl.Condition(flag != "PREDICT", name="is_not_predict"):
        op_sample_selection = SampleSelectionGaoqian(name='sample_select', global_params=global_params, tag=RUN_ENV)
        op_feature_create = FeatureCreateGaoqian(name='feature_create', global_params=global_params, tag=RUN_ENV,
                                                sample=op_sample_selection.outputs['sample'])

        with dsl.Condition(flag == 'TRAIN', name="is_train"):
            op_sample_comb = GaoqianModel(name="model", global_params=global_params, tag=RUN_ENV,
                                         train_data=op_feature_create.outputs[
                                             op_feature_create.OUTPUT_TRAIN_FEATURE
                                         ],
                                         test_data=op_feature_create.outputs[
                                             op_feature_create.OUTPUT_TEST_FEATURE
                                         ])
    with dsl.Condition(flag == "PREDICT", name="is_predict"):
        predict_table_op = Cos("predict_cos_url",global_params)
        predict_feature_op = FeatureCreateGaoqianPredict(name="feature_create_predict", global_params=global_params,
                                                        sample=predict_table_op.outputs[Cos.OUTPUT_1],
                                                        tag=RUN_ENV)
        gaoqian_predict_op = GaoqianPredict(name="model_predict", global_params=global_params, tag=RUN_ENV,
                                          predict_table_name=predict_feature_op.outputs[
                                              predict_feature_op.OUTPUT_PREDICT_FEATURE
                                          ])
