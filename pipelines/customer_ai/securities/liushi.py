import kfp.dsl as dsl
from kfp.dsl import Condition
from digitforce.aip.components.sample import SampleSelectionLookalike
from digitforce.aip.components.feature_engineering import FeatureCreateLookalike
from digitforce.aip.components.preprocessing import SampleCombLookalike
from digitforce.aip.components.ml import Lookalike
import digitforce.aip.common.utils.kubeflow_helper as kubeflow_helper

pipeline_name = 'lookalike_new'
pipeline_path = f'/tmp/{pipeline_name}.yaml'


@dsl.pipeline(name=pipeline_name)
def ml_lookalike(global_params: str, flag='TRAIN'):
    op_sample_selection = SampleSelectionLookalike(name='sample_select', global_params=global_params)
    op_feature_create = FeatureCreateLookalike(name='feature_create', global_params=global_params,
                                               sample=op_sample_selection.outputs['sample'])
    op_sample_comb = SampleCombLookalike(name="sample_comb", sample=op_sample_selection.outputs['sample'],
                                         user_feature=op_feature_create.outputs['user_feature'],
                                         item_feature=op_feature_create.outputs['item_feature'])
    with Condition(flag == 'TRAIN', name="is_train"):
        Lookalike(name='lookalike', global_params=global_params,
                  train_data=op_sample_comb.outputs['train_data'],
                  test_data=op_sample_comb.outputs['test_data'],
                  user_data=op_sample_comb.outputs['user_data'],
                  other_data=op_sample_comb.outputs['other_data'])


kubeflow_helper.upload_pipeline(ml_lookalike, pipeline_name)
