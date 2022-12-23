import kfp
import kfp.dsl as dsl
from kfp.dsl import Condition
from kfp.compiler import Compiler
from digitforce.aip.components.sample import SampleSelectionGaoqian
from digitforce.aip.components.feature_engineering import FeatureCreateGaoqian
from digitforce.aip.components.preprocessing import SampleCombGaoqian
from digitforce.aip.components.ml import Gaoqian

pipeline_name = 'goaqian'



@dsl.pipeline(name=pipeline_name)
def ml_gaoqian(global_params: str, flag='TRAIN'):
    op_sample_selection = SampleSelectionGaoqian(name='sample_select', global_params=global_params)
    op_feature_create = FeatureCreateGaoqian(name='feature_create', global_params=global_params,
                                               sample=op_sample_selection.outputs['sample'])
    op_sample_comb = SampleCombGaoqian(name="sample_comb", sample=op_sample_selection.outputs['sample'],
                                         user_feature=op_feature_create.outputs['user_feature'])
    with Condition(flag == 'TRAIN', name="is_train"):
        Gaoqian(name='gaoqian', global_params=global_params,
                  train_data=op_sample_comb.outputs['train_data'],
                  test_data=op_sample_comb.outputs['test_data'],
                  other_data=op_sample_comb.outputs['other_data'])


Compiler().compile(ml_gaoqian, f'{pipeline_name}.yaml')

client = kfp.Client(host='http://172.22.20.9:30000/pipeline',
                    cookies="authservice_session=MTY3MTUxODU2MXxOd3dBTkZjMFYweFhVMWxGUzFaRlNVdEJWMVJaTmpKS1VGWmFVVFpLTkZwUVZrTTFRamRMU1RKTE5WQkJOa05IVFRSVU1saFpNMUU9fCIYgxDlcikBKZa1xemcasfvfKQJCKIuLfJw3tDennu2")
client.upload_pipeline('/data/pycharm_project_710/pipelines/customer_ai/securities/lookalike.yaml',
                       pipeline_name='gaoqian_tsx_test')
# client.create_run_from_pipeline_func(ml_lookalike, arguments={}, namespace='kubeflow-user-example-com', experiment_name='my_test')
