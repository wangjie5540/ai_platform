import kfp
import kfp.dsl as dsl
from kfp.dsl import Condition
from kfp.compiler import Compiler
from digitforce.aip.components.source import ReadTable
from digitforce.aip.components.sample import SampleSelectionLookalike

pipeline_name = 'lookalike'


@dsl.pipeline(name=pipeline_name)
def ml_lookalike(global_params: str, flag='TRAIN'):
    op_extract_data = ReadTable(name='extract_data', global_params=global_params)
    with Condition(flag == 'PREDICT'):
        op_sample_selection_lookalike = SampleSelectionLookalike(name='sample_select', global_params=global_params,
                                                                 data_input=op_extract_data.outputs['out'])


Compiler().compile(ml_lookalike, f'{pipeline_name}.yaml')

client = kfp.Client(host='http://172.22.20.9:30000/pipeline',
                    cookies="authservice_session=MTY3MTM2ODIxMXxOd3dBTkVWRU5VOUxSMGN6U2pSVlJFMHpSMUl6TTFFek5rOVBXVXd6VDBoSVRqSlRUMU5YUjFST1FrZENXakpGUXpOWlZWaE1XVkU9fHX2w3DmIkytxXYcJ4RVdIrL9akN8rdhzUU_ewopgV9V")
client.upload_pipeline('/data/pycharm_project_710/pipelines/customer_ai/securities/lookalike.yaml',
                       pipeline_name='lookalike_wtg_test')
# client.create_run_from_pipeline_func(ml_lookalike, arguments={}, namespace='kubeflow-user-example-com', experiment_name='my_test')
