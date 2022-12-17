import kfp
import kfp.dsl as dsl
from kfp.dsl import Condition
from kfp.compiler import Compiler
from digitforce.aip.components.source import ReadTable

pipeline_name = 'lookalike'


@dsl.pipeline(name=pipeline_name)
def ml_lookalike(global_params: str, flag='TRAIN'):
    op_read_table = ReadTable(global_params=global_params)


Compiler().compile(ml_lookalike, f'{pipeline_name}.yaml')

client = kfp.Client(host='http://172.22.20.9:30000/pipeline',
                    cookies="authservice_session=MTY3MTI0OTkyNHxOd3dBTkZoV1RVYzJWMWxSTTFwT1JVRmFORWhEVFU1V1drdFRVekpYVjFWVlREUlJTbFpDV1UxWVNFOVNWVVJPUjFwTVZqUktUMEU9fOm8vCcXXOgelqBzQ-4xXwi2ZE0SklfhPflyAjceGuWB")
client.upload_pipeline('/data/pycharm_project_710/pipelines/customer_ai/securities/lookalike.yaml', pipeline_name='lookalike_wtg_test')
# client.create_run_from_pipeline_func(ml_lookalike, arguments={}, namespace='kubeflow-user-example-com', experiment_name='my_test')
