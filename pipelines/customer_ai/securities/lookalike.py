import kfp
import kfp.dsl as dsl
from kfp.dsl import Condition
from kfp.compiler import Compiler
from digitforce.aip.components.source import ReadTable

pipeline_name = 'lookalike'


@dsl.pipeline(name=pipeline_name)
def ml_lookalike(global_params: str, flag):
    op_read_table = ReadTable(name='', global_params=global_params)


Compiler().compile(ml_lookalike, f'{pipeline_name}.yaml')

client = kfp.Client(host='http://172.22.20.13:30000/pipeline',
                    cookies="authservice_session=MTY2MjQ1MzQ3OXxOd3dBTkROS1ZFUkdNekpJUVVWT1JUUkNUMEkxUVZKUldEUkZUalEyVTBKWVdGQk9NMGRMVVZGWVJGTlVVMUpQVlZwVFR6VXlVbEU9fGskAOguKG0flNWKrk0EOtDuv0sCmRNQm27kvWpyn1mK")
client.upload_pipeline('/data/pycharm_project_768/pipelines/lookalike.yaml', pipeline_name='lookalike')
# client.create_run_from_pipeline_func(ml_lookalike, arguments={}, namespace='kubeflow-user-example-com', experiment_name='my_test')
