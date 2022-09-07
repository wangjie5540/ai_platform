import kfp
import kfp.dsl as dsl
from kfp.compiler import Compiler


# TODO 后续改成配置化


@dsl.pipeline(
    name="lookalike",
    description="for dev"
)
def ml_lookalike(train_data_start_date_str='', train_data_end_date_str='', run_datetime_str='', solution_id='',
                 instance_id=''):
    dsl.ContainerOp(
        name="lookalike",
        image='digit-force-docker.pkg.coding.net/ai-platform/ai-components/ml-lookalike',
        command=['python', 'main.py'],
        arguments=['--solution_id', solution_id, '--instance_id', instance_id],
    )


Compiler().compile(ml_lookalike, 'lookalike.yaml')

client = kfp.Client(host='http://172.22.20.13:30000/pipeline',
                    cookies="authservice_session=MTY2MjQ1MzQ3OXxOd3dBTkROS1ZFUkdNekpJUVVWT1JUUkNUMEkxUVZKUldEUkZUalEyVTBKWVdGQk9NMGRMVVZGWVJGTlVVMUpQVlZwVFR6VXlVbEU9fGskAOguKG0flNWKrk0EOtDuv0sCmRNQm27kvWpyn1mK")
client.upload_pipeline('/data/pycharm_project_768/pipelines/lookalike.yaml', pipeline_name='lookalike')
# client.create_run_from_pipeline_func(ml_lookalike, arguments={}, namespace='kubeflow-user-example-com', experiment_name='my_test')
