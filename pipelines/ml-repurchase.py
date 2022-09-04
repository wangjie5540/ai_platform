import kfp
import kfp.dsl as dsl
from kfp.compiler import Compiler


# TODO 后续改成配置化


@dsl.pipeline(
    name="repurchase",
    description="for dev"
)
def ml_repurchase(train_data_start_date_str='', train_data_end_date_str='', run_datetime_str='', solution_id='',
                  instance_id=''):
    dsl.ContainerOp(
        name="repurchase",
        image='digit-force-docker.pkg.coding.net/ai-platform/ai-components/ml-repurchase:latest',
        command=['python', 'main.py'],
        arguments=[],
    )


Compiler().compile(ml_repurchase, 'repurchase.yaml')
client = kfp.Client(host='http://172.22.20.13:30000/pipeline',
                    cookies="authservice_session=MTY2MjE4MjA3NHxOd3dBTkVnMFRVNVFXVWMwUlZWYVZVSkJSa1pGUTB4SlNUWkJOMWRCVFVKSE1sTk9RMVZLVlZWU1RVSXpTMUF6TWtVMFRFbFlOVkU9fDjYhy-YUVJHArEgdrjhVXMCa-WXnQE_9tlKgRTlaDIu")
client.upload_pipeline('/data/pycharm_project_768/pipelines/repurchase.yaml', pipeline_name='repurchase',
                       description='for dev')
