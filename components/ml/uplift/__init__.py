from kfp.dsl import component, ContainerOp
import common.utils.resource_helper as resource_helper


@component
def component_uplift(input_file: str):
    return ContainerOp(
        name='operator uplift',
        image='xxxxxxxxxxxxx',
        command=['python', 'uplift_model.py'],
        arguments=[
            '--input_file',
            input_file,
        ],
        file_outputs={
            'model': resource_helper.apply_resource()
        }
    )
