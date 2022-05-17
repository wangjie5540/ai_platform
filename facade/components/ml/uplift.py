# coding: utf-8
from kfp.dsl import ContainerOp

import digitforce.aip.common.constants.global_constant as global_constant
from digitforce.aip.common.utils import component_helper

component_name = 'uplift'
version = 'latest'
image_name = f'{global_constant.image_registry}/{global_constant.registry_db}/{component_name}'
image_tag = 'latest'
image_full_name = f'{image_name}:{image_tag}'
out_1 = component_helper.get_output(component_name)


# TODO 后续做成门面模式，单独提供pip包
class UpliftOp(ContainerOp):
    def __init__(self, input_file, volume):
        super(UpliftOp, self).__init__(
            name=component_name,
            image=image_full_name,
            command=['python', 'uplift_model.py'],
            arguments=['--input_file', input_file],
            file_outputs={
                'out_1': out_1
            },
            pvolumes={'/mnt/nfs': volume}
        )
