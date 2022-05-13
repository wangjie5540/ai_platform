# coding: utf-8
from kfp.dsl import ContainerOp, PipelineVolume

import common.constants.global_constant as global_constant
from common.utils import component_helper

component_name = 'uplift'
version = 'latest'
image_name = f'{global_constant.image_registry}/{global_constant.registry_db}/{component_name}'
image_tag = 'latest'
image_full_name = f'{image_name}:{image_tag}'
out_1 = component_helper.get_output(component_name)


# TODO 后续做成门面模式，单独提供pip包
class UpliftOp(ContainerOp):
    def __init__(self, input_1, image=image_full_name):
        super(UpliftOp, self).__init__(
            name=component_name,
            image=image,
            command=['python', 'uplift_model.py'],
            arguments=['--input_1', input_1],
            file_outputs={
                'out_1': out_1
            },
            pvolumes={global_constant.mount_nfs_dir: PipelineVolume(pvc=global_constant.default_pvc)}
        )
