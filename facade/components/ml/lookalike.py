# coding: utf-8
from kfp.dsl import ContainerOp

import digitforce.aip.common.constants.global_constant as global_constant
from digitforce.aip.common.utils import component_helper

component_name = 'lookalike'
version = 'latest'
image_name = f'{global_constant.IMAGE_REGISTRY}/{global_constant.REGISTRY_DB}/{component_name}'
image_tag = 'latest'
image_full_name = f'{image_name}:{image_tag}'
out_1 = component_helper.get_output(component_name)


# TODO 后续做成门面模式，单独提供pip包
class LookalikeOp(ContainerOp):
    def __init__(self, user_embedding_path, seed_file_path, crowd_file_path, volume):
        super(LookalikeOp, self).__init__(
            name=component_name,
            image=image_full_name,
            command=['python', 'lookalike.py'],
            arguments=['--user_embedding_path', user_embedding_path,
                       '--seed_file_path', seed_file_path,
                       '--crowd_file_path', crowd_file_path,
                       ],
            file_outputs={
                'out_1': out_1
            },
            pvolumes={'/mnt/nfs': volume}
        )
