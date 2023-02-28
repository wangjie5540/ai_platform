# coding: utf-8
import kfp.dsl as dsl

from digitforce.aip.common.constants.global_constant import ENV
from digitforce.aip.components import BaseComponent
import digitforce.aip.common.constants.global_constant as global_constant



class Lookalike(dsl.ContainerOp):
    def __init__(self, name, global_params, train_dataset, test_dataset, tag=ENV):
        super(Lookalike, self).__init__(
            name=name,
            image=f'digit-force-docker.pkg.coding.net/ai-platform/ai-components/'
                  f'ml-lookalike:{tag}',
            command=['python', 'main.py'],
            arguments=['--name', name, '--global_params', global_params, '--train_data', train_dataset,
                       '--test_data', test_dataset, ],
            file_outputs={
            }
        )


class LookalikeModel(dsl.ContainerOp):
    def __init__(self, name, global_params, train_dataset_table_name, test_dataset_table_name, tag=ENV):
        super(LookalikeModel, self).__init__(
            name=name,
            image=f'digit-force-docker.pkg.coding.net/ai-platform/ai-components/'
                  f'ml-lookalike:{tag}',
            command=['python', 'main.py'],
            arguments=['--name', name, '--global_params', global_params,
                       '--train_dataset_table_name', train_dataset_table_name,
                       '--test_dataset_table_name', test_dataset_table_name,
                       ],
            file_outputs={
            }
        )


class LookalikeModelPredict(dsl.ContainerOp):
    def __init__(self, name, global_params,
                 seeds_crowd_table_name, predict_crowd_table_name,  # todo 从数据源组件接收
                 tag=ENV):
        super(LookalikeModelPredict, self).__init__(
            name=name,
            image=f'digit-force-docker.pkg.coding.net/ai-platform/ai-components/'
                  f'ml-lookalike_predict:{tag}',
            command=['python', 'main.py'],
            arguments=['--name', name, '--global_params', global_params,
                       '--seeds_crowd_table_name', seeds_crowd_table_name,
                       '--predict_crowd_table_name', predict_crowd_table_name,
                       ],
            file_outputs={
            }
        )


class LiushiModel(BaseComponent):
    def __init__(self, name, global_params, train_data, test_data, tag='latest'):
        super().__init__(
            name=name,
            image=f'{global_constant.AI_PLATFORM_IMAGE_REPO}/ml-liushi',
            arguments=['--name', name, '--global_params', global_params, '--train_data', train_data,
                       '--test_data', test_data],
            tag=tag,
            file_outputs={
            }
        )


class LiushiPredict(BaseComponent):
    def __init__(self, name, global_params, predict_table_name, tag='latest'):
        super().__init__(
            name=name,
            image=f'{global_constant.AI_PLATFORM_IMAGE_REPO}/ml-liushi_predict',
            arguments=['--name', name, '--global_params', global_params, '--predict_table_name', predict_table_name,
                       ],
            tag=tag,
            file_outputs={
            }
        )

class GaoqianModel(BaseComponent):
    def __init__(self, name, global_params, train_data, test_data, tag=ENV):
        super(GaoqianModel, self).__init__(
            name=name,
            image=f'digit-force-docker.pkg.coding.net/ai-platform/ai-components/'
                  f'ml-gaoqian',
            tag=tag,
            arguments=['--name', name, '--global_params', global_params, '--train_data', train_data,
                       '--test_data', test_data],
            file_outputs={
            }
        )

class GaoqianPredict(BaseComponent):
    def __init__(self, name, global_params, predict_table_name, tag=ENV):
        super(GaoqianPredict, self).__init__(
            name=name,
            image=f'digit-force-docker.pkg.coding.net/ai-platform/ai-components/'
                  f'ml-gaoqian_predict',
            tag=tag,
            arguments=['--name', name, '--global_params', global_params, '--predict_table_name', predict_table_name,
                       ],
            file_outputs={
            }
        )
