# coding: utf-8
import kfp.dsl as dsl

from digitforce.aip.common.constants.global_constant import ENV
from digitforce.aip.components import BaseComponent
import digitforce.aip.common.constants.global_constant as global_constant


class LookalikeModel(BaseComponent):
    def __init__(self, name, global_params, train_dataset_table_name, test_dataset_table_name, tag='latest'):
        super().__init__(
            name=name,
            image=f'{global_constant.AI_PLATFORM_IMAGE_REPO_V2}/ml-lookalike',
            arguments=['--name', name, '--global_params', global_params,
                       '--train_dataset_table_name', train_dataset_table_name,
                       '--test_dataset_table_name', test_dataset_table_name,
                       ],
            tag=tag,
            file_outputs={
            }
        )


class LookalikeModelPredict(BaseComponent):
    def __init__(self, name, global_params,
                 seeds_crowd_table_name, predict_crowd_table_name,
                 tag='latest'):
        super().__init__(
            name=name,
            image=f'{global_constant.AI_PLATFORM_IMAGE_REPO_V2}/ml-lookalike_predict',
            arguments=['--name', name, '--global_params', global_params,
                       '--seeds_crowd_table_name', seeds_crowd_table_name,
                       '--predict_crowd_table_name', predict_crowd_table_name,
                       ],
            tag=tag,
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
    def __init__(self, name, global_params, train_data, test_data, tag='latest'):
        super(GaoqianModel, self).__init__(
            name=name,
            image=f'{global_constant.AI_PLATFORM_IMAGE_REPO}/'
                  f'ml-gaoqian',
            tag=tag,
            arguments=['--name', name, '--global_params', global_params, '--train_data', train_data,
                       '--test_data', test_data],
            file_outputs={
            }
        )


class GaoqianPredict(BaseComponent):
    def __init__(self, name, global_params, predict_table_name, tag='latest'):
        super(GaoqianPredict, self).__init__(
            name=name,
            image=f'{global_constant.AI_PLATFORM_IMAGE_REPO}/'
                  f'ml-gaoqian_predict',
            tag=tag,
            arguments=['--name', name, '--global_params', global_params, '--predict_table_name', predict_table_name,
                       ],
            file_outputs={
            }
        )


class DixiaohuModel(BaseComponent):
    def __init__(self, name, global_params, train_table_name, test_table_name, tag='latest'):
        super().__init__(
            name=name,
            image=f'{global_constant.AI_PLATFORM_IMAGE_REPO_V2}/ml-dixiaohu',
            arguments=['--name', name, '--global_params', global_params,
                       '--train_table_name', train_table_name, '--test_table_name', test_table_name],
            tag=tag,
            file_outputs={
            }
        )


class DixiaohuPredict(BaseComponent):
    def __init__(self, name, global_params, predict_table_name, tag='latest'):
        super().__init__(
            name=name,
            image=f'{global_constant.AI_PLATFORM_IMAGE_REPO_V2}/ml-dixiaohu_predict',
            arguments=['--name', name, '--global_params', global_params, '--predict_table_name', predict_table_name,
                       ],
            tag=tag,
            file_outputs={
            }
        )
