# coding: utf-8
import kfp.dsl as dsl

import digitforce.aip.common.utils.component_helper as component_helper
from digitforce.aip.common.constants.global_constant import ENV
from digitforce.aip.components import BaseComponent
import digitforce.aip.common.constants.global_constant as global_constant


class SampleSelectionLookalike(BaseComponent):
    """
    lookalike样本构造组件
    """
    output_name = 'sample'

    def __init__(self, name, global_params, tag='latest'):
        super().__init__(
            name=name,
            image=f'{global_constant.AI_PLATFORM_IMAGE_REPO_V2}/sample-sample_selection_lookalike',
            arguments=['--name', name, '--global_params', global_params],
            tag=tag,
            file_outputs={self.output_name: component_helper.generate_output_path(self.output_name)}
        )


class SampleSelectionLiushi(BaseComponent):
    """
    数据源-读取表组件
    """

    def __init__(self, name, global_params, tag='latest'):
        super().__init__(
            name=name,
            image=f'{global_constant.AI_PLATFORM_IMAGE_REPO}/sample-sample_selection_liushi',
            arguments=['--name', name, '--global_params', global_params],
            tag=tag,
            file_outputs={"sample_table_name": component_helper.generate_output_path("sample_table_name")}
        )


class SampleSelectionGaoqian(BaseComponent):
    """
    高潜样本组件
    """

    def __init__(self, name, global_params, tag='latest'):
        super(SampleSelectionGaoqian, self).__init__(
            name=name,
            image=f'{global_constant.AI_PLATFORM_IMAGE_REPO}/'
                  f'sample-sample_selection_gaoqian',
            tag=tag,
            arguments=['--name', name, '--global_params', global_params],
            file_outputs={"sample": component_helper.generate_output_path("sample")}
        )


class RawSample2ModelSample(BaseComponent):
    """
    lookalike样本ID特征编码组件
    """
    OUTPUT_KEY_MODEL_SAMPLE = 'model_sample_table_name'

    def __init__(self, name, global_params, raw_sample_table_name, tag='latest'):
        super().__init__(
            name=name,
            image=f'{global_constant.AI_PLATFORM_IMAGE_REPO_V2}/sample-raw_sample_to_sample',
            arguments=['--name', name, '--global_params', global_params,
                       '--raw_sample_table_name', raw_sample_table_name,
                       ],
            tag=tag,
            file_outputs={
                self.OUTPUT_KEY_MODEL_SAMPLE: component_helper.generate_output_path(
                    self.OUTPUT_KEY_MODEL_SAMPLE)
            }
        )
        
class SampleSelectionDixiaohu(BaseComponent):
    """
    数据源-读取表组件
    """

    def __init__(self, name, global_params, tag='latest'):
        super().__init__(
            name=name,
            image=f'{global_constant.AI_PLATFORM_IMAGE_REPO_V2}/sample-sample_selection_dixiaohu',
            arguments=['--name', name, '--global_params', global_params],
            tag=tag,
            file_outputs={"sample_table_name": component_helper.generate_output_path("sample_table_name")}
        )