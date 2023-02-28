# coding: utf-8
import kfp.dsl as dsl

import digitforce.aip.common.utils.component_helper as component_helper
from digitforce.aip.common.constants.global_constant import ENV
from digitforce.aip.components import BaseComponent
import digitforce.aip.common.constants.global_constant as global_constant

output_name = 'sample'


class SampleSelectionLookalike(dsl.ContainerOp):
    """
    数据源-读取表组件
    """

    def __init__(self, name, global_params, tag=ENV):
        super(SampleSelectionLookalike, self).__init__(
            name=name,
            image=f'digit-force-docker.pkg.coding.net/ai-platform/ai-components/'
                  f'sample-sample_selection_lookalike:{tag}',
            command=['python', 'main.py'],
            arguments=['--name', name, '--global_params', global_params],
            file_outputs={output_name: component_helper.generate_output_path(output_name)}
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


class RawSample2ModelSample(dsl.ContainerOp):
    OUTPUT_KEY_MODEL_SAMPLE = 'model_sample_table_name'

    def __init__(self, name, global_params, raw_sample_table_name, tag=ENV):
        super(RawSample2ModelSample, self).__init__(
            name=name,
            image=f'digit-force-docker.pkg.coding.net/ai-platform/ai-components'
                  f'/sample-raw_sample_to_sample:{tag}',
            command=['python', 'main.py'],
            arguments=['--name', name, '--global_params', global_params,
                       '--raw_sample_table_name', raw_sample_table_name,
                       ],
            file_outputs={
                self.OUTPUT_KEY_MODEL_SAMPLE: component_helper.generate_output_path(
                    self.OUTPUT_KEY_MODEL_SAMPLE)
            }
        )
