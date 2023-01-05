# coding: utf-8
import kfp.dsl as dsl
import digitforce.aip.common.utils.component_helper as component_helper

output_name = 'sample'


class SampleSelectionLookalike(dsl.ContainerOp):
    """
    数据源-读取表组件
    """
    def __init__(self, name, global_params):
        super(SampleSelectionLookalike, self).__init__(
            name=name,
            image=f'digit-force-docker.pkg.coding.net/ai-platform/ai-components/'
                  f'sample-sample_selection_lookalike',
            command=['python', 'main.py'],
            arguments=['--name', name, '--global_params', global_params],
            file_outputs={output_name: component_helper.generate_output_path(output_name)}
        )

class SampleSelectionLiushi(dsl.ContainerOp):
    """
    数据源-读取表组件
    """
    def __init__(self, name, global_params):
        super(SampleSelectionLookalike, self).__init__(
            name=name,
            image=f'digit-force-docker.pkg.coding.net/ai-platform/ai-components/sample-sample_selection_liushi',
            command=['python', 'main.py'],
            arguments=['--name', name, '--global_params', global_params],
            file_outputs={output_name: component_helper.generate_output_path(output_name)}
        )


class RawSample2ModelSample(dsl.ContainerOp):
    OUTPUT_KEY_MODEL_SAMPLE = 'model_sample_table_name'

    def __init__(self, name, global_params, raw_sample_table_name):
        super(RawSample2ModelSample, self).__init__(
            name=name,
            image=f'digit-force-docker.pkg.coding.net/ai-platform/ai-components'
                  f'/sample-raw_sample_to_sample',
            command=['python', 'main.py'],
            arguments=['--name', name, '--global_params', global_params,
                       '--raw_sample_table_name', raw_sample_table_name,
                       ],
            file_outputs={
                self.OUTPUT_KEY_MODEL_SAMPLE: component_helper.generate_output_path(
                    self.OUTPUT_KEY_MODEL_SAMPLE)
            }
        )
