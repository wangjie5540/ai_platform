# coding: utf-8
import kfp.dsl as dsl
import digitforce.aip.common.constants.global_constant as global_constant

output_name = 'out'


class SampleSelectionLookalike(dsl.ContainerOp):
    """
    数据源-读取表组件
    """
    def __init__(self, name, global_params, data_input):
        super(SampleSelectionLookalike, self).__init__(
            name=name,
            image=f'digit-force-docker.pkg.coding.net/ai-platform/ai-components/sample-sample_selection_lookalike',
            command=['python', 'main.py'],
            arguments=['--name', name, '--global_params', global_params, '--data_input', data_input],
            file_outputs={output_name: global_constant.JSON_OUTPUT_PATH}
        )
