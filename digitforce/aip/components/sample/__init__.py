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
            image=f'digit-force-docker.pkg.coding.net/ai-platform/ai-components/sample-sample_selection_lookalike',
            command=['python', 'main.py'],
            arguments=['--name', name, '--global_params', global_params],
            file_outputs={output_name: component_helper.generate_output_path(output_name)}
        )
