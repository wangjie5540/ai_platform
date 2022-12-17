import kfp.dsl as dsl
import digitforce.aip.common.constants.global_constant as global_constant


class ReadTable(dsl.ContainerOp):
    """
    数据源-读取表组件
    """
    def __init__(self, global_params, name='source-read_table'):
        super(ReadTable, self).__init__(
            name=name,
            image='digit-force-docker.pkg.coding.net/ai-platform/ai-components/source-read_table',
            command=['python', 'main.py'],
            arguments=['--global_params', global_params, '--name', name],
            file_outputs={"out": global_constant.JSON_OUTPUT_PATH}
        )
