from digitforce.aip.components import BaseComponent
import digitforce.aip.common.constants.global_constant as global_constant
import digitforce.aip.common.utils.component_helper as component_helper


class Cos(BaseComponent):
    OUTPUT_1 = 'table_name'

    def __init__(self, name: str, url: str, columns: str, tag='latest'):
        """
        :param name: 名称
        :param url: 目标文件地址(例如：https://bucket-name/path/to/file)
        :param columns: 列名，使用逗号分隔
        """
        super().__init__(
            name,
            image=f"{global_constant.AI_PLATFORM_IMAGE_REPO}/source-cos",
            arguments=['--url', url, '--columns', columns],
            file_outputs={
                Cos.OUTPUT_1: component_helper.generate_output_path(Cos.OUTPUT_1)
            },
            tag=tag
        )
