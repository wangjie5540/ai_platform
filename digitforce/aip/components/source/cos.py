from digitforce.aip.components import BaseComponent
import digitforce.aip.common.constants.global_constant as global_constant
import digitforce.aip.common.utils.component_helper as component_helper


class Cos(BaseComponent):
    OUTPUT_1 = 'table_name'

    def __init__(self, name: str, global_params: str, tag='latest'):
        """
        :param name: 名称
        :param global_params: pipeline全局参数
        """
        super().__init__(
            name,
            image=f"{global_constant.AI_PLATFORM_IMAGE_REPO}/source-cos",
            arguments=['--name', name, '--global_params', global_params],
            file_outputs={
                Cos.OUTPUT_1: component_helper.generate_output_path(Cos.OUTPUT_1)
            },
            tag=tag
        )
