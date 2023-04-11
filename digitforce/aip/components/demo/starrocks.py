from digitforce.aip.components import BaseComponent
import digitforce.aip.common.constants.global_constant as global_constant


class DemoStarrocks(BaseComponent):
    def __init__(self, name: str, global_params: str, tag='latest'):
        super().__init__(
            name=name,
            image=f'{global_constant.AI_PLATFORM_IMAGE_REPO_V2}/demo-starrocks',
            arguments=['--name', name, '--global_params', global_params],
            tag=tag,
        )
