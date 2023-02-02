from digitforce.aip.components import BaseComponent


class Sleep(BaseComponent):
    def __init__(self, name: str, minutes, tag='latest'):
        super().__init__(
            name=name,
            image=f'digit-force-docker.pkg.coding.net/ai-platform/ai-components/demo-sleep',
            arguments=['--minutes', minutes],
            tag=tag,
        )
