from digitforce.aip.components import BaseComponent


class Print(BaseComponent):
    def __init__(self, name: str, param, tag='latest'):
        super().__init__(
            name=name,
            image=f'digit-force-docker.pkg.coding.net/ai-platform/ai-components/demo-print',
            arguments=['--param', param],
            tag=tag,
        )
