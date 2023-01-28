from digitforce.aip.components import BaseComponent


class Print(BaseComponent):
    def __init__(self, name: str):
        super().__init__(name, image=f'digit-force-docker.pkg.coding.net/ai-platform/ai-components/demo-print')
