# coding: utf-8
import kfp.dsl as dsl
import digitforce.aip.common.utils.component_helper as component_helper


class BaseComponent(dsl.ContainerOp):
    """
    组件的基类，所有组件都需要继承该类
    """

    def __init__(self, name: str, image: str, arguments: list = None, file_outputs: dict = None):
        """
        :param name: 名称
        :param image: 镜像地址
        :param arguments: 参数列表
        :param file_outputs: 输出
        """
        environment = component_helper.get_environment()
        super(BaseComponent, self).__init__(
            name=name,
            image=f'{image}:{environment}',
            command=['python', 'main.py'],
            arguments=arguments,
            file_outputs=file_outputs,
            pvolumes={
                '/usr/local/etc/': dsl.PipelineVolume(pvc='aip-etc-pvc'),
                '/root/.kube': dsl.PipelineVolume(pvc='aip-kube-pvc'),
            },
        )
