# coding: utf-8
import kfp.dsl as dsl
import digitforce.aip.common.constants.global_constant as global_constant
import kubernetes.client.models as models


class BaseComponent(dsl.ContainerOp):
    """
    组件的基类，所有组件都需要继承该类
    """

    def __init__(self, name: str, image: str, tag='latest', arguments: list = None, file_outputs: dict = None):
        """
        :param name: 名称
        :param image: 镜像地址
        :param arguments: 参数列表
        :param file_outputs: 输出
        """
        super(BaseComponent, self).__init__(
            name=name,
            image=f'{image}:{tag}',
            command=['python', 'main.py'],
            arguments=arguments,
            file_outputs=file_outputs,
            pvolumes={
                # TODO
                global_constant.CONFIG_MOUNT_PATH: dsl.PipelineVolume(pvc='aip-config-pvc'),
                # '/root/.kube/config': dsl.PipelineVolume(config_map=models.V1ConfigMapVolumeSource(name='kube-config')),
                # '/opt/spark-2.4.8-bin-hadoop2.7/conf': dsl.PipelineVolume(config_map=models.V1ConfigMapVolumeSource(name='hive-site')),
                # '/usr/local/etc': dsl.PipelineVolume(config_map=models.V1ConfigMapVolumeSource(name='aip-config')),
            },
        )
