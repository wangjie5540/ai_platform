from kfp import dsl
from kubernetes.client import V1EnvFromSource, V1ConfigMapEnvSource, V1EnvVar
from kubernetes.client.models import V1VolumeMount

NFS_DATA_VOLUME = dsl.PipelineVolume(pvc="ai-platform-pvc")
DATA_VOLUME_MOUNT = V1VolumeMount(mount_path="/data", name="data_volume")
aip_env = V1ConfigMapEnvSource("aip-env")
special_env = V1EnvFromSource(config_map_ref=aip_env)


class BaseComponent(dsl.ContainerOp):

    def init_aip_container(self):
        # add pv to container
        self.add_pvolumes({"/data": NFS_DATA_VOLUME})
        # add config map to env
        self.container.add_env_from(special_env)

    def add_env_variable(self, name, value):
        env_var = V1EnvVar(name, value)
        self.container.add_env_variable(env_var)
