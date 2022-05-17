from kfp import dsl
from kubernetes.client.models import V1VolumeMount

NFS_DATA_VOLUME = dsl.PipelineVolume(pvc="ai-platform-pvc")
DATA_VOLUME_MOUNT = V1VolumeMount(mount_path="/data", name="data_volume")


def mount_data_pv(func):
    def wrapper(*args, **kwargs):
        op = func(*args, **kwargs)
        op.add_pvolumes({"/data": NFS_DATA_VOLUME})
        return op

    return wrapper
