from digitforce.aip.common.constants.global_constant import AI_PLATFORM_IMAGE_REPO
from digitforce.aip.components.op_decorator import *


@mount_data_pv
def hdfs_to_local(hdfs_file, local_path, image_tag="latest"):
    '''
    将hdfs中的文件下载到本地
    :param hdfs_file: hdfs文件路径
    :param local_path: 本地文件路径
    :param image_tag:
    :return:
    '''
    op = dsl.ContainerOp(name="hdfs_to_local'",
                         image=f"{AI_PLATFORM_IMAGE_REPO}"
                               "/src-source-hdfs" + f":{image_tag}",
                         command="python",
                         arguments=["main.py", 'hdfs_to_local', local_path, hdfs_file]
                         )
    return op


@mount_data_pv
def local_to_hdfs(hdfs_file, local_path, image_tag="latest"):
    '''
    将本地文件上传到hdfs
    :param hdfs_file: hdfs文件路径
    :param local_path: 本地文件路径
    :param image_tag:
    :return:
    '''
    op = dsl.ContainerOp(name="local_to_hdfs",
                         image=f"{AI_PLATFORM_IMAGE_REPO}"
                               "/src-source-hdfs" + f":{image_tag}",
                         command="python",
                         arguments=["main.py", 'local_to_hdfs',
                                    local_path, hdfs_file])
    return op
