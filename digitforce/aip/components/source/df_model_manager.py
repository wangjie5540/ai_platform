from digitforce.aip.common.constants.global_constant import AI_PLATFORM_IMAGE_REPO
from digitforce.aip.components.op_decorator import *


@mount_data_pv
def save_one_model_to_model_manage_system(solution_id, instance_id, local_model_path, target_hdfs_path, model_name,
                                          image_tag="latest"):
    """
        将模型文件托管到模型管理系统
                将一批模型文件托管到模型管理系统
        :param solution_id: solution_id
        :param instance_id: instance_id
        :param local_model_path: 模型文件
        :param target_hdfs_path: 模型文件保存的hdfs
        :param model_name: 模型名称
        :param image_tag: 镜像版本
    """
    _type = "one_file"
    op = dsl.ContainerOp(name="save_one_model_to_model_manage_system",
                         image=f"{AI_PLATFORM_IMAGE_REPO}"
                               "/src-source-df_model_manage" + f":{image_tag}",
                         command="bash",
                         arguments=["/data/entrypoint.sh", "python", "main.py", _type, solution_id, instance_id,
                                    local_model_path,
                                    target_hdfs_path,
                                    model_name]
                         )
    return op


@mount_data_pv
def save_models_to_model_manage_system(solution_id, instance_id, local_model_dir_path, target_hdfs_dir, model_name,
                                       image_tag="latest"):
    """
        将一批模型文件托管到模型管理系统
        :param solution_id: solution_id
        :param instance_id: instance_id
        :param local_model_dir_path: 模型文件目录
        :param target_hdfs_dir: 模型文件保存的hdfs目录
        :param model_name: 模型名称
        :param image_tag: 镜像版本
    """
    _type = "dir"
    op = dsl.ContainerOp(name="save_models_to_model_manage_system",
                         image=f"{AI_PLATFORM_IMAGE_REPO}"
                               "/src-source-df_model_manage" + f":{image_tag}",
                         command="bash",
                         arguments=["/data/entrypoint.sh", "python", "main.py", _type, solution_id, instance_id,
                                    local_model_dir_path, target_hdfs_dir,
                                    model_name]
                         )
    return op
