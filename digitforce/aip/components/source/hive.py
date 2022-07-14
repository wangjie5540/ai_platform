from digitforce.aip.common.constants.global_constant import AI_PLATFORM_IMAGE_REPO
from digitforce.aip.components.op_decorator import *


@mount_data_pv
def query_to_csv_op(query_sql, result_file, image_tag="latest"):
    """
    将hive查询结果保存到本地csv文件
    :param query_sql: sql
    :param result_file: csv路径
    :param image_tag:
    :return:
    """
    op = dsl.ContainerOp(name="query_to_csv",
                         image=f"{AI_PLATFORM_IMAGE_REPO}"
                               "/src-source-hive-to_csv" + f":{image_tag}",
                         command="python",
                         arguments=["main.py", query_sql, result_file]
                         )
    return op
