from digitforce.aip.common.constants.global_constant import AI_PLATFORM_IMAGE_REPO
from digitforce.aip.components.op_decorator import *


def hive_sql_executor(sql, table_name, image_tag="latest"):
    """
        执行hive sql 并将结果存入 指定的表中
        :param sql: hive sql
        :param table_name: 结果表
        :param image_tag: 组件版本
        :return: op
    """
    return dsl.ContainerOp(name="hive_sql_executor",
                           image=f"{AI_PLATFORM_IMAGE_REPO}"
                                 f"/src-data_preprocess-sql-hive_sql" + f":{image_tag}",
                           command="python",
                           arguments=["main.py", sql, table_name])
