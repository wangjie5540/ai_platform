from digitforce.aip.common.constants.global_constant import AI_PLATFORM_IMAGE_REPO
from digitforce.aip.components.op_decorator import *


def item_statistics(statistics_table, table_name=None, event_code_column_name=None, duration=None,
                    partition_name=None, date_str=None, item_column=None, image_tag='latest'):
    arguments = ['main.py', statistics_table]
    if table_name:
        arguments.append('--event_table')
        arguments.append(table_name)

    if event_code_column_name:
        arguments.append('--event_code_column_name')
        arguments.append(event_code_column_name)

    if duration:
        arguments.append('--duration')
        arguments.append(duration)

    if partition_name:
        arguments.append('--partition_name')
        arguments.append(partition_name)

    if date_str:
        arguments.append('--date_str')
        arguments.append(date_str)

    if item_column:
        arguments.append('--item_column')
        arguments.append(item_column)

    return dsl.ContainerOp(
        name='item_statistics',
        image=f"{AI_PLATFORM_IMAGE_REPO}"
              f"/src-data_preprocess-item_statistics" + f":{image_tag}",
        command='python',
        arguments=arguments
    )
