from digitforce.aip.common.constants.global_constant import AI_PLATFORM_IMAGE_REPO
from digitforce.aip.components.op_decorator import *


@mount_data_pv
def rank_data_process_op(sql, output_file, info_log_file, error_log_file, config_file,
                         user_features_file, item_features_file, image_tag='latest'):
    op = dsl.ContainerOp(
        name='rank_data_process_op',
        image=f"{AI_PLATFORM_IMAGE_REPO}"
              f"/src-recommend-rank-data_process" + f":{image_tag}",
        command="python",
        arguments=["main.py", sql, output_file, info_log_file, error_log_file, config_file,
                   user_features_file, item_features_file]
    )
    op.container.set_image_pull_policy('Always')
    return op


@mount_data_pv
def lightgbm_train_op(dataset_path, model_output_file, info_log_file, error_log_file, image_tag='latest'):
    op = dsl.ContainerOp(
        name='lightgbm_train_op',
        image=f"{AI_PLATFORM_IMAGE_REPO}"
              f"/src-recommend-rank-lightgbm" + f":{image_tag}",
        command="python",
        arguments=["main.py", dataset_path, model_output_file, info_log_file, error_log_file]
    )
    op.container.set_image_pull_policy('Always')
    return op


@mount_data_pv
def jsonl_to_mongo(mode, db_name, collection, file_name, info_log_file, error_log_file,
                   cond_key=None, image_tag='latest'):
    """
    :param mode: 写入模式:
                1 insert_many
                2 replace_one: 根据cond_key查询后替换
    :param db_name 数据库
    :param collection 表
    :param file_name 源文件
    :param info_log_file
    :param error_log_file
    :param cond_key: replace_one 模式下替换文档时使用的key
    :param image_tag
    """
    args = ['main.py', '--mode', mode, '--db_name', db_name, '--collection', collection, '--file_name', file_name,
            '--info_log_file', info_log_file, '--error_log_file', error_log_file]
    if cond_key:
        args.append('--cond_key')
        args.append(cond_key)

    op = dsl.ContainerOp(
        name='jsonl_to_mongo_op',
        image=f"{AI_PLATFORM_IMAGE_REPO}"
              f"/src-recommend-rank-jsonl_to_mongo" + f":{image_tag}",
        command="python",
        arguments=args
    )

    return op

