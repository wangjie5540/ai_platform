import os

import digitforce.aip.common.utils.s3_helper as s3_helper
import digitforce.aip.common.utils.config_helper as config_helper
import json
import tempfile

client = s3_helper.S3Client.get()
base_prefix = 'model'


def report_model_metrics(metrics: dict, train_id):
    """
    上报模型评估指标
    :param metrics: 指标
    :param train_id: 模型训练id
    :return:
    """
    # 创建临时文件
    with tempfile.NamedTemporaryFile(delete=True) as f:
        # 使用临时文件
        f.write(json.dumps(metrics).encode('utf-8'))
        f.flush()
        # 目标文件地址
        target_file_path = os.path.join(base_prefix, train_id, 'metrics.json')
        client.upload_file(bucket_name=config_helper.get_module_config('s3')['bucket'], local_file_path=f.name,
                           output_file_name=target_file_path)


def get_model_metrics(train_id):
    """
    打印模型评估指标
    :param train_id: 模型训练id
    :return:
    """
    # 目标文件地址
    target_file_path = os.path.join(base_prefix, train_id, 'metrics.json')
    metrics_str = client.get_object_str(bucket_name=config_helper.get_module_config('s3')['bucket'], key=target_file_path)
    return json.loads(metrics_str)


def upload_data(local_data_path, train_id, target_file_name):
    """
    保存数据
    :param local_data_path: 本地数据路径
    :param train_id: 模型训练id
    :param target_file_name: 目标文件名
    :return:
    """
    target_file_path = os.path.join(base_prefix, train_id, target_file_name)
    client.upload_file(bucket_name=config_helper.get_module_config('s3')['bucket'], local_file_path=local_data_path,
                       output_file_name=target_file_path)


def download_data(train_id, target_file_name):
    """
    下载数据
    :param train_id: 模型训练id
    :param target_file_name: 目标文件名
    :return:
    """
    target_file_path = os.path.join(base_prefix, train_id, target_file_name)
    client.download_file(bucket_name=config_helper.get_module_config('s3')['bucket'], key=target_file_path,
                         local_file_path=target_file_name)
