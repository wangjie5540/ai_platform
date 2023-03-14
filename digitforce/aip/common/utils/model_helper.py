import os

import digitforce.aip.common.utils.s3_helper as s3_helper
import digitforce.aip.common.constants.global_constant as global_constants
import json
import tempfile

client = s3_helper.S3Client.get()


def report_model_metrics(metrics: dict, fitted_id):
    """
    上报模型评估指标
    :param metrics: 指标
    :param fitted_id: 模型训练id
    :return:
    """
    # 创建临时文件
    with tempfile.NamedTemporaryFile(delete=True) as f:
        # 使用临时文件
        f.write(json.dumps(metrics).encode('utf-8'))
        f.flush()
        # 目标文件地址
        target_file_path = os.path.join(fitted_id, 'metrics.json')
        client.upload_file(bucket_name=global_constants.MODEL_PACKAGE_BUCKET, local_file_path=f.name,
                           output_file_name=target_file_path)
