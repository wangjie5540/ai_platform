import digitforce.aip.common.utils.s3_helper as s3_helper
import tempfile
import json
import os
import digitforce.aip.common.utils.config_helper as config_helper

client = s3_helper.S3Client.get()


def report_ale(data, instance_id):
    """
    上报ale
    :param data: ale数据
    :param instance_id: 实例id
    :return:
    """
    # 创建临时文件
    with tempfile.NamedTemporaryFile(delete=True) as f:
        # 使用临时文件
        f.write(json.dumps(data).encode('utf-8'))
        f.flush()
        # 目标文件地
        target_file_path = os.path.join('predict', instance_id, 'ale.json')
        client.upload_file(bucket_name=config_helper.get_module_config('s3')['bucket'], local_file_path=f.name,
                           output_file_name=target_file_path)


def get_ale(instance_id):
    """
    获取ale
    :param instance_id: 实例id
    :return:
    """
    # 目标文件地址
    target_file_path = os.path.join('predict', instance_id, 'ale.json')
    ale_str = client.get_object_str(bucket_name=config_helper.get_module_config('s3')['bucket'], key=target_file_path)
    return json.loads(ale_str)
