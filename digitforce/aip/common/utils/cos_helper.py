# -*- coding=utf-8
from qcloud_cos import CosConfig
from qcloud_cos import CosS3Client
import digitforce.aip.common.utils.config_helper as config_helper

cos_confg = config_helper.get_module_config('cos')
# 替换为用户的 region，已创建桶归属的region可以在控制台查看，https://console.cloud.tencent.com/cos5/bucket
region = 'ap-beijing'
# COS支持的所有region列表参见https://cloud.tencent.com/document/product/436/6224
# 如果使用永久密钥不需要填入token，如果使用临时密钥需要填入，临时密钥生成和使用指引参见https://cloud.tencent.com/document/product/436/14048
token = None
# 指定使用 http/https 协议来访问 COS，默认为 https，可不填
scheme = 'https'


def upload_file(local_file_path, output_file_name):
    config = CosConfig(
        Region=region, SecretId=cos_confg['secret_id'], SecretKey=cos_confg['secret_key'], Token=token, Scheme=scheme)
    client = CosS3Client(config)
    client.upload_file(
        Bucket=cos_confg['bucket'],
        LocalFilePath=local_file_path,
        Key=output_file_name,
        ACL='public-read',
    )
    return f"{cos_confg['base_url']}/{output_file_name}"
