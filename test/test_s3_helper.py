import digitforce.aip.common.utils.s3_helper as s3_helper

client = s3_helper.S3Client.get()
# 获取所有桶
print(client.list_buckets())
# 获取桶内文件
print(client.list_objects('warehouse', max_keys=3))
# 上传文件
client.upload_file('warehouse', 'test_s3_client.py', 'test_s3_client.py')
# 获取置顶路径的对象
print(client.list_objects('warehouse', '/wtg/test/'))
# 测试上传模型训练指标文件
import digitforce.aip.common.utils.model_helper as model_helper
model_helper.report_model_metrics({'a': 1, 'b': 2}, 'test_model_id')
