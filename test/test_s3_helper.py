import digitforce.aip.common.utils.s3_helper as s3_helper

client = s3_helper.S3Client.get()
# 获取所有桶
print(client.list_buckets())
# 获取桶内文件
print(client.list_objects('algorithm-1308011215', max_keys=3, prefix="dev/"))
# # 上传文件
client.upload_file('algorithm-1308011215', 'test_s3_helper.py', 'test_s3_helper.py')
object_str = client.get_object_str('algorithm-1308011215', 'test_s3_helper.py')
print(object_str)
# # 测试上传模型训练指标文件
# import digitforce.aip.common.utils.model_helper as model_helper
# model_helper.report_model_metrics({'a': 1, 'b': 2}, 'test_model_id')
