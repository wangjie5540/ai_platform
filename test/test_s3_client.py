import digitforce.aip.common.utils.s3_helper as s3_helper

client = s3_helper.S3Client.get()
print(client.list_buckets())
print(client.list_objects('warehouse', max_keys=3))
client.upload_file('warehouse', 'test_s3_client.py', 'test_s3_client.py')

print(s3_helper.S3Client().list_objects('warehouse', '/wtg/test/'))
