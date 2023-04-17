import digitforce.aip.common.utils.model_helper as model_helper

model_helper.report_model_metrics({'a': 1, 'b': 2}, 'test_model_id')
print(model_helper.get_model_metrics('test_model_id'))

# model_helper.upload_data('test_s3_client.py', 'test_model_id', 'test_s3_client.py')
# model_helper.download_data('test_model_id', 'metrics.json')
