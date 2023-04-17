import digitforce.aip.common.utils.hdfs_helper as hdfs_helper

client = hdfs_helper.hdfs_client
print(client.list_dir('/'))

client.copy_to_local("/user/ai/aip/predict/1646809101662289921/ale.json", "ale.json")

# client.copy_from_local('wtg_test_replace.csv', '/tmp/wtg_test_replace.csv')