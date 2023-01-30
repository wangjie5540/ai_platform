import read_cos

table_name, column_list = read_cos.read_to_table("https://algorithm-1308011215.cos.ap-beijing.myqcloud.com/aip_test_lookalike_predict.csv", "user_id")
print(table_name)
print(column_list)
# https://algorithm-1308011215.cos.ap-beijing.myqcloud.com/aip_test_lookalike_seeds.csv
# # aip.cos_4717365493815578625
# https://algorithm-1308011215.cos.ap-beijing.myqcloud.com/aip_test_lookalike_predict.csv
# # aip.cos_4717597372074430465