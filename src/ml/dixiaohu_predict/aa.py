import joblib
import digitforce.aip.common.utils.hdfs_helper as hdfs_helper
import pickle


def read_hdfs_path(local_path, hdfs_path, client):
    if client.exists(hdfs_path):
        client.copy_to_local(hdfs_path, local_path)


def pkl_load(filename):
    pkl_file = open(filename, "rb")
    file = pickle.load(pkl_file)
    pkl_file.close()
    return file


hdfs_client = hdfs_helper.HdfsClient()
# local_file_path = "/data/zyf/dixiaohu.model"
# model_hdfs_path = "/user/ai/aip/zq/dixiaohu/model/latest.model"

# local_file_path = "/data/zyf/dixiaohu.pk"
# # model_hdfs_path = "/user/ai/aip/zq/dixiaohu/model/latest_model.pk"
# model_hdfs_path = "/user/ai/aip/222/model/model.pk"

local_file_path = "/data/zyf/model.pickle.dat"
model_hdfs_path = "/user/ai/aip/zq/dixiaohu/model/latest_model.pickle.dat"
print("local_file_path--------------", local_file_path)
print("model_hdfs_path--------------", model_hdfs_path)
read_hdfs_path(local_file_path, model_hdfs_path, hdfs_client)
model = joblib.load(local_file_path)
# pkl_load(local_file_path)
