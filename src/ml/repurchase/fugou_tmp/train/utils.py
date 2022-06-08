import requests
import json
import traceback
import pyhdfs

def upload(filepath):
    try:
        url = 'http://jszt-dev.file-storage-service.jszt-003.devgw.yonghui.cn/upload'
        data = None
        with open(filepath, 'rb') as file:
            files = { 'file' : file}
            headers = {'Api-Key': 'AxRJJ5fy.nGGGZeH1wRT2RWH8BMoHHngmRZ620b2d'}
            response = requests.post(url, data, files = files, headers=headers, timeout=20)
            str_respose = response.text
            json_text = json.loads(str_respose)
            if json_text['msg'] == 'ok':
                return json_text['path']
            else:
                return ""
    except:
        print(traceback.format_exc())
#             logging.info(traceback.format_exc())
        return ""


def upload_hdfs(filepath, target_file_path):
    try:
        cli = pyhdfs.HdfsClient(hosts='10.100.0.82:4008', user_name='root') 
        if cli.exists(target_file_path):
            cli.delete(target_file_path)
        cli.copy_from_local(filepath, target_file_path)
        return target_file_path
    except:
        print(traceback.format_exc())
        #             logging.info(traceback.format_exc())
        return ""

# def download_hdfs(self, current_path, ModelFileUrl):
#     try:
#         cli = pyhdfs.HdfsClient(hosts="bigdata-server-08:9870")
#         file_path = os.path.join(current_path, businessId + '_seed.csv')
#         cli.copy_to_local(seedCrowdFileUrl, file_path)
#         return file_path
#     except:
#         print(traceback.format_exc())
#         #             logging.info(traceback.format_exc())
#         return ""