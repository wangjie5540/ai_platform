#!/usr/bin/env python3
# coding: utf-8


import re
import traceback
from io import BytesIO
import datetime
import requests
import sys
import time
import json
from collections import Counter
import pandas as pd
from collections import defaultdict
import numpy as np
import os
import logging
import shutil
from Automatic_train_bayesopt import train
from utils import *
from common_logging_config import *
# logging.basicConfig(level=logging.DEBUG,
#                     format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',  
#                     datefmt='%a, %d %b %Y %H:%M:%S',  
#                     filename='test.log',  
#                     filemode='w')



path = os.getcwd()
setup_logging(os.path.join(path,'Automatic_train_main.log'), os.path.join(path, 'Error.log'))

class GaoQianExtract():
    def __init__(self):
        print('start')
        if os.path.exists('input'):
            pass
        else:
            os.makedirs('input')
        if os.path.exists('model'):
            pass
        else:
            os.makedirs('model')
        if os.path.exists('done'):
            pass
        else:
            os.makedirs('done')
                    
        
    def json_param_get_output(self):
        while True:
            try:
                f = os.path.join(path,"input")
                ff = os.path.join(f,'.ipynb_checkpoints')

                if os.path.exists(ff):
                    os.rmdir(ff)  
                else:
                    pass

                if os.listdir(f):
                    file_list = os.listdir(f)
                    for x in file_list:
                        file_path = os.path.join(f, x)
#                         print(file_path)
                        with open(file_path, 'r') as file:
                            data = file.readline()
#                         print(data)
                        logging.info('任务参数:')
                        logging.info(data)
                        data_dict = json.loads(data)
                        
                          
#                         print("上传文件")
                        logging.info('开始训练')
                        res = train(data_dict, path)
                        if res['fileDownloadUrl'] != '-1' and res['fileDownloadUrl']:
                            params_target_file_path = os.path.join('hdfs:///usr/algorithm/cd/fugou/model', str(data_dict['taskid'])+'.txt')
                            upload_flag = upload_hdfs(file_path,params_target_file_path)
                            
#                         logging.info('回调平台，返回模型结果')
                        a=self.response_plat(res)
                        logging.info(a)
                        
                        
#                         dst = os.path.join(path,"done", x)
#                         shutil.move(file_path, dst)
#                         if a == 'success':
                        os.remove(file_path)
                else:
                    time.sleep(30)
            except:
#                 print(traceback.format_exc())
                logging.info(traceback.format_exc())
      
        
    def response_plat(self, res):
        try:
            # 回调标签平台接口
            url = 'http://10.100.0.101:8658/api/labelx/algorithmModelVersions/callBackTrainingModel'
#             url = 'http://batchx.digitforce.com/api/labelx/algorithmModelVersions/callBackTrainingModel'

            # 文件服务器地址
#             url_head = 'http://pro.file-storage-service.jszt-003.gw.yonghui.cn/download/'
            url_doc = res['fileDownloadUrl']

            headers = {'Content-Type': 'application/json'}
            data = {"flag": 0, "taskId": res['taskId'], "auc": res['auc'], "fileSize": res['fileSize'],
                    "generateTime": res['generateTime'], "elapsed": res['elapsed'],
                     "fileDownloadUrl": url_doc, "result": ""}
            if res['fileDownloadUrl'] and res['fileDownloadUrl'] != "-1":
                data["flag"]=1
                data["result"] = 'Train finished, and model have saved!'
#                 print(data)                
            else:
                data['result'] = 'Trainning error!'
            logging.info('回调返回的参数:')
            logging.info(data)
            try:
                requests.adapters.DEFAULT_RETRIES=3
                response = requests.post(url, data=json.dumps(data), headers=headers, timeout=60)
                str_respose = response.text
                logging.info(str_respose)
                json_text = json.loads(str_respose)
                if json_text['code'] == '00000':
                    return "success"
                else:
                    return "" 
            except Exception as e:
                logging.error(e)
                      
        except:
#             print(traceback.format_exc())
            logging.info(traceback.format_exc())
            return ""
        


if __name__ == '__main__':
   
    bpwe = GaoQianExtract()
    vio_word=bpwe.json_param_get_output()
