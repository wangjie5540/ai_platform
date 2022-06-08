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
from Automatic_predict import predict_main
from common_logging_config import *
# logging.basicConfig(level=logging.DEBUG,
#                     format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',  
#                     datefmt='%a, %d %b %Y %H:%M:%S',  
#                     filename='test.log',  
#                     filemode='w')



path = os.getcwd()
setup_logging(os.path.join(path,'Automatic_predict_main.log'), os.path.join(path, 'Error.log'))

class GaoQianExtract():
    def __init__(self):
        print('start')
        if os.path.exists('input'):
            pass
        else:
            os.makedirs('input')
        if os.path.exists('output'):
            pass
        else:
            os.makedirs('output')
        if os.path.exists('tmp'):
            pass
        else:
            os.makedirs('tmp')
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
                        logging.info('--------Parameters of this task:-----------')
                        logging.info(data)
                        data_dict = json.loads(data)

                        logging.info("Start predicting")
                        
                        csv_filepath = predict_main(data_dict, path)
                        
                        logging.info("Return the crowd result to label platform.")
                        
                        a=self.response_plat(data_dict['crowdid'], csv_filepath)
#                         logging.info('Is successful? ：'+a)
                        
#                         if a == 'success':
                        os.remove(file_path)
                else:
                    time.sleep(30)
            except:
#                 print(traceback.format_exc())
                logging.error('Raise errors!',exc_info=1)
      
        
    def response_plat(self, crowdid, filepath):
        try:
            # 回调标签平台接口
            url = 'http://10.100.0.101:8658/api/labelx/open/algorithm/callback'


            # 文件服务器地址
#             url_head = 'http://pro.file-storage-service.jszt-003.gw.yonghui.cn/download/'
            # url_doc = url_head + filepath
            url_doc = filepath
            headers = {'Content-Type': 'application/json'}
            data = {"flag": 0, "businessId": crowdid, "fileDownloadUrl": url_doc}
            if filepath != "-1":
                data["flag"]=1
                logging.info(data)
                response = requests.post(url, data=json.dumps(data), headers=headers, timeout=60)
                str_respose = response.text
                logging.info(str_respose)
                json_text = json.loads(str_respose)
                if json_text['code'] == "00000":
                    return "success"
                else:
                    return ""
            else:
                response = requests.post(url, data=json.dumps(data), headers=headers, timeout=60)
                str_respose = response.text
                logging.info(str_response)
                json_text = json.loads(str_respose)
                
                if json_text['code'] == "00000":
                    return "success"
                else:
                    return ""               
        except:
#             print(traceback.format_exc())
            logging.error('Raise errors when response to platform',exc_info=1)
            return ""
        


if __name__ == '__main__':
   
    bpwe = GaoQianExtract()
    vio_word=bpwe.json_param_get_output()
