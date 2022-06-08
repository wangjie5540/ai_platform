#!/usr/bin/env python3
# coding: utf-8


import re
import tornado.web
from tornado import gen
import tornado.ioloop
from base_handler import JsonHandler, HtmlHandler
from base import JdTraceback, ISPY3, ProcessInfo, BaseObject
# from repeat_info_detect import ViolateWordExtract
import threading
from threading import Lock
import socket
import os
import time
try:
    import ujson as json
except:
    import json

import traceback
from io import BytesIO
import datetime
import requests
import sys
import time
from collections import Counter
# from pyspark.sql import SparkSession
# from pyspark import SparkFiles
import pandas as pd
from collections import defaultdict
import numpy as np
import logging
    

class baseMethod(object):
    
    @classmethod
    @gen.coroutine
    def process_final_whole(cls, json_param):
        state_code, ret = yield cls.do_detect(json_param)
        if state_code == 200:
            result = {'code': 200, 'msg': ret, 'data': None}
        else:
#             state_flag = True
            result = {'code': 500, 'msg': ret, 'data': None}
        raise gen.Return((200, json.dumps(result, sort_keys=True, indent=4, ensure_ascii=False)))   
        
    
    @classmethod
    @gen.coroutine
    def do_detect(cls, json_param):
        '''
        异步处理函数 供 api handler 异步调用
        :param intxt:
        :return:
        '''
        try:
            crowdid = json_param.get('businessId', 0)
            modelFileUrl = json_param.get('modelFileUrl', '')
            where_sql = json_param.get('whereSql', '')
            targetLimit = json_param.get('targetLimit', {})

            if not crowdid:
                raise Exception('请输入bussinessId')
            if not modelFileUrl:
                raise Exception('请输入modelFileUrl')
            if not where_sql:
                raise Exception('请输入whereSql')
            if not targetLimit:
                raise Exception('请输入targetLimit')

            code, result = yield cls.dealMethod(crowdid, modelFileUrl, where_sql, targetLimit)
            if code != 200:
                raise Exception(result)
        except:
            msg = JdTraceback()
#             msg = str(type(spuid))+ msg
            raise gen.Return((500, msg))
        raise gen.Return((200, result))

        
    @classmethod
    @gen.coroutine
    def dealMethod(cls, crowdid, modelFileUrl, where_sql, targetLimit):
        try:
            input_param = {}
            input_param['crowdid'] = crowdid
            input_param['modelFileUrl'] = modelFileUrl
            input_param['where'] = where_sql
            input_param['targetLimit'] = targetLimit
            input_param_str = json.dumps(input_param, ensure_ascii=False)
            
            fname=str(crowdid)+r".txt"
            if os.path.exists('input'):
                with open(os.path.join("input",fname), 'w') as f:
                    f.write(input_param_str)
            else:
                os.makedirs('input')
                with open(os.path.join("input",fname), 'w') as f:
                    f.write(input_param_str)
            
            res = "上传参数成功"

        except:
            msg = JdTraceback()
#             msg = str(type(spuid))+ msg
            raise gen.Return((500, msg))
        raise gen.Return((200, res))


if __name__ == '__main__':
    # print("...online ...")
    baseClass = baseMethod()

    res=baseClass.dealMethod(21314354332)
    #
    print(res)




