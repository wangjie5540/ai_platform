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
            taskid = json_param.get('taskId', '')
            category = json_param.get('forecastCategory', [])
            userData = json_param.get('userData', {})
            trafficData = json_param.get('trafficData', {})
            orderData = json_param.get('orderData', {})
            goodsData = json_param.get('goodsData',{})
            trainingScope = json_param.get('trainingScope', '')
            forecastPeriod = json_param.get('forecastPeriod', '')
            eventCode = json_param.get('eventCode', {})
            
            if not taskid:
                raise Exception('请输入taskId')
            if not category:
                raise Exception('请输入category')
            if not userData:
                raise Exception('请输入userData')
            if not trafficData:
                raise Exception('请输入trafficData')
            if not orderData:
                raise Exception('请输入orderData')
            if not goodsData:
                raise Exception('请输入goodsData')
            if not trainingScope:
                raise Exception('请输入traningScope')
            if not forecastPeriod:
                raise Exception('请输入forecastPeriod')
            if not eventCode:
                raise Exception('请输入eventCode')

            code, result = yield cls.dealMethod(taskid, category, userData, trafficData, orderData, goodsData, trainingScope, forecastPeriod, eventCode)
            if code != 200:
                raise Exception(result)
        except:
            msg = JdTraceback()
#             msg = str(type(spuid))+ msg
            raise gen.Return((500, msg))
        raise gen.Return((200, result))

        
    @classmethod
    @gen.coroutine
    def dealMethod(cls, taskid, category, userData, trafficData, orderData, goodsData, trainingScope, forecastPeriod, eventCode):
        try:
            input_param = {}
            input_param['taskid'] = taskid
            input_param['category'] = category
            input_param['userData'] = userData
            input_param['trafficData'] = trafficData
            input_param['orderData'] = orderData
            input_param['goodsData'] = goodsData
            input_param['trainingScope'] = trainingScope
            input_param['forecastPeriod'] = forecastPeriod
            input_param['eventCode'] = eventCode
            
            input_param_str = json.dumps(input_param, ensure_ascii=False)
            
            fname=str(taskid)+r".txt"
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




