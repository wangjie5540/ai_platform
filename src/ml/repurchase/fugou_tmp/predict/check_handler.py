#!/usr/bin/env python3
# coding: utf-8


import sys
from base_handler import JsonHandler, HtmlHandler
from base import JdTraceback, ProcessInfo, ISPY3, BaseObject
from check_api import *
try:
    import ujson as json
except:
    import json

if not ISPY3:
    BaseObject.loger.error("Can only be run under python3.5+")
    sys.exit(-1)


class CheckHandlerWhole(JsonHandler):
    
    
    @gen.coroutine
    def get(self):
        self.finish('请用post方法访问')
    
    @gen.coroutine
    def post(self):
        json_param = json.loads(self.request.body)
        (status, rsp) = yield baseMethod.process_final_whole(json_param)
        self.finish(rsp)
        



if __name__ == "__main__":
    pass

