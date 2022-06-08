#!/usr/bin/env python3
# coding: utf-8
import os
import sys
import threading
from tornado.web import Application
from tornado import ioloop, httpserver
from check_handler import CheckHandlerWhole
import asyncio

# # import nest_asyncio
# # nest_asyncio.apply()


sys.path.append(os.path.realpath(os.path.dirname('__file__')))


def http_api(port = 8888):
    
    handlers = [
        (r'/online-fugou-train', CheckHandlerWhole)
    ]

    settings = dict(
        static_path = os.path.join(os.path.dirname('__file__'), "static"),
        debug = False
    )

    api_instance = Application(handlers, compress_response=True, **settings)
    api_server = httpserver.HTTPServer(api_instance)
    

    api_server.bind(port, "")
    api_server.start(num_processes=2)  # 如果是 0 tornado将按照cpu核数来fork进程
#     api_server.listen(port) # 本地测试 单线程

    ioloop.IOLoop.instance().start()


if __name__ == "__main__":
#     import argparse

#     parser = argparse.ArgumentParser()

#     parser.add_argument("port", help=r"listen port", type=int)

#     args = parser.parse_args()

    http_api()


