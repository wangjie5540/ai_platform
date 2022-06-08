#!/usr/bin/python3
#-*- coding: utf-8 -*-


import sys
import logging
ISPY3 = sys.version_info >= (3,)

if ISPY3:
    xrange = range

try:
    if ISPY3:
        import _pickle as pickle
    else:
        import cPickle as pickle
except:
    import pickle

import os
import time
import socket
from timeit import default_timer
import datetime
import traceback
import re
import functools


logging.basicConfig(format='%(levelname)-8s %(asctime)s] %(message)s', level=logging.INFO)#, datefmt='%Y/%m/%d %H:%M:%S %p')

def format_num(num):
    return re.sub('\B(?=(\d{3})+(?!\d))', ',', str(num))


def JdTraceback():
    try:
        return traceback.format_exc()
    except:
        pass

def date2s(d):
    if type(d) != type(datetime.datetime.now()):
        raise Exception('param is not datetime type!')
    return int(time.mktime(d.timetuple()))

def s2date(s):
    if type(s) == type('a'):
        s = int(s)
    return datetime.datetime.fromtimestamp(s)

def dayofweek(y, m, d):
    t = (0, 3, 2, 5, 0, 3, 5, 1, 4, 6, 2, 4)
    y -= m < 3
    return (y + y/4 - y/100 + y/400 + t[m-1] + d) % 7

def parse_bin_flag(inflag, base_flag=[1, 2, 4, 8, 16, 32, 64]):
    parse_result = []
    base_flag.reverse()
    for flag in base_flag:
        if inflag < flag:
            continue
        elif inflag in base_flag:
            parse_result.append(inflag)
            break
        else:
            inflag = inflag ^ flag
            parse_result.append(flag)

    return parse_result

def format_datatime(strdate, usenow=False):
    guess_frms = (
        '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%Y-%m-%d', '%y-%m-%d %H:%M:%S', '%y-%m-%d %H:%M:%S', '%y-%m-%d',
        '%Y/%m/%d %H:%M:%S', '%Y/%m/%d %H:%M', '%Y/%m/%d', '%y/%m/%d %H:%M:%S', '%y/%m/%d %H:%M:%S', '%y/%m/%d',
    )
    for frms in guess_frms:
        try:
            return datetime.datetime.strptime(strdate, frms)
        except:
            pass

    return datetime.datetime.now() if usenow else None

def convert_to_unicode(text):
    if ISPY3:
        if isinstance(text, str):
            return text
        elif isinstance(text, bytes):
            return text.decode("utf-8", "ignore")
        else:
            raise ValueError("Unsupported string type: %s" % (type(text)))
    else:
        if isinstance(text, str):
            return text.decode("utf-8", "ignore")
        elif isinstance(text, unicode):
            return text
        else:
            raise ValueError("Unsupported string type: %s" % (type(text)))


def format_bytes(n):
    unit = ('K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y')
    sinfo = {}
    for i, s in enumerate(unit):
        sinfo[s] = 1 << (i + 1) * 10
    for s in reversed(unit):
        if n >= sinfo[s]:
            value = float(n) / sinfo[s]
            return '{0:.2f}{1}'.format(value, s)
    return "{0}B".format(n)

def warp_func_use_time(func):
    @functools.wraps(func)
    def clac_time_run(*args, **kwargs):
        stime = default_timer()
        func(*args, **kwargs)
        u = default_timer() - stime
        BaseObject.loger.info('do function: {0} use: {1} seconds'.format(func.__name__, u))
    return clac_time_run

# 新的序列化
def obj_save(sobject, filename, protocol=2):
    try:
        flag = False
        dst_file = open(filename, 'wb')
        pickle.dump(sobject, dst_file, protocol)
        dst_file.close()
        flag = True
    except Exception as e:
        print(JdTraceback())
        print("save object failed.(%s) %s" % (str(e), filename))
    return flag

# 新的反序列化
def obj_load(filename):
    try:
        flg = False
        obj_file = open(filename, 'rb')
        # 如果是python3 读取 python2 序列化的数据 需要制定encoding
        if ISPY3:
            obj = pickle.load(obj_file, encoding='utf-8')
        else:
            obj = pickle.load(obj_file)
        obj_file.close()
        flg = True
        return flg, obj
    except Exception as er:
        print(JdTraceback())
        print("load object failed.(%s) %s" % (str(er), filename))
    return flg, None


# def show_words_cloud(wdobj, flag=0, savefile=None):
#     '''
#     show_words_cloud(wdobj, flag=0, savefile=None)
#     flag 0 仅展示 1 保存到文件 2 展示并且保存到文件
#     '''
#
#
#     from wordcloud import WordCloud
#     import matplotlib.pyplot as plt
#
#     wcobj = WordCloud(
#                    font_path='/project/python/KnowledgeGraph/fonts/msyh.ttf',  # 设置字体
#                    background_color="black",  # 背景颜色max_words=2000,# 词云显示的最大词数
#                    max_words=500000,
#                    width=1920,
#                    height = 1080,
#                    # mask=alice_coloring,  # 设置背景图片
#                    # max_font_size=50,  # 字体最大值
#                    min_font_size=10,
#                    # random_state=42,
#                    margin=15)
#
#     wx = {}
#     for k, v in wdobj.items():
#         wx[k.decode('utf-8', 'ignore')] = v
#
#     wcobj.generate_from_frequencies(wx)
#     if flag in (1, 2) and savefile is not None:
#         wcobj.to_file(savefile)
#
#     if flag != 1:
#         plt.figure()
#         plt.imshow(wcobj)
#         plt.axis("off")
#         plt.show()

def FullWidth2HalfWidth(unicodestring):
    '''
    :param unicodestring: unicode字符串
    :return:    半角unicode字符串
    '''
    result = []
    for uchar in unicodestring:
        c_code = ord(uchar)
        # 空格
        if c_code == 12288:
            c_code = 32
        # 全角的“”转为 "
        elif c_code == 8220 or c_code == 8221:
            c_code = 34
        # 全角的‘’转为 '
        elif c_code == 8216 or c_code == 8217:
            c_code = 39
        # 其它全角字符串按照换算关系换算
        elif c_code >= 65281 and c_code <= 65374:
            c_code -= 65248

        if ISPY3:
            result.append(chr(c_code))
        else:
            result.append(unichr(c_code))
    return u''.join(result)


class BaseObject(object):
    loger = logging.getLogger()
    def __init__(self, log_file=None):
        self.debug = True
        self.loger.setLevel(logging.DEBUG if self.debug else logging.ERROR)
        self.obj_load_timepos = default_timer()
        self.log_file = log_file or None
        self.has_logfile_handler = False

    def out_log_file(self, log_file=None, log_level=1):
        '''
        添加一个文件日志输出
        :param log_file:        日志文件
        :param log_level:       日志基本 1 info 2 debug 3 warning 4 error
        :return:
        '''
        # if log_file is None:
        #     try:
        #         os.makedirs('../logs')
        #     except: pass
        #     self.log_file = os.path.join('../logs', '{0}.log'.format(datetime.datetime.now().strftime("%Y-%m-%d-%H-%M")))

        self.log_file = log_file
        f_handler = logging.FileHandler(self.log_file)
        f_handler.setFormatter(logging.Formatter("%(levelname)-8s %(asctime)s] %(message)s"))
        # f_handler.setLevel(logging.ERROR)
        f_handler.setLevel(logging.DEBUG if self.debug else logging.ERROR)
        self.loger.addHandler(f_handler)
        self.has_logfile_handler = True


class ProcessInfo(object):
    __slots__ = ()
    @classmethod
    def get_process_by_pid(cls, pid):
        import psutil
        try:
            process_info = psutil.Process(pid)
            return process_info
        except:
            return None

    @classmethod
    def get_process_by_name(cls, process_name=None, base_filename=None):
        import psutil
        process_info = {}
        try:
            for p in psutil.process_iter():
                if process_name is not None:
                    if p.name() == process_name:
                        process_info[p.pid] = p
                if base_filename is not None:
                    if os.path.basename(p.cmdline()) == base_filename:
                        process_info[p.pid] = p
        except:
            return None
        return list(process_info.values())

    @classmethod
    def get_node_info(cls):
        hostname = socket.gethostname()
        # 获取本机ip
        host_ip = socket.gethostbyname(hostname)
        return (hostname, host_ip)

    @staticmethod
    def info_format(proc):
        ioinfo = proc.io_counters()
        meminfo = proc.memory_full_info()
        info = {
            # 进程pid
            "pid": proc.pid,
            # 进程名
            'process_name': proc.name(),
            'ip_info': ProcessInfo.get_node_info(),
            # 进程 cmd_line
            'cmd_line': proc.cmdline(),
            # 进程创建时间
            'create_time': datetime.datetime.fromtimestamp(proc.create_time()).strftime("%Y-%m-%d %H:%M:%S"),
            'io_read_bytes': format_bytes(ioinfo.read_bytes),
            'io_write_bytes': format_bytes(ioinfo.write_bytes),
            # 进程线程数目
            'num_threads': proc.num_threads(),
            # 子进程数目
            'child_process': len(proc.children(recursive=False)),
            # cpu使用百分比
            'cpu_percent': proc.cpu_percent(),
            # 进程上下文切换次数
            'num_ctx_switches': proc.num_ctx_switches(),
            # 进程网络连接数目
            'net_connections': len(proc.connections()),
            # 进程当前打开的文件描述符数
            'num_fds': proc.num_fds(),
            # 进程内存使用
            'pss_mem': format_bytes(meminfo.pss),
            # 父进程id
            'parent_pid': proc.ppid(),
        }

        return info

    @staticmethod
    def get(pid=None, process_name=None, base_filename=None):
        if pid is not None:
            pinfo = ProcessInfo.get_process_by_pid(pid)
            if pinfo is not None:
                return ProcessInfo.info_format(pinfo)
            return None
        if process_name is not None or base_filename is not None:
            pinfos = ProcessInfo.get_process_by_name(process_name, base_filename)
            if pinfos is None:
                return None
            return list(map(lambda y: ProcessInfo.info_format(y), pinfos))
        return None

if __name__ == "__main__":
    # pass
    T = BaseObject()
    T.loger.info('hello')
    T.loger.error('我来测试下错误')