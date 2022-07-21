# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    保存和加载模型
"""
import pickle
import os
import pyhdfs
import sys
# file_path=os.path.abspath(os.path.join(os.path.dirname(__file__),'../../'))#兼顾spark-submit方式
# sys.path.append(file_path)
from zipfile import ZipFile
import shutil
from forecast.common.config import get_config


def save_model(model,filename):
    '''
    保存模型
    :param model:模型
    :param filename:模型存放地址
    :return:
    '''
    s=pickle.dumps(model)
    f=open(filename, "wb+")
    f.write(s)
    f.close()


def load_model(filename):
    '''
    加载模型
    :param filename:模型文件地址
    :param model_name:模型命名
    :return:加载的模型
    '''
    f2=open(filename, 'rb')
    s2=f2.read()
    model=pickle.loads(s2)
    return model


def get_client():
    """
    获取操作hdfs的client
    :return:
    """
    file_tmp="/common/config/"
    environment=file_tmp+r'environment.toml'
    if os.path.exists(file_path):#如果压缩文件存在，是为了兼顾spark_submit形式
        try:
            dst_dir=os.getcwd()+'/zip_tmp'
            zo=ZipFile(file_path,'r')
            if os.path.exists(dst_dir):
                shutil.rmtree(dst_dir)
            os.mkdir(dst_dir)
            for file in zo.namelist():
                zo.extract(file, dst_dir)
            environment=dst_dir+environment#解压后的地址
        except:
            environment=file_path+environment#解压后的地址
    hdfs_conf=get_config(environment,'hdfs')#hdfs的配置
    hosts=hdfs_conf['hosts']
    user_name=hdfs_conf['user_name']
    client=pyhdfs.HdfsClient(hosts=hosts,user_name=user_name)
    return client

client=get_client()#hdfs的client

def save_model_hdfs(model,model_name,hdfs_path):
    """
    保存model到hdfs
    :param model:模型文件
    :param model_name: model命名
    :param hdfs_path: hdfs地址
    :return:
    """
    file_local=r'model_tmp'#创建临时文件地址
    file_local_tmp= file_local+'/'+model_name
    if os.path.exists(file_local)==False:
        os.makedirs(file_local)
    save_model(model, file_local_tmp)
    hdfs_path_tmp = hdfs_path + '/' + model_name
    client.copy_from_local(file_local_tmp, hdfs_path_tmp, overwrite=True)  # 上传到hdfs
    os.remove(file_local_tmp)
    os.rmdir(file_local)

def load_model_hdfs(model_name, hdfs_path):
    """
    从hdfs上加载模型
    :param model_name:模型命名
    :param hdfs_path: hdfs地址
    :return: model文件
    """
    file_local=r'model_tmp'
    if os.path.exists(file_local)==False:
        os.makedirs(file_local)
    file_local_tmp=file_local + '/' + model_name
    hdfs_path_tmp=hdfs_path + '/' + model_name
    client.copy_to_local(hdfs_path_tmp, file_local_tmp)
    model=load_model(file_local_tmp)
    os.remove(file_local_tmp)
    os.rmdir(file_local)
    return model