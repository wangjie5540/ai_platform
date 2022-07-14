# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    spark初始化
"""
import os
from pyspark.sql import SparkSession
from pyspark import SparkConf
import sys
file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))#兼顾spark-submit方式
sys.path.append(file_path)
from zipfile import ZipFile
import shutil
from common_helper.config import get_config

def spark_init(add_file=None):
    """
    spark初始化,兼顾pyspark和spark-submit形式
    :param add_file:
    :return:
    """
    file_tmp="/common/config/"
    environment=file_tmp+r'environment.toml'
    if os.path.exists(file_path):#如果压缩文件存在，是为了兼顾spark_submit形式
        try:
            dst_dir=os.getcwd() + '/zip_tmp'
            zo=ZipFile(file_path,'r')
            if os.path.exists(dst_dir):
                shutil.rmtree(dst_dir)
            os.mkdir(dst_dir)
            for file in zo.namelist():
                zo.extract(file, dst_dir)
            environment=dst_dir+environment  # 解压后的地址
        except:
            environment = file_path+environment  # 解压后的地址
    spark_conf_dict=get_config(environment,'spark')#spark的配置
    os.environ["PYSPARK_DRIVER_PYTHON"]=spark_conf_dict['pyspark_driver_python']
    os.environ['PYSPARK_PYTHON']=spark_conf_dict['pyspark_python']
    sparkconf=SparkConf().setAppName(spark_conf_dict['app_name']).setMaster(spark_conf_dict['master'])
    for key,value in spark_conf_dict.items():
        if key in ['pyspark_driver_python','pyspark_python','app_name','master']:
            continue
        sparkconf.set(key,value)
    spark=SparkSession.builder.config(conf=sparkconf).enableHiveSupport().getOrCreate()
    sc=spark.sparkContext
    try:
        if add_file in file_path:
            zip_path=file_path
        else:
            zip_path=file_path+'/'+add_file
        sc.addPyFile(zip_path)
    except:
        pass
    print('spark 启动成功')
    try:
        shutil.rmtree(dst_dir)
        zo.close()
    except:
        pass
    return spark