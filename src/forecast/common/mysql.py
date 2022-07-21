# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    mysql读取数据
"""
from forecast.common.reference_package import *
from forecast.common.config import get_config


def connect_mysql():
    """
    主要是一些链接mysql的信息
    :return:
    """
    file_path = os.getcwd()+"/forecast/common/config/environment.toml"
    # if os.path.exists(file_path):#如果压缩文件存在，是为了兼顾spark_submit形式
    #     try:
    #         dst_dir = os.getcwd()+'/zip_tmp'
    #         zo = ZipFile(file_path, 'r')
    #         if os.path.exists(dst_dir):
    #             shutil.rmtree(dst_dir)
    #         os.mkdir(dst_dir)
    #         for file in zo.namelist():
    #             zo.extract(file, dst_dir)
    #         environment = dst_dir+environment#解压后的地址
    #     except:
    #         environment = file_path + environment  #解压后的地址
    mysql_dict=get_config(file_path, 'mysql')#spark的配置
    mysql_host=mysql_dict['mysql_host']
    mysql_port=mysql_dict['mysql_port']
    mysql_user=mysql_dict['mysql_user']
    mysql_password=mysql_dict['mysql_password']
    db = pymysql.Connect(host=mysql_host, port=mysql_port, user=mysql_user, passwd=mysql_password)
    return db


def get_data_from_mysql(query):
    """
    链接mysql
    :param query:sql语句
    :return:返回运行SQL得到的数据
    """
    db = connect_mysql()
    cur = db.cursor()
    cur.execute(query)
    data = cur.fetchall()
    columnDes = cur.description
    columnNames = [columnDes[i][0] for i in range(len(columnDes))]
    df = pd.DataFrame([list(i) for i in data], columns=columnNames)
    cur.close()
    db.close()
    return df