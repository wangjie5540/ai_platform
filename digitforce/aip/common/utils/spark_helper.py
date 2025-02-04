# coding: utf-8
import digitforce.aip.common.utils.config_helper as config_helper

import os
import zipfile
import json

SUBMIT_ZIP_PATH = 'submit.zip'


class SparkClient(object):
    _session = None
    _spark_client = None

    def __init__(self, client_host=None,):
        spark_config = config_helper.get_module_config("spark")
        spark_config.update(config_helper.get_component_config("spark"))
        print("spark_config: ")
        print(json.dumps(spark_config, indent=4, sort_keys=True))
        os.environ['SPARK_HOME'] = spark_config['spark_home']
        os.environ['JAVA_HOME'] = spark_config['java_home']
        import findspark
        findspark.init()
        from pyspark.sql import SparkSession
        import digitforce.aip.common.utils.ip_helper as ip_helper
        import digitforce.aip.common.utils.component_helper as component_helper
        if client_host is None:
            client_host = ip_helper.get_local_ip()
        # 动态分区配置参考：https://blog.csdn.net/lovetechlovelife/article/details/114544073
        self._session = SparkSession.builder.appName(component_helper.get_component_app_name()) \
            .master(spark_config['master_uri']) \
            .config("spark.driver.host", client_host) \
            .config("spark.kubernetes.container.image", spark_config['kubernetes_runtime_image']) \
            .config("spark.kubernetes.container.image.pullPolicy", "Always") \
            .config("spark.kubernetes.namespace", "kubeflow-user-example-com") \
            .config("spark.sql.autoBroadcastJoinThreshold", -1) \
            .config("spark.executor.instances", spark_config.get("spark.executor.instances", "2")) \
            .config("spark.debug.maxToStringFields", spark_config.get("spark.debug.maxToStringFields", 100)) \
            .config("spark.executor.cores", spark_config.get("spark.executor.cores", "2")) \
            .config("spark.executor.memory", spark_config.get("spark.executor.memory", "2g")) \
            .config("spark.rpc.message.maxSize", spark_config.get("spark.rpc.message.maxSize", 1000)) \
            .config("spark.driver.memory", spark_config.get("spark.driver.memory", "2g")) \
            .config("spark.driver.cores", spark_config.get("spark.driver.cores", "2")) \
            .config("spark.driver.maxResultSize", spark_config.get("spark.driver.maxResultSize", "1g")) \
            .config("spark.sql.sources.partitionOverwriteMode", "DYNAMIC") \
            .config("hive.metastore.uris", spark_config['hive_uris']) \
            .config("spark.submit.pyFiles", SUBMIT_ZIP_PATH) \
            .enableHiveSupport().getOrCreate()

    def get_session(self):
        return self._session

    def get_starrocks_table_df(self, table_name):
        starrocks_config = config_helper.get_module_config("starrocks")
        return self._session.read.format('starrocks') \
            .option('starrocks.table.identifier', f'{table_name}') \
            .option('starrocks.fenodes', f'{starrocks_config["fenodes"]}') \
            .option('user', f"{starrocks_config['user']}") \
            .option('password', f"{starrocks_config['password']}").load()

    @staticmethod
    def get():
        if SparkClient._spark_client is None:
            SparkClient._spark_client = SparkClient()
        return SparkClient._spark_client


# 定义递归函数，用于打包目录及其子目录中的文件和文件夹
def zipdir(zip_file_obj, path):
    # 获取当前目录的文件和文件夹列表
    files = os.listdir(path)

    # 遍历文件和文件夹
    for file in files:
        if file == SUBMIT_ZIP_PATH:
            continue
        # 拼接文件/文件夹的完整路径
        full_path = os.path.join(path, file)
        # 如果是文件夹，递归调用 zipdir 函数
        if os.path.isdir(full_path):
            zipdir(zip_file_obj, full_path)
        # 如果是文件，打包文件
        else:
            zip_file_obj.write(full_path)


# 创建 zip 压缩包
zip_file = zipfile.ZipFile(SUBMIT_ZIP_PATH, 'w')

# 打包当前目录及其子目录中的文件和文件夹
zipdir(zip_file, '.')
zip_file.close()
