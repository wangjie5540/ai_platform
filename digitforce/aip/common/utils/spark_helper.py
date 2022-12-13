# coding: utf-8
import digitforce.aip.common.utils.config_helper as config_helper

spark_config = config_helper.get_module_config("spark")
starrocks_config = config_helper.get_module_config("starrocks")
import os

os.environ['SPARK_HOME'] = spark_config['spark_home']
os.environ['JAVA_HOME'] = spark_config['java_home']
import findspark

findspark.init()
from pyspark.sql import SparkSession
import digitforce.aip.common.utils.ip_helper as ip_helper
import digitforce.aip.common.utils.component_helper as component_helper


class SparkClient(object):
    def __init__(self, client_host=None):
        if client_host is None:
            client_host = ip_helper.get_local_ip()
        self._session = SparkSession.builder.appName(component_helper.get_component_app_name()) \
            .master(spark_config['master_uri']) \
            .config("spark.driver.host", client_host) \
            .config("spark.kubernetes.container.image", spark_config['kubernetes_runtime_image']) \
            .config("hive.metastore.uris", spark_config['hive_uris']) \
            .enableHiveSupport().getOrCreate()

    def get_session(self):
        return self._session

    def get_starrocks_table_df(self, table_name):
        return self._session.read.format("jdbc") \
            .option('url', starrocks_config['jdbc_url']) \
            .option('dbtable', table_name) \
            .option('user', starrocks_config['user']) \
            .option('password', starrocks_config['password']) \
            .load()


spark_client = SparkClient()
spark_session = spark_client.get_session()
