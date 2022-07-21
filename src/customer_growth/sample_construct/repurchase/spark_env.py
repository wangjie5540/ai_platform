import os

from digitforce.aip.common.utils.spark_helper import SparkClient

class SparkEnv:
    def __new__(self, envname):
        self.spark = SparkClient().get_session()
        return self.spark


