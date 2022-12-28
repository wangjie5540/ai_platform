import logging
import os.path
import pickle

from digitforce.aip.common.utils.hdfs_helper import hdfs_client
from digitforce.aip.common.utils.hive_helper import hive_client


class Feature:
    def __init__(self, name, version, default=None, source_table_name=None):
        self.name = name
        self.default = default
        self.version = version
        self.source_table_name = source_table_name
        if hdfs_client.exists(self.get_save_path()):
            self.read_from_hdfs()

    def get_save_path(self):
        return "/user/ai/aip/model_feature/v1" + f"/{self.source_table_name}/{self.name}/{self.version}"

    def save_to_hdfs(self):
        hdfs_path = self.get_save_path()
        hdfs_client.mkdir_dirs(os.path.dirname(hdfs_path))
        result = pickle.dumps(self)
        hdfs_client.write_to_hdfs(result, hdfs_path)

    def read_from_hdfs(self):
        return self

    def get_model_feature_value(self, value):
        pass


class CategoryFeatureEncoder(Feature):
    def __init__(self, name, version, default=None, source_table_name=None):
        self.vocabulary = None
        self.source_table_name = source_table_name
        super(CategoryFeatureEncoder, self).__init__(name, version, default, source_table_name)

    def generate_vocabulary(self):
        # todo 如果结果表是分区表 则用version取对应分区的数据
        query_sql = f'''
                    SELECT 
                        DISTINCT {self.name}  AS {self.name}
                    FROM {self.source_table_name}
            '''
        df = hive_client.query_to_df(query_sql)
        self.vocabulary = dict([(_, i + 1) for i, _ in enumerate(df[self.name].astype(str))])

    def get_model_feature_value(self, value):
        # todo 如果结果表是分区表 则去version所在分区的数据
        if not isinstance(value, str):
            logging.warning(f"the category feature is wanted as string but is {type(value)} {value}")
            value = str(value)
        return self.vocabulary.get(value, self.default)

    def read_from_hdfs(self):
        hdfs_path = self.get_save_path()
        c = hdfs_client.read_pickle_from_hdfs(hdfs_path)
        self.vocabulary = c.vocabulary
        return self


class NumberFeature(Feature):
    def __init__(self, name, version, default=None, source_table_name=None):
        self.source_table_name = source_table_name
        self.mean = None
        self.std = None
        super(NumberFeature, self).__init__(name, version, default, source_table_name)

    def generate_mean_and_std(self):
        query_sql = f'''
                    SELECT 
                        SUM({self.name}) / COUNT(*)  AS mean_res,
                        STDDEV({self.name}) AS stddev_res 
                    FROM {self.source_table_name}
            '''
        df = hive_client.query_to_df(query_sql)
        self.mean = df["mean_res"].tolist()[0]
        self.std = df["stddev_res"].tolist()[0]

    def get_model_feature_value(self, value):
        try:
            result = (value - self.mean) / self.std
            return result
        except Exception as e:
            logging.error(e)
            return self.default

    def read_from_hdfs(self):
        hdfs_path = self.get_save_path()
        c = hdfs_client.read_pickle_from_hdfs(hdfs_path)
        self.std = c.std
        self.mean = c.mean
        return self


def main():
    print(hdfs_client.list_dir("/user"))


if __name__ == '__main__':
    main()
