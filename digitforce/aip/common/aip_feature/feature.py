import logging
import os.path
import pickle

from digitforce.aip.common.utils.hdfs_helper import hdfs_client
from digitforce.aip.common.utils.hive_helper import hive_client


class Feature:
    def __init__(self, name, version, default=None):
        self.name = name
        self.default = default
        self.version = version

    def save_to_hdfs(self):
        hdfs_path = "/user/ai/aip/model_feature/v1" + f"/{self.name}/{self.version}"
        hdfs_client.mkdir_dirs(os.path.dirname(hdfs_path))
        result = pickle.dumps(self)
        hdfs_client.write_to_hdfs(hdfs_path, result)

    def read_from_hdfs(self):
        return self

    def get_model_feature_value(self, value):
        pass


class CategoryFeatureEncoder(Feature):
    def __init__(self, name, version, default=None, source_table_name=None):
        super(CategoryFeatureEncoder, self).__init__(name, version, default)
        self.vocabulary = None
        self.source_table_name = source_table_name

    def generate_vocabulary(self):
        query_sql = f'''
                    SELECT 
                        DISTINCT {self.name}  
                    FROM {self.source_table_name}
            '''
        df = hive_client.query_to_df(query_sql)
        self.vocabulary = dict([(_, i + 1) for i, _ in enumerate(df[self.name].astype(str))])

    def get_model_feature_value(self, value):
        if not isinstance(value, str):
            logging.warning(f"the category feature is wanted as string but is {type(value)} {value}")
            value = str(value)
        return self.vocabulary.get(value, self.default)

    def save_to_hdfs(self):
        hdfs_path = "/user/ai/aip/model_feature/v1" + f"/{self.name}/{self.version}"
        hdfs_client.mkdir_dirs(os.path.dirname(hdfs_path))
        result = pickle.dumps(self)
        hdfs_client.write_to_hdfs(result, hdfs_path)

    def read_from_hdfs(self):
        hdfs_path = "/user/ai/aip/model_feature/v1" + f"/{self.name}/{self.version}"
        c = hdfs_client.read_pickle_from_hdfs(hdfs_path)
        self.vocabulary = c.vocabulary
        return self


class NumberFeature(Feature):
    def __init__(self, name, version, default=None):
        super(NumberFeature, self).__init__(name, version, default)
        self.mean = None
        self.std = None

    def generate_mean_and_std(self):
        pass

    def get_model_feature_value(self, value):
        try:
            result = (value - self.mean) / self.std
            return result
        except Exception as e:
            logging.error(e)
            return self.default

    def read_from_hdfs(self):
        hdfs_path = "/user/ai/aip/model_feature/v1" + f"/{self.name}/{self.version}"
        c = hdfs_client.read_pickle_from_hdfs(hdfs_path)
        self.std = c.std
        self.mean = c.mean
        return self


def main():
    print(hdfs_client.list_dir("/user"))


if __name__ == '__main__':
    main()
