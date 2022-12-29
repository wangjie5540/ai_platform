import logging
import os.path
import pickle

import glob
import os
import pickle
import uuid

import pyhdfs
# coding: utf-8
import pandas as pd
from pyhive import hive



class HiveClient:
    def __init__(self, host=None, port=None, username='root'):
        self.conn = hive.Connection(host=host, port=port, username=username)

    def __del__(self):
        self.conn.close()

    def get_table_size(self, table_name):
        cur = self.conn.cursor()
        sql = f'desc formatted {table_name}'
        cur.execute(sql)
        all_data = cur.fetchall()
        cur.close()
        for item in all_data:
            if item[1] is not None and item[1].startswith('totalSize'):
                return int(item[2].strip())

    def query_to_df(self, sql):
        df = pd.read_sql(sql, self.conn)
        return df

    def query_to_table(self, sql, table_name, db=None, delete_tb=False):
        cursor = self.conn.cursor()
        if delete_tb:
            self.delete_table(table_name)
        table_name = f"{db}.{table_name}" if db else table_name
        _sql = f"CREATE TABLE IF NOT EXISTS {table_name} AS " \
               f"{sql}"
        cursor.execute(_sql)

    def delete_table(self, table_name):
        cursor = self.conn.cursor()
        _sql = f"DROP TABLE IF EXISTS {table_name}"
        cursor.execute(_sql)

    def close(self):
        if self.conn:
            self.conn.close()
        self.conn = None


hive_client = HiveClient(host="172.22.20.57", port=7001)



class HdfsClient:
    def __init__(self, hosts=None, user_name="root"):
        self.hdfs_client = pyhdfs.HdfsClient(hosts=hosts, user_name=user_name)

    def get_client(self):
        return self.hdfs_client

    def list_dir(self, path):
        return self.hdfs_client.listdir(path)

    def list_status(self, path):
        return self.hdfs_client.list_status(path)

    def delete(self, path, recursive=True):
        return self.hdfs_client.delete(path, recursive=recursive)

    def copy_to_local(self, src: str, localdest: str):
        return self.hdfs_client.copy_to_local(src, localdest)

    def copy_from_local(self, localsrc: str, dest: str):
        return self.hdfs_client.copy_from_local(localsrc, dest)

    def mkdirs(self, path):
        return self.hdfs_client.mkdirs(path)

    def exists(self, path):
        return self.hdfs_client.exists(path)

    def mkdir_dirs(self, path):
        assert path.startswith("/")
        vals = path.split('/')
        _path = ""
        for _ in vals:
            _path += _ + "/"
            if not self.get_client().exists(_path):
                self.mkdirs(_path)

    def copy_from_local_dir(self, local_dir, dest):
        self.mkdir_dirs(dest)
        files = glob.glob(local_dir)
        for _ in files:
            _dest = os.path.join(dest, os.path.basename(_))
            self.copy_from_local(_, _dest)

    def copy_dir_to_local(self, local_dir, dest_dir):
        dest_files = self.list_dir(dest_dir)
        if not os.path.exists(local_dir):
            os.makedirs(local_dir, exist_ok=True)
        for _ in dest_files:
            dest = os.path.join(dest_dir, _)
            local_path = os.path.join(local_dir, _)
            self.copy_to_local(dest, local_path)

    def write_to_hdfs(self, content, dest_path):
        self.delete(dest_path)
        tmp_file = f"/tmp/{uuid.uuid4()}"
        with open(tmp_file, "wb") as fo:
            fo.write(content)
        self.copy_from_local(tmp_file, dest_path)
        os.remove(tmp_file)

    def read_pickle_from_hdfs(self, src):
        tmp_file = f"/tmp/{uuid.uuid4()}"
        self.copy_to_local(src, tmp_file)
        with open(tmp_file, "rb") as fi:
            obj = pickle.load(fi)
        os.remove(tmp_file)
        return obj


hdfs_client = HdfsClient("172.22.20.137:4008,172.22.20.110:4008")

class FeatureEncoder(object):
    def __init__(self, name, version, default=None, source_table_name=None):
        self.source_table_name = source_table_name
        self.name = name
        self.version = version
        self.default = default

    def get_model_feature_value(self, value):
        return self.default


class CategoryFeatureEncoder(FeatureEncoder):
    def __init__(self, name, version, default=None, source_table_name=None):
        super(CategoryFeatureEncoder, self).__init__(name, version, default, source_table_name)
        self.vocabulary = {}

    def get_model_feature_value(self, value):
        if not isinstance(value, str):
            logging.warning(f"the category feature is wanted as string but is {type(value)} {value}")
            value = str(value)
        return self.vocabulary.get(value, self.default)


class NumberFeatureEncoder(FeatureEncoder):
    def __init__(self, name, version, default=None, source_table_name=None):
        super(NumberFeatureEncoder, self).__init__(name, version, default, source_table_name)
        self.mean = None
        self.std = None

    def get_model_feature_value(self, value):
        try:
            result = (value - self.mean) / self.std
            return result
        except Exception as e:
            logging.error(e)
            return self.default


class FeatureEncoderCalculator:

    @classmethod
    def calculate(cls, encoder):
        pass

    @classmethod
    def get_save_path(cls, encoder):
        return "/user/ai/aip/zq/model_feature/v1" + f"/{encoder.source_table_name}/{encoder.name}/{encoder.version}"

    @classmethod
    def save_to_hdfs(cls, encoder: FeatureEncoder):
        hdfs_path = cls.get_save_path(encoder)
        hdfs_client.mkdir_dirs(os.path.dirname(hdfs_path))
        result = pickle.dumps(encoder)
        hdfs_client.write_to_hdfs(result, hdfs_path)

    @classmethod
    def read_from_hdfs(cls, encoder):
        hdfs_path = cls.get_save_path(encoder)
        pickle_encoder = hdfs_client.read_pickle_from_hdfs(hdfs_path)
        return pickle_encoder

    @classmethod
    def load_encoder(cls, encoder: FeatureEncoder, use_hdfs=True):
        if hdfs_client.exists(cls.get_save_path(encoder)) and use_hdfs:
            encoder = cls.read_from_hdfs(encoder)
            return encoder
        cls.calculate(encoder)
        cls.save_to_hdfs(encoder)


class CategoryFeatureEncoderCalculator(FeatureEncoderCalculator):
    @classmethod
    def calculate(cls, encoder: CategoryFeatureEncoder):
        query_sql = f'''
                            SELECT 
                                DISTINCT {encoder.name}  AS {encoder.name}
                            FROM {encoder.source_table_name}
                    '''
        df = hive_client.query_to_df(query_sql)
        encoder.vocabulary = dict([(_, i + 1) for i, _ in enumerate(df[encoder.name].astype(str))])

    @classmethod
    def read_from_hdfs(cls, encoder: CategoryFeatureEncoder):
        _encoder = super().read_from_hdfs(encoder)
        encoder.vocabulary = _encoder.vocabulary
        return encoder


class NumberFeatureEncoderCalculator(FeatureEncoderCalculator):
    @classmethod
    def calculate(cls, encoder: NumberFeatureEncoder):
        query_sql = f'''
                            SELECT 
                                SUM({encoder.name}) / COUNT(*)  AS mean_res,
                                STDDEV({encoder.name}) AS stddev_res 
                            FROM {encoder.source_table_name}
                    '''
        df = hive_client.query_to_df(query_sql)
        encoder.mean = df["mean_res"].tolist()[0]
        encoder.std = df["stddev_res"].tolist()[0]
        return encoder

    @classmethod
    def read_from_hdfs(cls, encoder: NumberFeatureEncoder):
        _encoder = super().read_from_hdfs(encoder)
        encoder.std = _encoder.std
        encoder.mean = _encoder.mean
        return encoder


def main():
    print(hdfs_client.list_dir("/user/ai/aip/zq/model_feature/v1/algorithm.tmp_raw_user_feature_table_name/"))


if __name__ == '__main__':
    main()
