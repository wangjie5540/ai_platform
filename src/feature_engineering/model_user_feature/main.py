import glob
import json
import logging
import os
import os.path
import pickle
import uuid

# coding: utf-8
import pandas as pd
import pyhdfs
from pyhive import hive

from digitforce.aip.common.utils.spark_helper import spark_client


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
        if value in [None, "null", "", "None", "NULL"]:
            return float(self.default)
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


DATE_FORMAT = "%Y-%m-%d"
PATH_DATE_FORMAT = "%Y/%m/%d"
PATH_DATE_HOUR_FORMAT = "%Y/%m/%d/%H"

import datetime


def get_today_str(data_format=DATE_FORMAT):
    return datetime.datetime.today().strftime(data_format)


USER_RAW_FEATURE_TABLE_NAME = "algorithm.tmp_raw_user_feature_table_name"

ITEM_RAW_FEATURE_TABLE_NAME = "algorithm.tmp_raw_item_feature_table_name"


###################################################################################
#   ['gender', 'EDU', 'RSK_ENDR_CPY', 'NATN', 'OCCU', 'IS_VAIID_INVST']
class UserIdEncoder(CategoryFeatureEncoder):
    def __init__(self, source_table_name, version=get_today_str()):
        super().__init__("user_id", version, 0, source_table_name)


class UserGenderEncoder(CategoryFeatureEncoder):
    def __init__(self, source_table_name, version=get_today_str()):
        super().__init__("gender", version, 0, source_table_name)


class UserEducationEncoder(CategoryFeatureEncoder):
    def __init__(self, source_table_name, version=get_today_str()):
        super().__init__("edu", version, 0, source_table_name)


class UserRSKENDRCPYEncoder(CategoryFeatureEncoder):
    def __init__(self, source_table_name, version=get_today_str()):
        super().__init__("rsk_endr_cpy", version, 0, source_table_name)


class UserOCCUEncoder(CategoryFeatureEncoder):
    def __init__(self, source_table_name, version=get_today_str()):
        super().__init__("occu", version, 0, source_table_name)


class UserNATNEncoder(CategoryFeatureEncoder):
    def __init__(self, source_table_name, version=get_today_str()):
        super().__init__("natn", version, 0, source_table_name)


class UserISVAIIDINVSTEncoder(CategoryFeatureEncoder):
    def __init__(self, source_table_name, version=get_today_str()):
        super().__init__("is_vaiid_invst", version, 0, source_table_name)


# NumberFeatureEncoder

class UserBUY_COUNTS_30D(NumberFeatureEncoder):
    def __init__(self, source_table_name, version=get_today_str()):
        super().__init__("u_buy_counts_30d", version, 0, source_table_name)


class UserAMOUNT_SUM_30D(NumberFeatureEncoder):
    def __init__(self, source_table_name, version=get_today_str()):
        super().__init__("u_amount_sum_30d", version, 0, source_table_name)


class UserAMOUNT_AVG_30D(NumberFeatureEncoder):
    def __init__(self, source_table_name, version=get_today_str()):
        super().__init__("u_amount_avg_30d", version, 0, source_table_name)


class UserAMOUNT_MIN_30D(NumberFeatureEncoder):
    def __init__(self, source_table_name, version=get_today_str()):
        super().__init__("u_amount_min_30d", version, 0, source_table_name)


class UserAMOUNT_MAX_30D(NumberFeatureEncoder):
    def __init__(self, source_table_name, version=get_today_str()):
        super().__init__("u_amount_max_30d", version, 0, source_table_name)


class UserBUY_DAYS_30D(NumberFeatureEncoder):
    def __init__(self, source_table_name, version=get_today_str()):
        super().__init__("u_buy_days_30d", version, 0, source_table_name)


class UserBUY_AVG_DAYS_30D(NumberFeatureEncoder):
    def __init__(self, source_table_name, version=get_today_str()):
        super().__init__("u_buy_avg_days_30d", version, 0, source_table_name)


class UserLAST_BUY_DAYS_30D(NumberFeatureEncoder):
    def __init__(self, source_table_name, version=get_today_str()):
        super().__init__("u_last_buy_days_30d", version, 0, source_table_name)


####################################################################################
#  ['ts_code', 'fund_type', 'management', 'custodian', 'invest_type'] ##
class ItemIdEncoder(CategoryFeatureEncoder):
    def __init__(self, source_table_name, version=get_today_str()):
        super().__init__("item_id", version, 0, source_table_name)


class FundTypeEncoder(CategoryFeatureEncoder):
    def __init__(self, source_table_name, version=get_today_str()):
        super().__init__("fund_type", version, 0, source_table_name)


class FundManagementEncoder(CategoryFeatureEncoder):
    def __init__(self, source_table_name, version=get_today_str()):
        super().__init__("management", version, 0, source_table_name)


class FundCustodianEncoder(CategoryFeatureEncoder):
    def __init__(self, source_table_name, version=get_today_str()):
        super().__init__("custodian", version, 0, source_table_name)


class FundInvestTypeEncoder(CategoryFeatureEncoder):
    def __init__(self, source_table_name, version=get_today_str()):
        super().__init__("invest_type", version, 0, source_table_name)


class ItemBUY_COUNTS_30D(NumberFeatureEncoder):
    def __init__(self, source_table_name, version=get_today_str()):
        super().__init__("i_buy_counts_30d", version, 0, source_table_name)


class ItemAMOUNT_SUM_30D(NumberFeatureEncoder):
    def __init__(self, source_table_name, version=get_today_str()):
        super().__init__("i_amount_sum_30d", version, 0, source_table_name)


class ItemAMOUNT_AVG_30D(NumberFeatureEncoder):
    def __init__(self, source_table_name, version=get_today_str()):
        super().__init__("i_amount_avg_30d", version, 0, source_table_name)


class ItemAMOUNT_MIN_30D(NumberFeatureEncoder):
    def __init__(self, source_table_name, version=get_today_str()):
        super().__init__("i_amount_min_30d", version, 0, source_table_name)


class ItemAMOUNT_MAX_30D(NumberFeatureEncoder):
    def __init__(self, source_table_name, version=get_today_str()):
        super().__init__("i_amount_max_30d", version, 0, source_table_name)


######################################################################################################################

class EncoderFactory:
    def __init__(self, source_table_name, version=get_today_str(), encoder_cls=None):
        if encoder_cls is None:
            encoder_cls = []
        self.encoder_cls = encoder_cls
        self._factory = None
        self.source_table_name = source_table_name
        self.version = version

    def get_encoder(self, name):
        return self._factory.get(name, None)

    def get_encoder_names(self):
        return self._factory.keys()


def full_factory(encoder_factory):
    encoder_factory._factory = {}
    for _ in encoder_factory.encoder_cls:
        encoder = _(encoder_factory.source_table_name, encoder_factory.version)
        if isinstance(encoder, NumberFeatureEncoder):
            if not (encoder.std and encoder.mean):
                NumberFeatureEncoderCalculator.load_encoder(encoder)
        elif isinstance(encoder, CategoryFeatureEncoder):
            if not encoder.vocabulary:
                CategoryFeatureEncoderCalculator.load_encoder(encoder)
        else:
            raise TypeError(f"the encoder cls is wanted {NumberFeatureEncoder} or {CategoryFeatureEncoder}"
                            f" but {type(encoder)} instance {encoder}")
        encoder_factory._factory[encoder.name] = encoder


class UserEncoderFactory(EncoderFactory):
    def __init__(self, source_table_name, version=get_today_str()):
        self.encoder_cls = [
            ############ user category feature
            UserIdEncoder,
            UserGenderEncoder,
            UserEducationEncoder,
            UserRSKENDRCPYEncoder,
            UserOCCUEncoder,
            UserNATNEncoder,
            UserISVAIIDINVSTEncoder,
            ############ user number feature
            UserBUY_COUNTS_30D,
            UserAMOUNT_SUM_30D,
            UserAMOUNT_AVG_30D,
            UserAMOUNT_MIN_30D,
            UserAMOUNT_MAX_30D,
            UserBUY_DAYS_30D,
            UserBUY_AVG_DAYS_30D,
            UserLAST_BUY_DAYS_30D, ]
        super().__init__(source_table_name, version, self.encoder_cls)


class ItemEncoderFactory(EncoderFactory):
    def __init__(self, source_table_name, version=get_today_str()):
        self.encoder_cls = [
            ################ item category feature
            ItemIdEncoder,
            FundTypeEncoder,
            FundManagementEncoder,
            FundCustodianEncoder,
            FundInvestTypeEncoder,
            ############### item number feature
            ItemBUY_COUNTS_30D,
            ItemAMOUNT_SUM_30D,
            ItemAMOUNT_AVG_30D,
            ItemAMOUNT_MIN_30D,
            ItemAMOUNT_MAX_30D,
        ]
        super(ItemEncoderFactory, self).__init__(source_table_name, version, self.encoder_cls)


user_feature_factory = UserEncoderFactory(USER_RAW_FEATURE_TABLE_NAME)
full_factory(user_feature_factory)
item_feature_factory = ItemEncoderFactory(ITEM_RAW_FEATURE_TABLE_NAME)
full_factory(item_feature_factory)


def to_array_string(array):
    result = ""
    for _ in array:
        result += str(_) + "|"
    return result[:-1]


def raw_user_feature_to_model_user_feature(user_raw_feature: dict) -> dict:
    """
    核心算法 负责将从特征平台拿到的原始特征转换模型需要的特征， 改方法和模型成对出现
    """
    if isinstance(user_raw_feature, str):
        user_raw_feature = json.loads(user_raw_feature)
    model_feature = {}  # copy.deepcopy(user_raw_feature)
    # feature in factory
    for feature_name in user_feature_factory.get_encoder_names():
        encoder = user_feature_factory.get_encoder(feature_name)
        raw_feature_value = user_raw_feature.get(feature_name, "")
        model_feature[feature_name] = encoder.get_model_feature_value(raw_feature_value)
    # category list feature
    for feature_name, feature_name_1 in [("u_buy_list", "item_id")]:
        encoder = item_feature_factory.get_encoder(feature_name_1)
        raw_feature_value = user_raw_feature.get(feature_name, "")
        if raw_feature_value in [None, "None", "null", "NULL"]:
            raw_feature_value = ""
        model_value = []
        for c in raw_feature_value.split("|"):
            model_value.append(encoder.get_model_feature_value(c))
        model_value = to_array_string(model_value)
        model_feature[feature_name] = model_value

    #  other feature
    for feature_name in user_raw_feature.keys():
        if feature_name not in model_feature and feature_name.lower() not in model_feature:
            model_feature[feature_name] = user_raw_feature[feature_name]
    # 保留 raw_id
    for _ in ["user_id",]:
        if _ in user_raw_feature:
            model_feature[_+"_raw"] = user_raw_feature[_]

    return model_feature


def raw_feature2model_feature(raw_feature_table_name, model_feature_table):
    raw_user_feature_dataframe = spark_client.get_session().sql(f"select * from {raw_feature_table_name}")
    # todo 改为调用 raw_user_feature_to_model_user_feature
    model_user_feature_rdd = raw_user_feature_dataframe.toJSON().map(raw_user_feature_to_model_user_feature)
    model_user_feature_dataframe = spark_client.get_session().createDataFrame(model_user_feature_rdd)
    model_user_feature_dataframe.write.format("hive").mode("overwrite").saveAsTable(model_feature_table)


# if __name__ == '__main__':
#     raw_feature2model_feature("algorithm.tmp_raw_user_feature_table_name",
#                               "algorithm.tmp_model_user_feature_table_name")

#!/usr/bin/env python3
# encoding: utf-8

import digitforce.aip.common.utils.component_helper as component_helper
from digitforce.aip.common.utils.argument_helper import df_argument_helper
# from raw_user_feature_to_model_user_feature import *


def run():
    # for test
    # import os
    # import json
    # os.environ["global_params"] = json.dumps(
    #     {"op_name": {"raw_user_feature_table_name": "algorithm.tmp_raw_user_feature_table_name",
    #     "model_user_feature_table_name": "algorithm.tmp_model_user_feature_table_name"}})
    # os.environ["name"] = "op_name"
    # 参数解析
    df_argument_helper.add_argument("--global_params", type=str, required=False, help="全局参数")
    df_argument_helper.add_argument("--name", type=str, required=False, help="name")
    df_argument_helper.add_argument("--raw_user_feature_table_name", type=str, required=False, help="原始特征")
    df_argument_helper.add_argument("--model_user_feature_table_name",
                                    default="algorithm.tmp_model_user_feature_table_name",
                                    type=str, required=False, help="模型的特征")

    # todo model_user_feature_table_name 的key 从组件中获取
    raw_user_feature_table_name = df_argument_helper.get_argument("raw_user_feature_table_name")
    model_user_feature_table_name = df_argument_helper.get_argument("model_user_feature_table_name")
    raw_feature2model_feature(raw_user_feature_table_name, model_user_feature_table_name)

    component_helper.write_output("model_user_feature_table_name", model_user_feature_table_name)


if __name__ == '__main__':
    run()
