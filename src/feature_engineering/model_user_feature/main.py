#!/usr/bin/env python3
# encoding: utf-8

import logging
import os
import os.path
import pickle
import uuid
import copy
import json
import digitforce.aip.common.utils.component_helper as component_helper

# 初始化组件
component_helper.init_config()
from digitforce.aip.common.utils.argument_helper import df_argument_helper
from digitforce.aip.common.utils.spark_helper import SparkClient
from digitforce.aip.common.utils.hdfs_helper import hdfs_client
from digitforce.aip.common.utils.hive_helper import hive_client
from digitforce.aip.common.utils.time_helper import *

USER_RAW_FEATURE_TABLE_NAME = "algorithm.tmp_test_raw_user_feature"

ITEM_RAW_FEATURE_TABLE_NAME = "algorithm.tmp_test_raw_item_feature"


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
        self.default = float(self.default)
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
        tmp_file = f"tmp-{uuid.uuid4()}"
        hdfs_client.copy_to_local(hdfs_path, tmp_file)
        with open(tmp_file, "rb") as fi:
            pickle_encoder = pickle.load(fi)
        os.remove(tmp_file)
        return pickle_encoder

    @classmethod
    def load_encoder(cls, encoder: FeatureEncoder, use_hdfs=True):
        if hdfs_client.exists(cls.get_save_path(encoder)) and use_hdfs:
            encoder = cls.read_from_hdfs(encoder)
            return encoder
        logging.info(f"begin calculate feature encoder: {encoder.name} version:{encoder.version} "
                     f"source_table_name:{encoder.source_table_name}")
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


###################################################################################
class UserIdEncoder(CategoryFeatureEncoder):
    def __init__(self, source_table_name, version=get_today_str()):
        super().__init__("user_id", version, 0, source_table_name)


class UserSexEncoder(CategoryFeatureEncoder):
    def __init__(self, source_table_name, version=get_today_str()):
        super().__init__("sex", version, 0, source_table_name)


class UserEducationEncoder(CategoryFeatureEncoder):
    def __init__(self, source_table_name, version=get_today_str()):
        super().__init__("edu", version, 0, source_table_name)


class UserRiskLevelEncoder(CategoryFeatureEncoder):
    def __init__(self, source_table_name, version=get_today_str()):
        super().__init__("risk_level", version, 0, source_table_name)


class UserOCCUEncoder(CategoryFeatureEncoder):
    def __init__(self, source_table_name, version=get_today_str()):
        super().__init__("occu", version, 0, source_table_name)


class UserNATNEncoder(CategoryFeatureEncoder):
    def __init__(self, source_table_name, version=get_today_str()):
        super().__init__("natn", version, 0, source_table_name)


class UserISVAIIDINVSTEncoder(CategoryFeatureEncoder):
    def __init__(self, source_table_name, version=get_today_str()):
        super().__init__("is_vaiid_invst", version, 0, source_table_name)


class UserCityEncoder(CategoryFeatureEncoder):
    def __init__(self, source_table_name, version=get_today_str()):
        super().__init__("city_name", version, 0, source_table_name)


class UserProvinceEncoder(CategoryFeatureEncoder):
    def __init__(self, source_table_name, version=get_today_str()):
        super().__init__("province_name", version, 0, source_table_name)


class UserInvestTypeEncoder(CategoryFeatureEncoder):
    def __init__(self, source_table_name, version=get_today_str()):
        super().__init__("investor_type", version, 0, source_table_name)


# NumberFeatureEncoder
class UserAgeEncoder(NumberFeatureEncoder):
    def __init__(self, source_table_name, version=get_today_str()):
        super().__init__("age", version, 0, source_table_name)


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
class ItemIdEncoder(CategoryFeatureEncoder):
    def __init__(self, source_table_name, version=get_today_str()):
        super().__init__("item_id", version, 0, source_table_name)


class FundTypeEncoder(CategoryFeatureEncoder):
    def __init__(self, source_table_name, version=get_today_str()):
        super().__init__("fund_type", version, 0, source_table_name)


class ProductTypeEncoder(CategoryFeatureEncoder):
    def __init__(self, source_table_name, version=get_today_str()):
        super().__init__("product_type_pri", version, 0, source_table_name)


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

    def get_encoder(self, name) -> FeatureEncoder:
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
            UserSexEncoder,
            UserCityEncoder,
            UserProvinceEncoder,
            UserInvestTypeEncoder,
            ############ user number feature
            UserAgeEncoder,
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
        self.factory = {}
        self.encoder_cls = [
            ################ item category feature
            ItemIdEncoder,
            ProductTypeEncoder,
            ############### item number feature
            ItemBUY_COUNTS_30D,
            ItemAMOUNT_SUM_30D,
            ItemAMOUNT_AVG_30D,
            ItemAMOUNT_MIN_30D,
            ItemAMOUNT_MAX_30D,
        ]
        super(ItemEncoderFactory, self).__init__(source_table_name, version, self.encoder_cls)


user_feature_factory = UserEncoderFactory(USER_RAW_FEATURE_TABLE_NAME)
# TODO: 用户特征中用到itemEncoder的处理
item_feature_factory = ItemEncoderFactory(ITEM_RAW_FEATURE_TABLE_NAME)
full_factory(item_feature_factory)


def show_all_encoder():
    for f in [user_feature_factory, item_feature_factory]:
        for _ in f.get_encoder_names():
            e = f.get_encoder(_)
            if isinstance(e, CategoryFeatureEncoder):
                print(f"{_} voc size:{len(e.vocabulary)}")
            if isinstance(e, NumberFeatureEncoder):
                print(f"{_} std:{e.std} mean:{e.mean}")


def init_feature_encoder_factory(raw_user_feature_table=USER_RAW_FEATURE_TABLE_NAME,
                                 raw_item_feature_table=ITEM_RAW_FEATURE_TABLE_NAME):
    global user_feature_factory, item_feature_factory
    user_feature_factory = UserEncoderFactory(raw_user_feature_table)
    full_factory(user_feature_factory)
    item_feature_factory = ItemEncoderFactory(raw_item_feature_table)
    full_factory(item_feature_factory)
    return user_feature_factory, item_feature_factory


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
    for _ in ["user_id", ]:
        if _ in user_raw_feature:
            model_feature[_ + "_raw"] = user_raw_feature[_]

    return model_feature


def raw_feature2model_feature(raw_feature_table_name, model_feature_table):
    spark_client = SparkClient.get()
    global user_feature_factory
    user_feature_factory = UserEncoderFactory(raw_feature_table_name)
    full_factory(user_feature_factory)
    raw_user_feature_dataframe = spark_client.get_session().sql(f"select * from {raw_feature_table_name}")
    model_user_feature_rdd = raw_user_feature_dataframe.toJSON().map(raw_user_feature_to_model_user_feature)
    model_user_feature_dataframe = spark_client.get_session().createDataFrame(model_user_feature_rdd)
    model_user_feature_dataframe.write.format("hive").mode("overwrite").saveAsTable(model_feature_table)


def run():
    # for test
    # import os
    # import json
    # os.environ["global_params"] = json.dumps(
    #     {"op_name": {"raw_user_feature_table_name": "algorithm.tmp_test_raw_user_feature",
    #     "model_user_feature_table_name": "algorithm.tmp_model_user_feature_table_name"}})
    # os.environ["name"] = "op_name"
    # 参数解析
    df_argument_helper.add_argument("--global_params", type=str, required=False, help="全局参数")
    df_argument_helper.add_argument("--name", type=str, required=False, help="name")
    df_argument_helper.add_argument("--raw_user_feature_table_name", type=str, required=False, help="")
    df_argument_helper.add_argument("--raw_item_feature_table_name", type=str, required=False, help="")
    df_argument_helper.add_argument("--model_user_feature_table_name",
                                    default="algorithm.tmp_model_user_feature_table_name",
                                    type=str, required=False, help="模型的特征")

    # todo model_user_feature_table_name 的key 从组件中获取
    raw_user_feature_table_name = df_argument_helper.get_argument("raw_user_feature_table_name")
    raw_item_feature_table_name = df_argument_helper.get_argument("raw_item_feature_table_name")
    model_user_feature_table_name = df_argument_helper.get_argument("model_user_feature_table_name")
    print(f"raw_user_feature_table_name:{raw_user_feature_table_name} "
          f"model_user_feature_table_name:{model_user_feature_table_name} "
          f"raw_item_feature_table_name:{raw_item_feature_table_name}")
    raw_feature2model_feature(raw_user_feature_table_name, model_user_feature_table_name)
    component_helper.write_output("model_user_feature_table_name", model_user_feature_table_name)
    print()


if __name__ == '__main__':
    run()
