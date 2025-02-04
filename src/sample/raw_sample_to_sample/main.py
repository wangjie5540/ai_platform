#!/usr/bin/env python3
# encoding: utf-8

import glob
import json
import logging
import os
import os.path
import pickle

import digitforce.aip.common.utils.component_helper as component_helper
from digitforce.aip.common.utils.argument_helper import df_argument_helper

# 初始化组件
component_helper.init_config()
from digitforce.aip.common.utils.hive_helper import hive_client
from digitforce.aip.common.utils.hdfs_helper import hdfs_client
from digitforce.aip.common.utils.time_helper import *
from digitforce.aip.common.utils.spark_helper import SparkClient

import copy


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


USER_RAW_FEATURE_TABLE_NAME = "algorithm.tmp_test_raw_user_feature"

ITEM_RAW_FEATURE_TABLE_NAME = "algorithm.tmp_test_raw_item_feature"


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
full_factory(user_feature_factory)
item_feature_factory = ItemEncoderFactory(ITEM_RAW_FEATURE_TABLE_NAME)
full_factory(item_feature_factory)


def model_sample_map_fn(raw_sample):
    if isinstance(raw_sample, str):
        raw_sample = json.loads(raw_sample)

    model_sample = copy.deepcopy(raw_sample)
    for feature_name in ["user_id", ]:
        if feature_name in model_sample:
            encoder = user_feature_factory.get_encoder(feature_name)
            raw_feature = raw_sample[feature_name]
            model_sample[feature_name] = encoder.get_model_feature_value(raw_feature)
    for feature_name in ["item_id", ]:
        if feature_name in model_sample:
            encoder = item_feature_factory.get_encoder(feature_name)
            raw_feature = raw_sample[feature_name]
            model_sample[feature_name] = encoder.get_model_feature_value(raw_feature)
    return model_sample


def raw_sample_to_sample(raw_sample_table_name, sample_table_name):
    spark_client = SparkClient.get()
    raw_sample_dataframe = spark_client.get_session().sql(f"select * from {raw_sample_table_name}")
    model_sample_rdd = raw_sample_dataframe.toJSON().map(model_sample_map_fn)
    model_sample_dataframe = spark_client.get_session().createDataFrame(model_sample_rdd)
    model_sample_dataframe.write.format("hive").mode("overwrite").saveAsTable(sample_table_name)
    return sample_table_name


# def main():
#     raw_sample_to_sample("algorithm.tmp_aip_sample", "algorithm.tmp_aip_model_sample")
#

def run():
    # for test
    # import os
    # import json
    # os.environ["global_params"] = json.dumps(
    #     {"op_name": {
    #         "raw_sample_table_name": "algorithm.tmp_aip_sample",
    #         "model_sample_table_name": "algorithm.tmp_aip_model_sample",
    #     }})
    # os.environ["name"] = "op_name"
    # 参数解析
    df_argument_helper.add_argument("--global_params", type=str, required=False, help="全局参数")
    df_argument_helper.add_argument("--name", type=str, required=False, help="name")
    df_argument_helper.add_argument("--raw_sample_table_name", type=str, required=False, help="样本数据")
    df_argument_helper.add_argument("--model_sample_table_name", type=str, required=False,
                                    default="algorithm.tmp_aip_model_sample", help="样本数据")

    raw_sample_table_name = df_argument_helper.get_argument("raw_sample_table_name")
    model_sample_table_name = df_argument_helper.get_argument("model_sample_table_name")
    # todo for test
    if not model_sample_table_name:
        model_sample_table_name = "algorithm.tmp_aip_model_sample"
    print(f"raw_sample_table_name:{raw_sample_table_name}, model_sample_table_name:{model_sample_table_name}")
    model_sample_table_name = raw_sample_to_sample(raw_sample_table_name, model_sample_table_name)

    component_helper.write_output("model_sample_table_name", model_sample_table_name)


if __name__ == '__main__':
    run()
