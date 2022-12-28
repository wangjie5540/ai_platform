import copy

from pyspark.sql import functions as F
from pyspark.sql.types import *

from digitforce.aip.common.aip_feature.zq_feature import UserEncoderFactory
from digitforce.aip.common.utils.spark_helper import spark_client

user_encoder_factory = UserEncoderFactory("algorithm.tmp_raw_user_feature_table_name_1")


def to_array_string(array):
    result = ""
    for _ in array:
        result += str(_) + "|"
    return result[:-1]


def raw_user_feature_to_model_user_feature(user_raw_feature: dict) -> dict:
    model_user_feature = copy.deepcopy(user_raw_feature)
    # feature in factory
    for feature_name in user_encoder_factory.get_encoder_names():
        encoder = user_encoder_factory.get_encoder(feature_name)
        raw_feature_value = user_raw_feature.get(feature_name, "")
        model_user_feature[feature_name] = encoder.get_model_feature_value(raw_feature_value)
    # category list feature
    for feature_name in ["u_buy_list"]:
        encoder = user_encoder_factory.get_encoder(feature_name)
        raw_feature_value = user_raw_feature.get(feature_name, "")
        if raw_feature_value in [None, "None", "null", "NULL"]:
            raw_feature_value = ""
        model_value = []
        for c in raw_feature_value.split("|"):
            model_value.append(encoder.get_model_feature_value(c))
        model_value = to_array_string(model_value)
        model_user_feature[feature_name] = model_value

    # the feature is
    for feature_name in user_raw_feature.keys():
        if feature_name not in model_user_feature:
            model_user_feature[feature_name] = user_raw_feature[feature_name]

    return model_user_feature


def raw_feature2model_feature(raw_user_feature_table_name):
    raw_user_feature_dataframe = spark_client.get_session().sql(f"select * from {raw_user_feature_table_name}")
    raw_user_feature_dataframe.show()
    raw_user_feature_dataframe = raw_user_feature_dataframe.withColumnRenamed("user_id", "raw_user_id")
    raw_user_feature_dataframe.toJSON().map(raw_user_feature_to_model_user_feature).foreach(print)


if __name__ == '__main__':
    raw_feature2model_feature("algorithm.tmp_raw_user_feature_table_name_1")
