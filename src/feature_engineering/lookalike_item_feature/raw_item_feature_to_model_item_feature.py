import copy

from digitforce.aip.common.aip_feature.zq_feature import ItemEncoderFactory
from digitforce.aip.common.utils.spark_helper import spark_client

item_encoder_factory = ItemEncoderFactory("algorithm.tmp_raw_user_feature_table_name_1")


def to_array_string(array):
    result = ""
    for _ in array:
        result += str(_) + "|"
    return result[:-1]


def raw_item_feature_to_model_item_feature(user_raw_feature: dict) -> dict:
    """
    核心算法 负责将从特征平台拿到的原始特征转换模型需要的特征， 改方法和模型成对出现
    """
    model_feature = copy.deepcopy(user_raw_feature)
    # feature in factory
    for feature_name in item_encoder_factory.get_encoder_names():
        encoder = item_encoder_factory.get_encoder(feature_name)
        raw_feature_value = user_raw_feature.get(feature_name, "")
        model_feature[feature_name] = encoder.get_model_feature_value(raw_feature_value)
    # # category list feature
    # for feature_name in ["u_buy_list"]:
    #     encoder = item_encoder_factory.get_encoder(feature_name)
    #     raw_feature_value = user_raw_feature.get(feature_name, "")
    #     if raw_feature_value in [None, "None", "null", "NULL"]:
    #         raw_feature_value = ""
    #     model_value = []
    #     for c in raw_feature_value.split("|"):
    #         model_value.append(encoder.get_model_feature_value(c))
    #     model_value = to_array_string(model_value)
    #     model_user_feature[feature_name] = model_value

    # the other feature
    for feature_name in user_raw_feature.keys():
        if feature_name not in model_feature:
            model_feature[feature_name] = user_raw_feature[feature_name]

    return model_feature


def raw_feature2model_feature(raw_feature_table_name, model_feature_table):
    raw_user_feature_dataframe = spark_client.get_session().sql(f"select * from {raw_feature_table_name}")
    # todo 改为调用 raw_user_feature_to_model_user_feature
    model_user_feature_rdd = raw_user_feature_dataframe.toJSON().map(__tmp)
    model_user_feature_dataframe = spark_client.get_session().createDataFrame(model_user_feature_rdd)
    model_user_feature_dataframe.write.format("hive").mode("overwrite").saveAsTable(model_feature_table)


if __name__ == '__main__':
    raw_feature2model_feature("algorithm.tmp_raw_item_feature_table_name",
                              "algorithm.tmp_model_item_feature_table_name")
