from digitforce.aip.common.aip_feature.feature import CategoryFeatureEncoder
from digitforce.aip.common.utils.time_helper import *

USER_RAW_FEATURE_TABLE_NAME = "algorithm.tmp_raw_user_feature_table_name_1"

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
        super().__init__("EDU", version, 0, source_table_name)


class UserRSKENDRCPYEncoder(CategoryFeatureEncoder):
    def __init__(self, source_table_name, version=get_today_str()):
        super().__init__("RSK_ENDR_CPY", version, 0, source_table_name)


class UserOCCUEncoder(CategoryFeatureEncoder):
    def __init__(self, source_table_name, version=get_today_str()):
        super().__init__("OCCU", version, 0, source_table_name)


class UserNATNEncoder(CategoryFeatureEncoder):
    def __init__(self, source_table_name, version=get_today_str()):
        super().__init__("NATN", version, 0, source_table_name)


class UserISVAIIDINVSTEncoder(CategoryFeatureEncoder):
    def __init__(self, source_table_name, version=get_today_str()):
        super().__init__("IS_VAIID_INVST", version, 0, source_table_name)


class UserEncoderFactory:
    def __init__(self, source_table_name, version=get_today_str()):
        self._factory = {}
        for _ in [UserIdEncoder, UserGenderEncoder, UserEducationEncoder, UserRSKENDRCPYEncoder, UserOCCUEncoder,
                  UserNATNEncoder, UserISVAIIDINVSTEncoder]:  # todo add all userFeatureEncoder
            encoder = _(source_table_name, version)
            self._factory[encoder.name] = encoder

    def get_encoder(self, name):
        return self._factory.get(name, None)

    def get_encoder_names(self):
        return self._factory.keys()


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


class ItemEncoderFactory:
    def __init__(self, source_table_name, version=get_today_str()):
        self.factory = {}
        for _ in [ItemIdEncoder, FundTypeEncoder, FundManagementEncoder,
                  FundCustodianEncoder, FundInvestTypeEncoder,
                  ]:  # todo add all userFeatureEncoder
            encoder = _(source_table_name, version)
            self.factory[encoder.name] = encoder

    def get_encoder(self, name):
        return self.factory.get(name, None)


def main():
    user_feature_encoder = UserIdEncoder(USER_RAW_FEATURE_TABLE_NAME)
    item_feature_encoder = ItemIdEncoder(ITEM_RAW_FEATURE_TABLE_NAME)
    user_feature_encoder.generate_vocabulary()
    item_feature_encoder.generate_vocabulary()

    user_feature_encoder.save_to_hdfs()
    item_feature_encoder.save_to_hdfs()
    user_feature_encoder.read_from_hdfs()
    item_feature_encoder.read_from_hdfs()
    print(user_feature_encoder.get_model_feature_value("AAA"))
    print(user_feature_encoder.get_model_feature_value("7080"))
    print(item_feature_encoder.get_model_feature_value("7080"))
    print(item_feature_encoder.get_model_feature_value("3006"))



if __name__ == '__main__':
    main()
