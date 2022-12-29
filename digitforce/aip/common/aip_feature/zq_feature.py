from digitforce.aip.common.aip_feature.feature import *
from digitforce.aip.common.utils.time_helper import *

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
            if encoder.std and encoder.mean:
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
        self.factory = {}
        # todo add all itemFeatureEncoder
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


def main():
    pass


if __name__ == '__main__':
    main()
