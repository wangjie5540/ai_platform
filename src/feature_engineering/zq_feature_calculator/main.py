from digitforce.aip.common.aip_feature.zq_feature import *
from digitforce.aip.common.utils.argument_helper import df_argument_helper


def main():
    import os
    import json
    os.environ["global_params"] = json.dumps(
        {"zq_feature": {"raw_user_feature_table_name": "algorithm.tmp_raw_user_feature_table_name",
                        "raw_item_feature_table_name": "algorithm.tmp_raw_item_feature_table_name"}})
    os.environ["name"] = "raw_user_feature"
    # 参数解析
    df_argument_helper.add_argument("--global_params", type=str, required=False, help="全局参数")
    df_argument_helper.add_argument("--name", type=str, required=False, help="name")
    df_argument_helper.add_argument("--raw_user_feature_table_name",
                                    default="algorithm.tmp_raw_user_feature_table_name",
                                    type=str, required=False, help="")
    df_argument_helper.add_argument("--raw_item_feature_table_name",
                                    default="algorithm.tmp_raw_item_feature_table_name",
                                    type=str, required=False, help="")

    raw_user_feature_table_name = df_argument_helper.get_argument("raw_user_feature_table_name")
    raw_item_feature_table_name = df_argument_helper.get_argument("raw_item_feature_table_name")
    print(f"raw_user_feature_table_name:{raw_user_feature_table_name}")
    print(f"raw_item_feature_table_name:{raw_item_feature_table_name}")
    init_feature_encoder_factory(raw_user_feature_table_name, raw_item_feature_table_name)
    show_all_encoder()


if __name__ == '__main__':
    main()
