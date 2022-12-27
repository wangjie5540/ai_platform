# coding: utf-8
import kfp.dsl as dsl

import digitforce.aip.common.utils.component_helper as component_helper
from digitforce.aip.components.base_component import BaseComponent

user_feature_name = "user_feature"
item_feature_name = "item_feature"


class FeatureCreateLookalike(dsl.ContainerOp):
    """
    数据源-读取表组件
    """

    def __init__(self, name, global_params, sample):
        super(FeatureCreateLookalike, self).__init__(
            name=name,
            image=f'digit-force-docker.pkg.coding.net/ai-platform/ai-components/feature_engineering-feature_create_lookalike',
            command=['python', 'main.py'],
            arguments=['--name', name, '--global_params', global_params, '--sample', sample],
            file_outputs={
                user_feature_name: component_helper.generate_output_path(user_feature_name),
                item_feature_name: component_helper.generate_output_path(item_feature_name)
            }
        )


class RawUserFeatureCalculateOp(dsl.ContainerOp, BaseComponent):
    def __init__(self, name, global_params, raw_user_feature_table_name):
        super(RawUserFeatureCalculateOp, self).__init__(
            name=name,
            image=f'digit-force-docker.pkg.coding.net/'
                  f'ai-platform/ai-components/feature_engineering-raw_user_feature',
            command=['python', 'main.py'],
            arguments=['--raw_user_feature_table_name', raw_user_feature_table_name],
            file_outputs={
                raw_user_feature_table_name: component_helper.generate_output_path(raw_user_feature_table_name)
            }
        )
        super(RawUserFeatureCalculateOp, self).init_aip_container()
        super(RawUserFeatureCalculateOp, self).add_env_variable("global_params", global_params)
        super(RawUserFeatureCalculateOp, self).add_env_variable("name", name)


class RawItemFeatureCalculateOp(dsl.ContainerOp, BaseComponent):
    def __init__(self, name, global_params, raw_item_feature_table_name):
        super(RawItemFeatureCalculateOp, self).__init__(
            name=name,
            image=f'digit-force-docker.pkg.coding.net/'
                  f'ai-platform/ai-components/feature_engineering-raw_item_feature',
            command=['python', 'main.py'],
            arguments=['--raw_item_feature_table_name', raw_item_feature_table_name],
            file_outputs={
                raw_item_feature_table_name: component_helper.generate_output_path(raw_item_feature_table_name)
            }
        )
        super(RawItemFeatureCalculateOp, self).init_aip_container()
        super(RawItemFeatureCalculateOp, self).add_env_variable("global_params", global_params)
        super(RawItemFeatureCalculateOp, self).add_env_variable("name", name)
