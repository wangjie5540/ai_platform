# coding: utf-8
import kfp.dsl as dsl
import digitforce.aip.common.utils.component_helper as component_helper

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

class FeatureCreateGaoqian(dsl.ContainerOp):
    """
    特征构造组件
    """

    def __init__(self, name, global_params, sample):
        super(FeatureCreateGaoqian, self).__init__(
            name=name,
            image=f'digit-force-docker.pkg.coding.net/ai-platform/ai-components/feature_engineering-feature_create_gaoqian',
            command=['python', 'main.py'],
            arguments=['--name', name, '--global_params', global_params, '--sample', sample],
            file_outputs={
                user_feature_name: component_helper.generate_output_path(user_feature_name)
            }
        )
