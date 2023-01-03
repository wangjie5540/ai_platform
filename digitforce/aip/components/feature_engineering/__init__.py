# coding: utf-8
import kfp.dsl as dsl

import digitforce.aip.common.utils.component_helper as component_helper


class FeatureCreateLookalike(dsl.ContainerOp):
    OUTPUT_USER_FEATURE = 'user_feature'
    OUTPUT_ITEM_FEATURE = 'item_feature'

    def __init__(self, name, global_params, sample):
        super(FeatureCreateLookalike, self).__init__(
            name=name,
            image=f'digit-force-docker.pkg.coding.net/ai-platform/ai-components/feature_engineering-feature_create_lookalike',
            command=['python', 'main.py'],
            arguments=['--name', name, '--global_params', global_params, '--sample', sample],
            file_outputs={
                self.OUTPUT_USER_FEATURE: component_helper.generate_output_path(self.OUTPUT_USER_FEATURE),
                self.OUTPUT_ITEM_FEATURE: component_helper.generate_output_path(self.OUTPUT_ITEM_FEATURE)
            }
        )


class RawUserFeatureOp(dsl.ContainerOp):
    OUTPUT_RAW_USER_FEATURE = 'raw_user_feature'

    def __init__(self, name, global_params):
        super(RawUserFeatureOp, self).__init__(
            name=name,
            image=f'digit-force-docker.pkg.coding.net/'
                  f'ai-platform/ai-components/feature_engineering-raw_user_feature',
            arguments=[
                '--name', name,
                '--global_params', global_params,
            ],
            file_outputs={
                self.OUTPUT_RAW_USER_FEATURE: component_helper.generate_output_path(self.OUTPUT_RAW_USER_FEATURE)
            }
        )


class RawItemFeatureOp(dsl.ContainerOp):
    OUTPUT_RAW_ITEM_FEATURE = 'raw_item_feature'

    def __init__(self, name, global_params):
        super(RawItemFeatureOp, self).__init__(
            name=name,
            image=f'digit-force-docker.pkg.coding.net/'
                  f'ai-platform/ai-components/feature_engineering-raw_item_feature',
            command=['python', 'main.py'],
            arguments=['--name', name, '--global_params', global_params],
            file_outputs={
                self.OUTPUT_RAW_ITEM_FEATURE: component_helper.generate_output_path(self.OUTPUT_RAW_ITEM_FEATURE)
            }
        )


class FeatureTransformerOp(dsl.ContainerOp):
    OUTPUT_PIPELINE_MODEL = 'pipeline_model'
    OUTPUT_TRANSFORMERS = 'transformers'
    OUTPUT_FEATURE_TABLE = 'feature_table'

    def __init__(self, name, global_params):
        super(FeatureTransformerOp, self).__init__(
            name=name,
            image=f'digit-force-docker.pkg.coding.net/'
                  f'ai-platform/ai-components/feature_engineering-feature_transformer',
            command=['python', 'main.py'],
            arguments=['--name', name, '--global_params', global_params],
            file_outputs={
                self.OUTPUT_PIPELINE_MODEL: component_helper.generate_output_path(self.OUTPUT_PIPELINE_MODEL),
                self.OUTPUT_TRANSFORMERS: component_helper.generate_output_path(self.OUTPUT_TRANSFORMERS),
                self.OUTPUT_FEATURE_TABLE: component_helper.generate_output_path(self.OUTPUT_FEATURE_TABLE)
            }
        )
