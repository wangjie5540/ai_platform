# coding: utf-8
import kfp.dsl as dsl

import digitforce.aip.common.utils.component_helper as component_helper
from digitforce.aip.components import BaseComponent
import digitforce.aip.common.constants.global_constant as global_constant


class FeatureCreateLookalike(BaseComponent):
    OUTPUT_USER_FEATURE = 'user_feature'
    OUTPUT_ITEM_FEATURE = 'item_feature'

    def __init__(self, name, global_params, sample, tag='latest'):
        super().__init__(
            image=f'{global_constant.AI_PLATFORM_IMAGE_REPO_V2}/feature_engineering-feature_create_lookalike',
            name=name,
            arguments=['--name', name, '--global_params',
                       global_params, '--sample', sample],
            tag=tag,
            file_outputs={
                self.OUTPUT_USER_FEATURE: component_helper.generate_output_path(self.OUTPUT_USER_FEATURE),
                self.OUTPUT_ITEM_FEATURE: component_helper.generate_output_path(
                    self.OUTPUT_ITEM_FEATURE)
            }
        )


class FeatureCreateLiushi(BaseComponent):
    OUTPUT_TRAIN_FEATURE = 'train_feature_table_name'
    OUTPUT_TEST_FEATURE = 'test_feature_table_name'

    def __init__(self, name, global_params, sample, tag='latest'):
        super().__init__(
            name=name,
            image=f'{global_constant.AI_PLATFORM_IMAGE_REPO}/feature_engineering-feature_create_liushi',
            arguments=['--name', name, '--global_params',
                       global_params, '--sample', sample],
            tag=tag,
            file_outputs={
                self.OUTPUT_TRAIN_FEATURE: component_helper.generate_output_path(self.OUTPUT_TRAIN_FEATURE),
                self.OUTPUT_TEST_FEATURE: component_helper.generate_output_path(
                    self.OUTPUT_TEST_FEATURE)
            }
        )


class FeatureCreateLiushiPredict(BaseComponent):
    OUTPUT_PREDICT_FEATURE = 'predict_feature_table_name'

    def __init__(self, name, global_params, sample, tag='latest'):
        super().__init__(
            name=name,
            image=f'{global_constant.AI_PLATFORM_IMAGE_REPO}/feature_engineering-feature_create_liushi_predict',
            arguments=['--name', name, '--global_params', global_params,
                       '--sample', sample],
            tag=tag,
            file_outputs={
                self.OUTPUT_PREDICT_FEATURE: component_helper.generate_output_path(self.OUTPUT_PREDICT_FEATURE),

            }
        )


class FeatureCreateGaoqian(BaseComponent):
    OUTPUT_TRAIN_FEATURE = 'train_feature_table_name'
    OUTPUT_TEST_FEATURE = 'test_feature_table_name'

    def __init__(self, name, global_params, sample, tag='latest'):
        super(FeatureCreateGaoqian, self).__init__(
            name=name,
            image=f'{global_constant.AI_PLATFORM_IMAGE_REPO_V2}/'
                  f'feature_engineering-feature_create_gaoqian',
            tag=tag,
            arguments=['--name', name, '--global_params',
                       global_params, '--sample', sample],
            file_outputs={
                self.OUTPUT_TRAIN_FEATURE: component_helper.generate_output_path(self.OUTPUT_TRAIN_FEATURE),
                self.OUTPUT_TEST_FEATURE: component_helper.generate_output_path(
                    self.OUTPUT_TEST_FEATURE)
            }
        )


class FeatureCreateGaoqianPredict(BaseComponent):
    OUTPUT_PREDICT_FEATURE = 'predict_feature_table_name'

    def __init__(self, name, global_params, sample, tag='latest'):
        super(FeatureCreateGaoqianPredict, self).__init__(
            name=name,
            image=f'{global_constant.AI_PLATFORM_IMAGE_REPO_V2}/'
                  f'feature_engineering-feature_create_gaoqian_predict',
            tag=tag,
            arguments=['--name', name, '--global_params', global_params,
                       '--sample', sample],
            file_outputs={
                self.OUTPUT_PREDICT_FEATURE: component_helper.generate_output_path(self.OUTPUT_PREDICT_FEATURE),

            }
        )


class RawUserFeatureOp(BaseComponent):
    OUTPUT_KEY_RAW_USER_FEATURE = 'raw_user_feature'

    def __init__(self, name, global_params, tag='latest'):
        super().__init__(
            name=name,
            image=f'{global_constant.AI_PLATFORM_IMAGE_REPO_V2}/feature_engineering-raw_user_feature',
            arguments=[
                '--name', name,
                '--global_params', global_params,
            ],
            tag=tag,
            file_outputs={
                self.OUTPUT_KEY_RAW_USER_FEATURE: component_helper.generate_output_path(
                    self.OUTPUT_KEY_RAW_USER_FEATURE)
            }
        )


class RawItemFeatureOp(BaseComponent):
    OUTPUT_KEY_RAW_ITEM_FEATURE = 'raw_item_feature'

    def __init__(self, name, global_params, tag='latest'):
        super().__init__(
            name=name,
            image=f'{global_constant.AI_PLATFORM_IMAGE_REPO_V2}/feature_engineering-raw_item_feature',
            arguments=['--name', name, '--global_params', global_params],
            tag=tag,
            file_outputs={
                self.OUTPUT_KEY_RAW_ITEM_FEATURE: component_helper.generate_output_path(
                    self.OUTPUT_KEY_RAW_ITEM_FEATURE)
            }
        )


class ModelUserFeatureOp(BaseComponent):
    OUTPUT_KEY_MODEL_USER_FEATURE = 'model_user_feature_table_name'

    def __init__(self, name, global_params, raw_user_feature_table, raw_item_feature_table, tag='latest'):
        super().__init__(
            name=name,
            image=f'{global_constant.AI_PLATFORM_IMAGE_REPO_V2}/feature_engineering-model_user_feature',
            arguments=['--name', name, '--global_params', global_params,
                       '--raw_user_feature_table_name', raw_user_feature_table,
                       '--raw_item_feature_table_name', raw_item_feature_table,
                       ],
            tag=tag,
            file_outputs={
                self.OUTPUT_KEY_MODEL_USER_FEATURE: component_helper.generate_output_path(
                    self.OUTPUT_KEY_MODEL_USER_FEATURE)
            }
        )


class ZqFeatureEncoderCalculator(BaseComponent):
    def __init__(self, name, global_params, raw_user_feature_table, raw_item_feature_table, tag='latest'):
        super().__init__(
            name=name,
            image=f'{global_constant.AI_PLATFORM_IMAGE_REPO_V2}/feature_engineering-zq_feature_calculator',
            arguments=['--name', name, '--global_params', global_params,
                       '--raw_user_feature_table_name', raw_user_feature_table,
                       '--raw_item_feature_table_name', raw_item_feature_table,
                       ],
            tag=tag,
        )


class ModelItemFeatureOp(BaseComponent):
    OUTPUT_KEY_RAW_ITEM_FEATURE = 'model_item_feature_table_name'

    def __init__(self, name, global_params, raw_item_feature_table, tag='latest'):
        super().__init__(
            name=name,
            image=f'{global_constant.AI_PLATFORM_IMAGE_REPO_V2}/feature_engineering-model_item_feature',
            arguments=['--name', name, '--global_params', global_params,
                       '--raw_item_feature_table_name', raw_item_feature_table,
                       ],
            tag=tag,
            file_outputs={
                self.OUTPUT_KEY_RAW_ITEM_FEATURE: component_helper.generate_output_path(
                    self.OUTPUT_KEY_RAW_ITEM_FEATURE)
            }
        )


class FeatureTransformerOp(dsl.ContainerOp):
    OUTPUT_PIPELINE_MODEL = 'pipeline_model'
    OUTPUT_TRANSFORMERS = 'transformers'

    # OUTPUT_FEATURE_TABLE = 'feature_table'

    def __init__(self, name, global_params, raw_user_feature_table, tag="latest"):
        super(FeatureTransformerOp, self).__init__(
            name=name,
            image=f'digit-force-docker.pkg.coding.net/'
                  f'ai-platform/ai-components/feature_engineering-feature_transformer:{tag}',
            command=['python', 'main.py'],
            arguments=['--name', name, '--global_params',
                       global_params, '--table_name', raw_user_feature_table],
            file_outputs={
                self.OUTPUT_PIPELINE_MODEL: component_helper.generate_output_path(self.OUTPUT_PIPELINE_MODEL),
                self.OUTPUT_TRANSFORMERS: component_helper.generate_output_path(self.OUTPUT_TRANSFORMERS),
                # self.OUTPUT_FEATURE_TABLE: component_helper.generate_output_path(self.OUTPUT_FEATURE_TABLE)
            }
        )


class FeatureCreateDixiaohu(BaseComponent):
    OUTPUT_TRAIN_FEATURE = 'train_feature_table_name'
    OUTPUT_TEST_FEATURE = 'test_feature_table_name'

    def __init__(self, name, global_params, sample, tag='latest'):
        super().__init__(
            name=name,
            image=f'{global_constant.AI_PLATFORM_IMAGE_REPO_V2}/feature_engineering-feature_create_dixiaohu',
            arguments=['--name', name, '--global_params',
                       global_params, '--sample', sample],
            tag=tag,
            file_outputs={
                self.OUTPUT_TRAIN_FEATURE: component_helper.generate_output_path(self.OUTPUT_TRAIN_FEATURE),
                self.OUTPUT_TEST_FEATURE: component_helper.generate_output_path(
                    self.OUTPUT_TEST_FEATURE)
            }
        )


class FeatureCreateDixiaohuPredict(BaseComponent):
    OUTPUT_PREDICT_FEATURE = 'predict_feature_table_name'

    def __init__(self, name, global_params, sample, tag='latest'):
        super().__init__(
            name=name,
            image=f'{global_constant.AI_PLATFORM_IMAGE_REPO_V2}/feature_engineering-feature_create_dixiaohu_predict',
            arguments=['--name', name, '--global_params', global_params,
                       '--sample', sample],
            tag=tag,
            file_outputs={
                self.OUTPUT_PREDICT_FEATURE: component_helper.generate_output_path(self.OUTPUT_PREDICT_FEATURE),

            }
        )
