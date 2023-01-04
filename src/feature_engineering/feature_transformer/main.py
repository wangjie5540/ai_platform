# coding: utf-8
import argparse
import json
import transformer
from digitforce.aip.components.feature_engineering import *
import digitforce.aip.common.utils.component_helper as component_helper


def run():
    # 参数解析
    parser = argparse.ArgumentParser()
    parser.add_argument('--global_params', type=str, required=True, help='全局参数')
    parser.add_argument('--name', type=str, required=True, help='名称')
    parser.add_argument('--table_name', type=str, required=True, help='表名')
    args = parser.parse_args()
    global_params = json.loads(args.global_params)
    component_params = global_params[args.name]
    create_params = component_params.get('create', None)
    if create_params is not None:
        pipeline_model_path, transformers_path = transformer.create(table_name=args.table_name,
                                                                    transform_rules=create_params['transform_rules'])
        params = {
            'type': 'hdfs_file',
            'path': pipeline_model_path
        }
        # component_helper.write_output(FeatureTransformerOp.OUTPUT_PIPELINE_MODEL, params)
        component_helper.write_output('pipeline_model', params, need_json_dump=True)
        params = {
            'type': 'hdfs_file',
            'path': transformers_path
        }
        # component_helper.write_output(FeatureTransformerOp.OUTPUT_TRANSFORMERS, params)
        component_helper.write_output('transformers', params, need_json_dump=True)
    transform_params = component_params.get('transform', None)
    if transform_params is not None:
        save_table_name = transformer.transform(table_name=transform_params['table_name'],
                                                transformers=transform_params['transformers'],
                                                name=transform_params['name'])
        params = {
            'type': 'hive_table',
            'table_name': save_table_name,
            'column_list': []
        }
        # component_helper.write_output(FeatureTransformerOp.OUTPUT_FEATURE_TABLE, params)
        component_helper.write_output('feature_table', params, need_json_dump=True)


if __name__ == '__main__':
    run()
