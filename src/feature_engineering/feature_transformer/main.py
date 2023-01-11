# coding: utf-8
import argparse
import json
import transformer
from digitforce.aip.components.feature_engineering import *
import digitforce.aip.common.utils.component_helper as component_helper


def run():
    # 参数解析
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', type=str, required=True, help='模式')
    parser.add_argument('--name', type=str, required=True, help='名称')
    parser.add_argument('--global_params', type=str, required=True, help='全局参数')
    parser.add_argument('--table_name', type=str, required=False, help='表名')
    parser.add_argument('--pipeline_model', type=str, required=False, help='pipeline_model')
    parser.add_argument('--transformers', type=str, required=False, help='transformers')
    args = parser.parse_args()
    global_params = json.loads(args.global_params)
    component_params = global_params[args.name]
    if args.mode == 'create':
        pipeline_model_path, transformers_path = transformer.create(
            table_name=args.table_name, transform_rules=component_params['transform_rules'])
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
    elif args.mode == 'do_transform':
        pipeline_model_path = json.loads(args.pipeline_model)['path']
        transformers_path = json.loads(args.transformers)['path']
        save_table_name = transformer.do_transform(
            table_name=component_params['table_name'],
            transformers=component_params['transformers'],
            pipeline_model_path=pipeline_model_path,
            transformers_path=transformers_path
        )
        params = {
            'type': 'hive_table',
            'table_name': save_table_name,
            'column_list': []
        }
        # component_helper.write_output(FeatureTransformerOp.OUTPUT_FEATURE_TABLE, params)
        component_helper.write_output('feature_table', params, need_json_dump=True)


if __name__ == '__main__':
    run()
