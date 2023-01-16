# coding: utf-8
from digitforce.aip.common.utils.spark_helper import spark_session
from pyspark.ml.feature import StandardScaler, StringIndexer, VectorAssembler
from pyspark.ml import Pipeline, PipelineModel
import digitforce.aip.common.utils.config_helper as config_helper
import digitforce.aip.common.utils.hdfs_helper as hdfs_helper
import digitforce.aip.common.utils.id_helper as id_helper
import json

"""
# 参考：https://spark.apache.org/docs/3.3.1/api/python/reference/api/pyspark.ml.feature.StringIndexer.html
"""


def create(table_name, transform_rules):
    """
    创建转换器
    :return:
    """
    stages = list()
    transformer_dict = dict()
    for i in range(len(transform_rules)):
        rule = transform_rules[i]
        if rule['type'] == 'string_indexer':
            indexer = StringIndexer(inputCol=rule['input_col'], outputCol=rule['output_col'], handleInvalid='keep')
            stages.append(indexer)
            l = list()
            l.append(len(stages) - 1)
            transformer_dict[rule['input_col']] = l
        elif rule['type'] == 'standard_scaler':
            l = list()
            vector_assembler = VectorAssembler(
                inputCols=[rule['input_col']], outputCol=rule['input_col'] + '_vector', handleInvalid='skip')
            stages.append(vector_assembler)
            l.append(len(stages) - 1)
            scaler = StandardScaler(inputCol=rule['input_col'] + '_vector', outputCol=rule['output_col'])
            stages.append(scaler)
            l.append(len(stages) - 1)
            transformer_dict[rule['input_col']] = l
    df = spark_session.sql(f"select * from {table_name}")
    pipeline_model = Pipeline(stages=stages).fit(df)
    save_name = f'feature_transformer_{id_helper.gen_uniq_id()}'
    pipeline_model_save_path = f"{config_helper.get_module_config('hdfs')['base_url']}/user/ai/aip/feature_engineering/{save_name}"
    pipeline_model.save(pipeline_model_save_path)
    transformers_save_path = f"/user/ai/aip/feature_engineering/{save_name}/transformers"
    hdfs_helper.hdfs_client.hdfs_client.create(transformers_save_path, json.dumps(transformer_dict))
    print(f"pipeline_model_save_path: {pipeline_model_save_path}")
    print(f"transformers_save_path: {transformers_save_path}")
    return pipeline_model_save_path, transformers_save_path


def do_transform(table_name, transformers, pipeline_model_path, transformers_path):
    """
    使用已有的编码器进行特征编码
    :param table_name: 表名
    :param transformers: 编码规则
    :param pipeline_model_path: pipeline_model的路径
    :param transformers_path: transformers的路径
    :return: 存储的表名
    """
    df = spark_session.sql(f"select * from {table_name}")
    with hdfs_helper.hdfs_client.hdfs_client.open(transformers_path) as f:
        transformer_dict = json.loads(f.read())
    pipeline_model = PipelineModel.load(pipeline_model_path)
    stages = list()
    for transformer_name in transformers:
        stage_indexes = transformer_dict[transformer_name]
        stages.extend([pipeline_model.stages[i] for i in stage_indexes])
    save_table_name = f"algorithm.feature_transformer_{id_helper.gen_uniq_id()}"
    PipelineModel(stages=stages).transform(df).write.saveAsTable(save_table_name, mode='overwrite')
    print(f"save_table_name: {save_table_name}")
    return save_table_name
