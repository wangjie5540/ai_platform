# coding: utf-8
from digitforce.aip.common.utils.spark_helper import spark_session
from pyspark.ml.feature import StandardScaler, StringIndexer, VectorAssembler
from pyspark.ml import Pipeline, PipelineModel
import digitforce.aip.common.utils.config_helper as config_helper
import digitforce.aip.common.utils.hdfs_helper as hdfs_helper
import json

"""
# 参考：https://spark.apache.org/docs/3.3.1/api/python/reference/api/pyspark.ml.feature.StringIndexer.html
"""


def create(table_name, transform_rules, name):
    """
    创建转换器
    :return:
    """
    stages = list()
    transformer_dict = dict()
    for i in range(len(transform_rules)):
        rule = transform_rules[i]
        if rule['type'] == 'string_indexer':
            indexer = StringIndexer(inputCol=rule['input_col'], outputCol=rule['output_col'], handleInvalid='skip')
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
    pipeline_model.save(
        f"{config_helper.get_module_config('hdfs')['base_url']}/user/ai/aip/feature_engineering/{name}")
    hdfs_helper.hdfs_client.hdfs_client.create(
        path=f"/user/ai/aip/feature_engineering/{name}/transformers",
        data=json.dumps(transformer_dict), overwrite=True)


def transform(table_name, transformers, name):
    """
    使用已有的编码器进行特征编码
    :param table_name: 表名
    :param transformers: 编码规则
    :param name: 编码器名称
    :return:
    """
    df = spark_session.sql(f"select * from {table_name}")
    with hdfs_helper.hdfs_client.hdfs_client.open(f'/user/ai/aip/feature_engineering/{name}/transformers') as f:
        transformer_dict = json.loads(f.read())
    pipeline_model = PipelineModel.load(
        f"{config_helper.get_module_config('hdfs')['base_url']}/user/ai/aip/feature_engineering/{name}")
    stages = list()
    for transformer_name in transformers:
        stage_indexes = transformer_dict[transformer_name]
        stages.extend([pipeline_model.stages[i] for i in stage_indexes])
    PipelineModel(stages=stages).transform(df).show()
