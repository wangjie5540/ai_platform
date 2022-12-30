# coding: utf-8


from digitforce.aip.common.utils.spark_helper import spark_client, spark_session
from pyspark.ml.feature import StandardScaler, StringIndexer, VectorAssembler
from pyspark.ml import Pipeline, PipelineModel
import digitforce.aip.common.utils.config_helper as config_helper
import digitforce.aip.common.utils.id_helper as id_helper


def transform(table_name, transform_rules):
    stages = list()
    index_rules = list()
    scale_rules = list()
    need_keep_cols = list()
    for rule in transform_rules:
        if rule['type'] == 'string_indexer':
            index_rules.append(rule)
        elif rule['type'] == 'standard_scaler':
            scale_rules.append(rule)
        if rule['need_keep']:
            need_keep_cols.append(rule['input_col'])

    for rule in index_rules:
        stages.append(StringIndexer(inputCol=rule['input_col'], outputCol=rule['output_col']))
    input_scale_cols = [rule['input_col'] for rule in scale_rules]
    output_scale_cols = [rule['output_col'] for rule in scale_rules]
    stages.append(VectorAssembler(inputCols=input_scale_cols, outputCol='feature_vector'))
    stages.append(StandardScaler(inputCol='feature_vector', outputCol='scaled_features'))
    df = spark_session.sql(f"select * from {table_name} where u_amount_sum_30d is not null")
    pipeline = Pipeline(stages=stages)
    pipeline.fit(df).save(f"{config_helper.get_module_config('hdfs')['base_url']}/user/ai/aip/wtg_test")
    # vector转column的功能可参考：https://www.youtube.com/watch?v=5f49EVqljH0
    indexed_output_cols = [rule['output_col'] for rule in index_rules]
    PipelineModel.load(f"{config_helper.get_module_config('hdfs')['base_url']}/user/ai/aip/wtg_test") \
        .transform(df).rdd.map(lambda x: [x[c] for c in need_keep_cols + indexed_output_cols] + [float(y) for y in x['scaled_features']]) \
        .toDF(need_keep_cols + indexed_output_cols + output_scale_cols).show()
