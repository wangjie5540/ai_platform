from forecast.common.reference_package import *
from digitforce.aip.common.spark_helper import *



def isometric_binning(sparkdf_config, isometric_label, group_nums, partition_col, col_orderby=None):
    """
    等分组
    :param data:数据
    :param amount: 分组数目
    :param partition_col: 依据的列，默认为空
    :return: 分组后的值
    """
    if col_orderby == None:
        col_orderby = random.rand(seed=10000)
    w = Window.partitionBy(partition_col).orderBy(col_orderby)  # 随机排序
    sparkdf_config = sparkdf_config.withColumn(isometric_label, (psf.row_number().over(w)) % group_nums)
    return sparkdf_config


def equal_frequency_binning(sparkdf_config, frequency_label, bound_dict):
    """等距分箱"""
    bound_cols = []
    for key_config in bound_dict:
        output = 'output_' + str(key_config)
        bound_cols.append(output)
        splits = [-float("inf")]
        value = list(set(bound_dict[key_config]))
        value.sort()
        splits.extend(value)
        splits.append(float("inf"))
        sparkdf_config = sparkdf_config.withColumn("key_label_tmp", sparkdf_config[key_config].cast(DoubleType()))
        bucketizer = Bucketizer(inputCol="key_label_tmp", outputCol=output, splits=splits, handleInvalid='keep')  # 进行分桶
        sparkdf_config = bucketizer.transform(sparkdf_config)
        sparkdf_config = sparkdf_config.drop("key_label_tmp")
    sparkdf_config = label_binning(sparkdf_config, bound_cols, frequency_label)
    return sparkdf_config


def label_binning(sparkdf_config, label_list, col_group):
    """
    根据label进行分组
    :param data: 数据
    :param grouping_label_cols:分组所用到的列
    :return: 依据列分组后的数据
    """
    sparkdf_config = sparkdf_config.drop(col_group)
    sparkdf_config = sparkdf_config.withColumn('col_concat_tmp', concat_ws("&#&", *[psf.col(c) for c in
                                                                                    sparkdf_config.select(
                                                                                        label_list).columns]))
    #     group_category_new_list=data.select('group_category_new').distinct().toPandas()
    group_category_nums = sparkdf_config.select("col_concat_tmp").distinct()
    w = Window.partitionBy().orderBy("col_concat_tmp")
    group_category_nums = group_category_nums.withColumn("{0}".format(col_group), psf.row_number().over(w))
    sparkdf_config = sparkdf_config.join(group_category_nums, on='col_concat_tmp', how='left')
    sparkdf_config = sparkdf_config.drop('col_concat_tmp')

    return sparkdf_config


def group_category(params_model_grouping):

    group_conditions = params_model_grouping['group_condition']
    col_key = params_model_grouping['col_key']
    col_apply_model = params_model_grouping['col_apply_model']
    isometric_label = params_model_grouping['isometric_label']
    frequency_label = params_model_grouping['frequency_label']
    label_label = params_model_grouping['label_label']
    group_label = params_model_grouping['group_label']
    shops = params_model_grouping['shop_list']
    model_selection_table = params_model_grouping['model_selection_table']
    model_grouping_table = params_model_grouping['model_grouping_table']
    sparkdf_config = forecast_spark_helper.read_table(model_selection_table, partition_list = shops) #读取模型选择生成的表
    sparkdf_config = sparkdf_config.withColumn(group_label, lit(None))
    for group_condition in group_conditions:
        print(group_condition)
        for apply_model in eval(group_condition):
            sparkdf_config_group = sparkdf_config.filter("{0}='{1}'".format(col_apply_model, apply_model))
            sparkdf_config_other = sparkdf_config.filter("{0}!='{1}'".format(col_apply_model, apply_model))
            for split_methods in eval(group_condition)[apply_model]:
                for split_method in split_methods:
                    col_groups = []
                    if split_method == 'isometric':
                        group_nums = 50 #sku数量去配置默认值
                        partition_col = col_key[0]
                        col_orderby = None
                        if 'group_nums' in split_methods[split_method]:
                            group_nums = split_methods[split_method]['group_nums']
                        if 'col_partition' in split_methods[split_method]:
                            col_partition = split_methods[split_method]['col_partition']
                        if 'col_orderby' in split_methods[split_method]:
                            col_orderby = split_methods[split_method]['col_orderby']
                        sparkdf_config_group = isometric_binning(sparkdf_config_group, isometric_label, group_nums,
                                                                 partition_col, col_orderby)
                        col_groups.append(isometric_label)
                    elif split_method == 'equal_frequency':
                        print("equal_frequency", split_methods[split_method])
                        sparkdf_config_group = equal_frequency_binning(sparkdf_config_group, frequency_label,
                                                                       split_methods[split_method])
                        col_groups.append(frequency_label)
                    elif split_method == 'col_labels':
                        print("col_labels", split_methods[split_method])
                        sparkdf_config_group = label_binning(sparkdf_config_group, split_methods[split_method],
                                                             label_label)
                        col_groups.append(label_label)
                    else:
                        print("continue")
                        continue
            sparkdf_config_group = label_binning(sparkdf_config_group, col_groups, group_label)
            sparkdf_config = sparkdf_config_group.select(sparkdf_config_other.columns).unionAll(sparkdf_config_other)
    forecast_spark_helper.save_table(sparkdf_config, model_grouping_table, partition=["shop_id"]) #shop配置partionname 数据
    return 'SUCCESS'
