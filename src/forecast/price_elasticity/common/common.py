# -*- coding:utf-8  -*-

def rename_columns(df, columns):
    """
    重命名spark的dataframe的列明
    """
    if isinstance(columns, dict):
        for old_name, new_name in columns.items():
            df = df.withColumnRenamed(old_name, new_name)
        return df
    else:
        raise ValueError("'columns' should be a dict, like {'old_name_1':'new_name_1', 'old_name_2':'new_name_2'}")


def days(i):
    """
    时间窗口
    """
    return i * 86400
