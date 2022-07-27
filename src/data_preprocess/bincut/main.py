import sys
from typing import List
from scipy.stats import chi2
import pandas as pd
import numpy as np
# from digitforce.aip.common.logging_config import setup_console_log
import logging


def calculate_chi(freq_array):
    """ 计算卡方值
    Args:
        freq_array: Array，待计算卡方值的二维数组，频数统计结果
    Returns:
        卡方值，float
    """
    assert (freq_array.ndim == 2)

    col_nums = freq_array.sum(axis=0)
    row_nums = freq_array.sum(axis=1)
    nums = freq_array.sum()
    E_nums = np.ones(freq_array.shape) * col_nums / nums
    E_nums = (E_nums.T * row_nums).T
    tmp_v = (freq_array - E_nums) ** 2 / E_nums
    tmp_v[E_nums == 0] = 0
    chi_v = tmp_v.sum()
    return chi_v


def get_chimerge_bincut(data, var, target, max_group=None, chi_threshold=None):
    """ 计算卡方分箱的最优分箱点
    Args:
        data: DataFrame，待计算卡方分箱最优切分点列表的数据集
        var: 待计算的连续型变量名称
        target: 待计算的目标列Y的名称
        max_group: 最大的分箱数量（因为卡方分箱实际上是合并箱体的过程，需要限制下最大可以保留的分箱数量）
        chi_threshold: 卡方阈值，如果没有指定max_group，我们默认选择类别数量-1，置信度95%来设置阈值

    Returns:
        最优切分点列表，List
    """
    freq_df = pd.crosstab(index=data[var], columns=data[target])
    freq_array = freq_df.values

    best_bincut = freq_df.index.values

    if max_group is None:
        if chi_threshold is None:
            chi_threshold = chi2.isf(0.05, df=freq_array.shape[-1])

    while True:
        min_chi = None
        min_idx = None
        for i in range(len(freq_array) - 1):
            v = calculate_chi(freq_array[i: i + 2])
            if min_chi is None or min_chi > v:
                min_chi = v
                min_idx = i

        if (max_group is not None and max_group < len(freq_array)) or (
                chi_threshold is not None and min_chi < chi_threshold):
            tmp = freq_array[min_idx] + freq_array[min_idx + 1]
            freq_array[min_idx] = tmp
            freq_array = np.delete(freq_array, min_idx + 1, 0)
            best_bincut = np.delete(best_bincut, min_idx + 1, 0)
        else:
            break

    best_bincut = best_bincut.tolist()
    best_bincut.append(data[var].min())
    best_bincut.append(data[var].max())
    best_bincut_set = set(best_bincut)
    best_bincut = list(best_bincut_set)

    best_bincut.remove(data[var].min())
    best_bincut.append(data[var].min() - 1)
    best_bincut.sort()
    # logging.info(f'变量{var}的最终切分点:{best_bincut}')
    return best_bincut

def data_after_chimerge_bincut(data:pd.DataFrame, var_list: List[str], target: str, max_group, chi_threshold):
    """
    获取经过卡方分箱处理后的数据， 默认使用所在分箱号代替分箱后的label
    :param data: 待处理的数据集
    :param var_list: 需要卡方分箱的连续变量名称列表
    :param target: 目标列Y的名称
    :param max_group: 变量对应的最大分箱数量
    :param chi_threshold: 卡方阈值
    :return:
    """
    bincut_dict = {}
    for idx, var in enumerate(var_list):
        bincut = get_chimerge_bincut(data, var, target, max_group=max_group, chi_threshold=chi_threshold)
        bincut_dict[var] = bincut
        data[var] = pd.cut(data[var], bins=bincut, labels=False)
    return data, bincut_dict

def main():
    # setup_console_log()
    dataset_path = sys.argv[1]
    output_filepath = sys.argv[2]
    bins = int(sys.argv[3])
    vars_need_bincut = sys.argv[4].split(',')
    target = sys.argv[5]

    dataset = pd.read_csv(dataset_path)
    dataset_after_bincut, binct_dict = data_after_chimerge_bincut(dataset, vars_need_bincut, target, bins, None)
    dataset_after_bincut.to_csv(output_filepath, index=False)

if __name__ == '__main__':
    main()