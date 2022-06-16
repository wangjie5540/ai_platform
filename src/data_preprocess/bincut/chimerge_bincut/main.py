import sys
from typing import List
from scipy.stats import chi2
import pandas as pd
import numpy as np
from digitforce.aip.common.logging_config import setup_console_log
import logging

# 卡方分箱
def calculate_chi(freq_array):
    """ 计算卡方值
    Args:
        freq_array: Array，待计算卡方值的二维数组，频数统计结果
    Returns:
        卡方值，float
    """
    # 检查是否为二维数组
    assert (freq_array.ndim == 2)

    # 计算每列的频数之和
    col_nums = freq_array.sum(axis=0)
    # 计算每行的频数之和
    row_nums = freq_array.sum(axis=1)
    # 计算总频数
    nums = freq_array.sum()
    # 计算期望频数
    E_nums = np.ones(freq_array.shape) * col_nums / nums
    E_nums = (E_nums.T * row_nums).T
    # 计算卡方值
    tmp_v = (freq_array - E_nums) ** 2 / E_nums
    # 如果期望频数为0，则计算结果记为0
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
        如果不知道卡方阈值怎么取，可以生成卡方表来看看，代码如下：
        import pandas as pd
        import numpy as np
        from scipy.stats import chi2
        p = [0.995, 0.99, 0.975, 0.95, 0.9, 0.5, 0.1, 0.05, 0.025, 0.01, 0.005]
        pd.DataFrame(np.array([chi2.isf(p, df=i) for i in range(1,10)]), columns=p, index=list(range(1,10)))
    Returns:
        最优切分点列表，List
    """
    freq_df = pd.crosstab(index=data[var], columns=data[target])
    # 转化为二维数组
    freq_array = freq_df.values

    # 初始化箱体，每个元素单独一组
    best_bincut = freq_df.index.values

    # 初始化阈值 chi_threshold，如果没有指定 chi_threshold，则默认选择target数量-1，置信度95%来设置阈值
    if max_group is None:
        if chi_threshold is None:
            chi_threshold = chi2.isf(0.05, df=freq_array.shape[-1])

    # 开始迭代
    while True:
        min_chi = None
        min_idx = None
        for i in range(len(freq_array) - 1):
            # 两两计算相邻两组的卡方值，得到最小卡方值的两组
            v = calculate_chi(freq_array[i: i + 2])
            if min_chi is None or min_chi > v:
                min_chi = v
                min_idx = i

        # 是否继续迭代条件判断
        # 条件1：当前箱体数仍大于 最大分箱数量阈值
        # 条件2：当前最小卡方值仍小于制定卡方阈值
        if (max_group is not None and max_group < len(freq_array)) or (
                chi_threshold is not None and min_chi < chi_threshold):
            tmp = freq_array[min_idx] + freq_array[min_idx + 1]
            freq_array[min_idx] = tmp
            freq_array = np.delete(freq_array, min_idx + 1, 0)
            best_bincut = np.delete(best_bincut, min_idx + 1, 0)
        else:
            break

    # 把切分点补上头尾
    best_bincut = best_bincut.tolist()
    best_bincut.append(data[var].min())
    best_bincut.append(data[var].max())
    best_bincut_set = set(best_bincut)
    best_bincut = list(best_bincut_set)

    best_bincut.remove(data[var].min())
    best_bincut.append(data[var].min() - 1)
    # 排序切分点
    best_bincut.sort()
    logging.info(f'变量{var}的最终卡方切分点:{best_bincut}')
    return best_bincut

def data_after_chimerge_bincut(data:pd.DataFrame, var_list: List[str], target: str, max_group_list: List[int], chi_threshold_list):
    """
    获取经过卡方分箱处理后的数据， 默认使用所在分箱号代替分箱后的label
    :param data: 待处理的数据集
    :param var_list: 需要卡方分箱的连续变量名称列表
    :param target: 目标列Y的名称
    :param max_group_list: 每个变量对应的最大分箱数量列表
    :param chi_threshold_list: 卡方阈值列表
    :return:
    """
    for idx, var in enumerate(var_list):
        data[var] = pd.cut(data[var], bins=get_chimerge_bincut(data, var, target, max_group=max_group_list[idx]), labels=False)
    return data

def main():
    setup_console_log()
    dataset_path = sys.argv[1]
    output_filepath = sys.argv[2]
    bins = [int(x) for x in sys.argv[3].split(',')]
    vars_need_bincut = sys.argv[4].split(',')
    target = sys.argv[5]

    dataset = pd.read_csv(dataset_path)
    dataset_after_bincut = data_after_chimerge_bincut(dataset, vars_need_bincut, target, bins, None)
    dataset_after_bincut.to_csv(output_filepath, index=False)

if __name__ == '__main__':
    main()