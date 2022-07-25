import sys
import pandas as pd
from typing import List

def normalization(dataset: pd.DataFrame, vars: List[str]):
    """
    Normalization features
    :param dataset:
    :param vars: vars need to be normalized
    :return:
    """
    for var in vars:
        max_value = dataset[var].max()
        min_value = dataset[var].min()
        dataset[var] = (dataset[var] - min_value) / (max_value - min_value)
    return dataset

def main():
    dataset_path = sys.argv[1]
    output_filepath = sys.argv[2]
    vars_need_normalization = sys.argv[3].split(',')
    # input vars must be split by ','  , represented as a string, like 'age,numbers1,numbers2'

    dataset = pd.read_csv(dataset_path)
    dataset_after_normalization = normalization(dataset, vars_need_normalization)
    dataset_after_normalization.to_csv(output_filepath, index=False)

if __name__ == '__main__':
    main()