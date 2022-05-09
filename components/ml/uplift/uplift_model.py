# coding: utf-8
from pylift import TransformedOutcome
import argparse
import pandas as pd
import pathlib
import os


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_file_1', type=str, required=True, help='样本数据')
    parser.add_argument('--output_file', type=str, required=True, help='模型文件')
    return parser.parse_args()


def uplift_train(df: pd.DataFrame, col_treatment='Treatment', col_outcome='Outcome'):
    up = TransformedOutcome(df, col_treatment=col_treatment, col_outcome=col_outcome)
    up.randomized_search(n_iter=10, n_jobs=10, random_state=1, verbose=1)
    up.fit(**up.rand_search_.best_params_)
    return up


if __name__ == '__main__':
    args = parse_arguments()
    # df = pd.read_csv(args.input_file_1)
    output_file = "/model/uplift/uplift.pickle"
    # uplift_train(df).model_final.to_pickle(output_file)
    dir_name = os.path.dirname(args.output_file)
    os.makedirs(dir_name, exist_ok=True)
    with open(args.output_file, 'w') as f:
        pass
