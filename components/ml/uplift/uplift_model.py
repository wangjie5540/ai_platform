# coding: utf-8
from pylift import TransformedOutcome
import argparse
import pandas as pd
import pathlib
import os
import common.constants.global_constant as global_constant
import common.utils.component_helper as component_helper
import common.utils.file_helper as file_helper


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_file', type=str, required=True, help='样本数据')
    return parser.parse_args()


def uplift_train(df: pd.DataFrame, col_treatment='Treatment', col_outcome='Outcome'):
    up = TransformedOutcome(df, col_treatment=col_treatment, col_outcome=col_outcome, stratify=df['Treatment'])
    up.randomized_search(n_iter=10, n_jobs=10, random_state=1, verbose=1)
    up.fit(**up.rand_search_.best_params_, productionize=True)
    return up


output_file = os.path.join(global_constant.mount_nfs_dir, 'uplift.model')

if __name__ == '__main__':
    args = parse_arguments()
    df = pd.read_csv(args.input_file)
    up = uplift_train(df)
    file_helper.save_to_pickle(up, output_file)
    # TODO 后续将进行封装
    component_helper.pass_output({'model': output_file})
