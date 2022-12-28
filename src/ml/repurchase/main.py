import argparse
import repurchase
from param_test import params_dict


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--solution_id', type=str, default='', help='solution id')
    parser.add_argument('--instance_id', type=str, default='', help='instance id')

    args = parser.parse_args()
    print(f'Argument Parameters: args={args}')

    repurchase.train(params_dict, args.solution_id)


if __name__ == '__main__':
    run()
