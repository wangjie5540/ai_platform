# coding: utf-8
import common.constants.global_constant as global_constant
import os


def pass_output(output_params: dict):
    os.makedirs(global_constant.component_output_path, exist_ok=True)
    if output_params:
        for key, value in output_params.items():
            save_path = os.path.join(global_constant.component_output_path, key)
            print(save_path)
            with open(save_path, 'w') as f:
                f.write(value)
