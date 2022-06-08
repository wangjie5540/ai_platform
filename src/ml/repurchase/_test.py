# coding: utf-8

import repurchase
import json

input_train_params = ''
output_model_path = ''

with open(input_train_params, 'r') as file:
    data = file.readline()

params_dict = json.loads(data)
repurchase.train(params_dict, output_model_path)
