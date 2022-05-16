# coding: utf-8

# 文件相关工具类
import pickle
import os


def save_to_pickle(object_to_save, save_path: str):
    save_dir = os.path.dirname(save_path)
    os.makedirs(save_dir, exist_ok=True)
    with open(save_path, 'wb') as f:
        pickle.dump(object_to_save, f)
