# coding: utf-8
import pickle
import os


def save_to_pickle(object_to_save, save_path: str):
    save_dir = os.path.dirname(save_path)
    os.makedirs(save_dir)
    with open(save_path, 'wb') as f:
        pickle.dump(object_to_save, f)
