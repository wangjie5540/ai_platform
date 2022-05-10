# coding: utf-8
import pickle


def save_to_pickle(object_to_save, save_path: str):
    with open(save_path, 'wb') as f:
        pickle.dump(object_to_save, f)
