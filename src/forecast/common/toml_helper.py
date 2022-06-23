# -*- coding: utf-8 -*-
# @Time : 2021/12/25
# @Author : Arvin
import toml
import os

BASE_DIR = os.path.abspath("../config/")


class TomlOperation:

    def __init__(self, file_name):
        self.file_name = file_name
        self.toml_file_path = os.path.join(BASE_DIR, file_name)

    # def pop_file(self, key):
    #     dic = self.read_file()
    #     if key in dic:
    #         dic.pop(key)
    #         self.overwrite_file(dic)
    #     else:
    #         print("文件中无此 {0} key值".format(key))
    #
    # def append_file(self, other):
    #     dic = self.read_file()
    #     dic.update(other)
    #     self.overwrite_file(dic)
    #
    # def overwrite_file(self, new_dic):
    #     with open(self.toml_file_path, "w", encoding="utf-8") as fs:
    #         toml.dump(new_dic, fs)

    def read_file(self):
        print(self.toml_file_path)
        with open(self.toml_file_path, "r", encoding="utf-8") as fs:
            dic_param = toml.load(fs)
        return dic_param