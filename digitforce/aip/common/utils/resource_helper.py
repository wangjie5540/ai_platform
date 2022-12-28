# coding: utf-8
import random


def apply_resource():
    '''
    资源申请 TODO 暂时先写死，后续要进行服务化
    :return:
    '''
    return f'/data/{str(random.randint(100, 1000))}'
