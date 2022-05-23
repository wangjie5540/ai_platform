import os
from setuptools import setup, find_packages

__version__ = '0.1.0-alpha'
with open('requirements.txt', 'r') as f:
    requirements = f.readlines()

setup(
    name='aip_sdk',
    version=__version__,
    author='wangtonggui',
    author_email='wangtonggui@digitforce.com',
    description='算法平台sdk',
    packages=find_packages(include=('common',)),
    python_requires='>=3.5.0',
    install_requires=requirements
)

# 执行python3 setup.py install进行本地包构建
