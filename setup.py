from setuptools import setup, find_packages

__version__ = '0.1.13'
requirements = [
    'pandas',
    'scipy',
    'PyHDFS',
    'redis',
    'kafka-python',
    'requests',
    'Cython',
    'fonttools',
    'pyaml',
    'python-dateutil',
    'xlrd',
    'findspark',
    'h5py',
    'hdfs',
    'lxml',
    'Pillow',
    'ortools',
    'openpyxl',
    'networkx',
    'kfp==1.0.4',
    'pysnowflake',
    'cos-python-sdk-v5'
]

setup(
    name='digitforce-aip',
    version=__version__,
    author='wangtonggui',
    author_email='wangtonggui@digitforce.com',
    description='算法平台sdk',
    packages=find_packages(
        include=[
            'digitforce',
            'digitforce.*',
        ]),
    python_requires='>=3.6.0',
    install_requires=requirements
)

# 参考：https://help.coding.net/docs/artifacts/quick-start/pypi.html
# 安装依赖包：pip install twine setuptools wheel -i https://pypi.tuna.tsinghua.edu.cn/simple
# 编译与安装：python3 setup.py sdist bdist_wheel
# 上传制品库：twine upload -r coding-pypi dist/*
