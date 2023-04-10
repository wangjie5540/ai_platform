from setuptools import setup, find_packages
__version__ = "0.1.39"

requirements = [
    'fonttools',
    'findspark',
    'networkx',
    'kfp==1.0.4',
    'cos-python-sdk-v5',
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
    python_requires='>=3.7.0',
    install_requires=requirements
)

'''
参考：
https://help.coding.net/docs/artifacts/quick-start/pypi.html
安装依赖包：
pip install twine setuptools wheel -i https://pypi.tuna.tsinghua.edu.cn/simple
编译&上传制品库：
python3 setup.py sdist bdist_wheel && twine upload -r coding-pypi dist/* && rm -rf build dist digitforce_aip.egg-info
'''