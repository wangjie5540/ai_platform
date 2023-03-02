FROM centos:7
ARG TMP_FILE=/tmp/miniconda3.sh
ARG MINICONDA_HOME=/opt/miniconda3
ARG MINICONDA3_INSTALL_FILE
ENV PATH $MINICONDA_HOME/bin:$MINICONDA_HOME/condabin:$PATH
RUN yum install -y gcc gcc-c++ zip unzip \
    && yum install -y wget \
    && wget -q -O $TMP_FILE --no-check-certificate https://mirrors.tuna.tsinghua.edu.cn/anaconda/miniconda/${MINICONDA3_INSTALL_FILE} \
    && sh $TMP_FILE -b -p $MINICONDA_HOME \
    && rm -rf $TMP_FILE \
    && $MINICONDA_HOME/bin/conda init
# 安装机器学习必要的基础包，这些包长时间不会新增
RUN conda install --yes pyhive
RUN pip install torch==1.10.1+cpu torchvision==0.11.2+cpu torchaudio==0.10.1 -f https://download.pytorch.org/whl/torch_stable.html
RUN pip install xgboost Cython pandas scipy sklearn requests -i https://pypi.tuna.tsinghua.edu.cn/simple


# 编译python3.7镜像
# docker build --no-cache --build-arg MINICONDA3_INSTALL_FILE=Miniconda3-py37_4.12.0-Linux-x86_64.sh -t aip-tcr.tencentcloudcr.com/aip/python:3.7 -f python.Dockerfile .
# 上传python3.7镜像
# docker push aip-tcr.tencentcloudcr.com/aip/python:3.7

# 编译python3.9镜像
# docker build --no-cache --build-arg MINICONDA3_INSTALL_FILE=Miniconda3-py39_4.12.0-Linux-x86_64.sh -t digit-force-docker.pkg.coding.net/ai-platform/base-images/python:3.9 -f python.Dockerfile .
# 上传python3.9镜像
# docker push digit-force-docker.pkg.coding.net/ai-platform/base-images/python:3.9

# 具体见CI中的脚本
