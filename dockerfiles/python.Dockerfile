FROM centos:7
ARG TMP_FILE=/tmp/miniconda3.sh
ARG MINICONDA_HOME=/opt/miniconda3
ARG MINICONDA3_INSTALL_FILE
ENV PATH $MINICONDA_HOME/bin:$MINICONDA_HOME/condabin:$PATH
RUN yum install -y gcc gcc-c++ unzip \
    && yum install -y wget \
    && wget -q -O $TMP_FILE --no-check-certificate https://mirrors.tuna.tsinghua.edu.cn/anaconda/miniconda/${MINICONDA3_INSTALL_FILE} \
    && sh $TMP_FILE -b -p $MINICONDA_HOME \
    && rm -rf $TMP_FILE \
    && $MINICONDA_HOME/bin/conda init
RUN conda install --yes pyhive

# 编译python3.7镜像
# docker build --no-cache --build-arg MINICONDA3_INSTALL_FILE=Miniconda3-py37_4.12.0-Linux-x86_64.sh -t digit-force-docker.pkg.coding.net/ai-platform/base-images/python:3.7 -f python.Dockerfile .
# 上传python3.7镜像
# docker push digit-force-docker.pkg.coding.net/ai-platform/base-images/python:3.7

# 编译python3.9镜像
# docker build --no-cache --build-arg MINICONDA3_INSTALL_FILE=Miniconda3-py39_4.12.0-Linux-x86_64.sh -t digit-force-docker.pkg.coding.net/ai-platform/base-images/python:3.9 -f python.Dockerfile .
# 上传python3.9镜像
# docker push digit-force-docker.pkg.coding.net/ai-platform/base-images/python:3.9
