FROM digit-force-docker.pkg.coding.net/ai-platform/base-images/miniconda3-base:latest

ARG PROJECT_DIR=/app/digit-force-kubeflow-pipeline-component-image

RUN mkdir -p $PROJECT_DIR

WORKDIR $PROJECT_DIR
ENV PYTHONPATH=$PROJECT_DIR

RUN mkdir -p $PROJECT_DIR/digitforce/aip
RUN pip install scipy==1.7.0 -i https://pypi.tuna.tsinghua.edu.cn/simple
RUN pip install bayesian-optimization -i https://pypi.tuna.tsinghua.edu.cn/simple
COPY ./digitforce/__init__.py $PROJECT_DIR/digitforce/__init__.py
COPY ./digitforce/aip/__init__.py $PROJECT_DIR/digitforce/aip/__init__.py

COPY ./digitforce/aip/common $PROJECT_DIR/digitforce/aip/common
COPY ./digitforce/aip/cgf $PROJECT_DIR/digitforce/aip/cgf

COPY src/recommend/rank/data_process/*py $PROJECT_DIR/

