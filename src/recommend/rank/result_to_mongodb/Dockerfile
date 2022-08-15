FROM digit-force-docker.pkg.coding.net/ai-platform/base-images/pytorch-cpu:latest

ARG PROJECT_DIR=/app/digit-force-kubeflow-pipeline-component-image

RUN mkdir -p $PROJECT_DIR

WORKDIR $PROJECT_DIR
ENV PYTHONPATH=$PROJECT_DIR

RUN mkdir -p $PROJECT_DIR/digitforce/aip
RUN pip install jsonlines -i https://pypi.tuna.tsinghua.edu.cn/simple
RUN pip install motor -i https://pypi.tuna.tsinghua.edu.cn/simple
RUN pip install pymongo -i https://pypi.tuna.tsinghua.edu.cn/simple
COPY ./digitforce/__init__.py $PROJECT_DIR/digitforce/__init__.py
COPY ./digitforce/aip/__init__.py $PROJECT_DIR/digitforce/aip/__init__.py

COPY ./digitforce/aip/common $PROJECT_DIR/digitforce/aip/common
COPY ./digitforce/aip/cgf $PROJECT_DIR/digitforce/aip/cgf

COPY src/recommend/rank/result_to_mongodb/*py $PROJECT_DIR/

