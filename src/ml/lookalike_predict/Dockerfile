FROM digit-force-docker.pkg.coding.net/ai-platform/base-images/algorithm-base:latest
RUN pip install digitforce-aip -i https://aip-1657964384920:546b044f44ad6936fef609faa512a53b3fa8b12f@digit-force-pypi.pkg.coding.net/ai-platform/aip/simple --upgrade
RUN pip install scikit-learn -i https://aip-1657964384920:546b044f44ad6936fef609faa512a53b3fa8b12f@digit-force-pypi.pkg.coding.net/ai-platform/aip/simple

RUN conda install pyhive --yes
ARG PROJECT_DIR=/app/digit-force-kubeflow-pipeline-component-image

RUN mkdir -p $PROJECT_DIR

WORKDIR $PROJECT_DIR
ENV PYTHONPATH=$PROJECT_DIR

RUN mkdir -p $PROJECT_DIR/digitforce/aip
COPY ./digitforce/__init__.py $PROJECT_DIR/digitforce/__init__.py
COPY ./digitforce/aip/__init__.py $PROJECT_DIR/digitforce/aip/__init__.py
RUN mkdir -p /usr/local/etc/
COPY ./dockerfiles/aip_config/dev/aip_config.yaml /usr/local/etc/aip_config.yaml
RUN mkdir -p /root/.kube
COPY ./dockerfiles/aip_config/dev/kube_config /root/.kube/config
COPY ./dockerfiles/aip_config/dev/hdfs-site.xml $SPARK_HOME/conf
COPY ./digitforce/aip/common $PROJECT_DIR/digitforce/aip/common
COPY ./digitforce/aip/components $PROJECT_DIR/digitforce/aip/components
COPY src/ml/lookalike_predict/ $PROJECT_DIR