FROM digit-force-docker.pkg.coding.net/ai-platform/base-images/miniconda3-base:latest
RUN pip install -i https://pypi.tuna.tsinghua.edu.cn/simple kfp==1.0.4
ARG COMPONENT_DIR=/componet
CMD mkdir -p $COMPONENT_DIR
WORKDIR $COMPONENT_DIR
COPY common ./common
COPY facade ./facade
COPY components/ml/uplift .