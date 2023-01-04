FROM digit-force-docker.pkg.coding.net/ai-platform/base-images/algorithm-dev:latest

# 安装aip-sdk
RUN pip install digitforce-aip scikit-learn -i https://aip-1657964384920:546b044f44ad6936fef609faa512a53b3fa8b12f@digit-force-pypi.pkg.coding.net/ai-platform/aip/simple
RUN pip install torch==1.10.1+cpu torchvision==0.11.2+cpu torchaudio==0.10.1 -f https://download.pytorch.org/whl/torch_stable.html

#FROM digit-force-docker.pkg.coding.net/ai-platform/base-images/pytorch-cpu:latest
RUN #pip install digitforce-aip -i https://aip-1657964384920:546b044f44ad6936fef609faa512a53b3fa8b12f@digit-force-pypi.pkg.coding.net/ai-platform/aip/simple


RUN conda install pyhive --yes
ARG PROJECT_DIR=/app/digit-force-kubeflow-pipeline-component-image

RUN mkdir -p $PROJECT_DIR

WORKDIR $PROJECT_DIR
ENV PYTHONPATH=$PROJECT_DIR

RUN mkdir -p $PROJECT_DIR/digitforce/aip
COPY ./digitforce/__init__.py $PROJECT_DIR/digitforce/__init__.py
COPY ./digitforce/aip/__init__.py $PROJECT_DIR/digitforce/aip/__init__.py
RUN mkdir -p /usr/local/etc/
COPY aip_config.yaml /usr/local/etc/aip_config.yaml
COPY ./digitforce/aip/common $PROJECT_DIR/digitforce/aip/common
COPY ./digitforce/aip/components $PROJECT_DIR/digitforce/aip/components
COPY src/ml/lookalike/ $PROJECT_DIR
#ENTRYPOINT ["python", "main.py"]
# 编译命令
# docker build --no-cache -t digit-force-docker.pkg.coding.net/ai-platform/ai-components/ml-lookalike .
# push命令
# docker push digit-force-docker.pkg.coding.net/ai-platform/ai-components/ml-lookalike