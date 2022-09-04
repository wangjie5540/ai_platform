FROM digit-force-docker.pkg.coding.net/ai-platform/base-images/algorithm-dev:latest
# 安装aip-sdk
RUN pip install digitforce-aip sklearn -i https://aip-1657964384920:546b044f44ad6936fef609faa512a53b3fa8b12f@digit-force-pypi.pkg.coding.net/ai-platform/aip/simple
# https://pytorch.org/get-started/previous-versions/
RUN pip install torch==1.10.1+cpu torchvision==0.11.2+cpu torchaudio==0.10.1 -f https://download.pytorch.org/whl/torch_stable.html
ARG COMPONENT_DIR=/component
RUN mkdir -p $COMPONENT_DIR
WORKDIR $COMPONENT_DIR
COPY . $COMPONENT_DIR

# 编译命令
# docker build -t digit-force-docker.pkg.coding.net/ai-platform/ai-components/ml-lookalike:latest .
# docker push digit-force-docker.pkg.coding.net/ai-platform/ai-components/ml-lookalike:latest