FROM digit-force-docker.pkg.coding.net/ai-platform/base-images/algorithm-dev:latest
# 安装aip-sdk
RUN pip install digitforce-aip -i https://aip-1657964384920:546b044f44ad6936fef609faa512a53b3fa8b12f@digit-force-pypi.pkg.coding.net/ai-platform/aip/simple
ARG COMPONENT_DIR=/component
RUN mkdir -p $COMPONENT_DIR
WORKDIR $COMPONENT_DIR
COPY . $COMPONENT_DIR

#ENTRYPOINT ["python", "main.py"]
# 编译命令
# docker build --no-cache -t digit-force-docker.pkg.coding.net/ai-platform/ai-components/sample-sample_selection_lookalike .
# push命令
# docker push digit-force-docker.pkg.coding.net/ai-platform/ai-components/sample-sample_selection_lookalike:latest