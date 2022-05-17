# coding=utf-8
import logging
import os


def get_dockerfile_content(image_dir, bottom_image_name=None):
    if bottom_image_name is None:
        bottom_image_name = "digit-force-docker.pkg.coding.net/ai-platform/base-images/miniconda3-base:latest"
    return f'''FROM {bottom_image_name}

ARG PROJECT_DIR=/app/digit-force-kubeflow-pipeline-component-image

RUN mkdir -p $PROJECT_DIR

WORKDIR $PROJECT_DIR
ENV PYTHONPATH=$PROJECT_DIR

COPY .digitforce_ai_platform $PROJECT_DIR/.digitforce_ai_platform
COPY ./common $PROJECT_DIR/common

COPY {image_dir}/* $PROJECT_DIR/
'''


def generate_docker_file(one_dir, bottom_image_name=None):
    dockerfile_path = os.path.join(one_dir, "Dockerfile")
    with open(dockerfile_path, mode='w', encoding='utf-8') as fo:
        fo.write(get_dockerfile_content(one_dir, bottom_image_name))
    build_cmd = f"docker build -t " \
                f"digit-force-docker.pkg.coding.net/ai-platform/ai-src/{one_dir.replace('/', '-')}" \
                f" -f {dockerfile_path} ."
    os.system(build_cmd)
    push_cmt = f"docker push " \
               f"digit-force-docker.pkg.coding.net/ai-platform/ai-src/{one_dir.replace('/', '-')}"
    os.system(push_cmt)


def find_main_file(one_dir, result):
    if not os.path.exists(one_dir):
        logging.warning(f"this dir is not exists dir:{one_dir}")
    if os.path.isdir(one_dir):
        if os.path.exists(os.path.join(one_dir, "main.py")):
            result.append(one_dir)
            return result
        child_dirs = os.listdir(one_dir)
        for _dir in child_dirs:
            _dir = os.path.join(one_dir, _dir)
            if os.path.isdir(_dir):
                find_main_file(_dir, result)


def main():
    for _dir in ["src/recommend", "src/data_preprocess", "src/source"]:
        result = []
        find_main_file(_dir, result)
        for _ in result:
            generate_docker_file(_)


if __name__ == '__main__':
    main()
