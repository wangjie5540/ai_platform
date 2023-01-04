# coding:utf-8

import os
import subprocess


def main():
    component_name = os.environ['COMPONENT_NAME']
    component_path = os.path.join('src', *component_name.split('-'))
    os.chdir(component_path)
    cmd = "docker build -t digit-force-docker.pkg.coding.net/ai-platform/ai-components/{component_name} .".format(
        component_name=component_name)
    subprocess.check_call(cmd, shell=True)
    subprocess.check_call(
        "docker login -u ai-components-1672810563540 -p 1d228954e03793ce2e79bf655335abc4e961ec75 digit-force-docker.pkg.coding.net",
        shell=True)
    subprocess.check_call(
        "docker push digit-force-docker.pkg.coding.net/ai-platform/ai-components/{component_name}".format(
            component_name=component_name), shell=True)


if __name__ == '__main__':
    main()
