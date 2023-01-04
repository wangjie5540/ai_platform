# coding:utf-8

import os
import subprocess


def main():
    component_name = os.environ['COMPONENT_NAME']
    component_path = os.path.join('src', *component_name.split('-'))
    os.chdir(component_path)
    cmd = "docker build -t digit-force-docker.pkg.coding.net/ai-platform/ai-components/{component_name} .".format(
        component_name=component_name)
    print()
    subprocess.check_call(cmd, shell=True)


if __name__ == '__main__':
    main()
