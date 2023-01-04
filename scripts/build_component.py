# coding:utf-8

import os
import subprocess


def main():
    component_name = os.environ['COMPONENT_NAME']
    component_path = os.path.join('src', *component_name.split('-'))
    print(component_path)
    os.chdir(component_path)
    command = f"docker build -t digit-force-docker.pkg.coding.net/ai-platform/ai-components/{component_name} ."
    subprocess.check_call(command, shell=True)


if __name__ == '__main__':
    main()
