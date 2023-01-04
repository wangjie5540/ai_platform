# coding:utf-8

import os


def main():
    component_name = os.environ['COMPONENT_NAME']
    component_path = os.path.join('src', *component_name.split('-'))
    print(component_path)
    os.system(
        f"docker build -t digit-force-docker.pkg.coding.net/ai-platform/ai-components/{component_name} {component_path}")


if __name__ == '__main__':
    main()
