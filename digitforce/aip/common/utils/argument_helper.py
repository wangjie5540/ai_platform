import argparse
import json
import logging
import os


class DigitforceAipCmdArgumentHelper:

    def __init__(self):
        self.parser = argparse.ArgumentParser()
        self.args = None

    def parse_envs_from_aip_config(self):
        global_params = os.getenv("global_params", None)
        name = os.getenv("name", None)
        # todo 如果global_params从命令传入，则覆盖掉环境变量，不建议通过命令行传入global_params 后续版本移除
        if hasattr(self.args, "global_params") and getattr(self.args, "global_params"):
            global_params = getattr(self.args, "global_params")
            name = getattr(self.args, "name")
        # 如果 存在glob_params 替换掉 aip_pipeline_env
        if global_params and name:
            os.environ["aip_pipeline_env"] = global_params
            os.environ["container_name"] = name
        aip_pipeline_env_json_str = os.getenv("aip_pipeline_env", None)
        container_name = os.getenv("container_name", None)
        if aip_pipeline_env_json_str and container_name:
            pipeline_config_json_obj = json.loads(aip_pipeline_env_json_str)
            if container_name not in pipeline_config_json_obj:
                logging.info(f"cant not parse env for container aip_pipeline_env: \n{aip_pipeline_env_json_str}\n"
                             f"container_name:{container_name}")
            container_envs = pipeline_config_json_obj[container_name]
            for k, v in container_envs.items():
                os.environ[k] = v

    def add_argument(self, *args, **kwargs):
        self.parser.add_argument(*args, **kwargs)

    def get_argument(self, argument_name, is_json_obj=False):
        self.parse_envs_from_aip_config()

        self.args = self.parser.parse_args()
        # env or the json string(aip_pipeline_env for global_params)
        argument_value_from_env = os.environ.get(argument_name, None)

        # argument from cmdline
        argument_value_from_cmdline = getattr(self.args, argument_name) if hasattr(self.args, argument_name) else None
        # todo get argument_value from config file
        parser_default_value = self.parser.get_default(argument_name)
        argument_value = argument_value_from_cmdline
        if argument_value_from_cmdline == parser_default_value and argument_value_from_env:
            argument_value = argument_value_from_env

        if is_json_obj and isinstance(argument_value, str):
            try:
                argument_value = json.loads(argument_value)
            except Exception as e:
                logging.error(f"{argument_value} wanted jsonObject real get {argument_value}")
                raise e
        return argument_value


df_argument_helper = DigitforceAipCmdArgumentHelper()


def main():
    os.environ["aip_pipeline_env"] = '''{"aip-test-container":{"input_file":"hdfs:///user/aip/test.csv"}}'''
    os.environ["container_name"] = "aip-test-container"
    helper = DigitforceAipCmdArgumentHelper()
    helper.add_argument('--input_file', type=str, default="data.csv", help='input filename')
    helper.add_argument('--output_file', type=str, default="output.txt", help='output filename')
    helper.add_argument('--model_type', type=str, default="simply", help='simply or ordered')

    input_file = helper.get_argument("input_file")

    print('input args', input_file, helper.get_argument("output_file"),
          helper.get_argument("model_type"),
          helper.get_argument("aa"))


if __name__ == '__main__':
    main()
