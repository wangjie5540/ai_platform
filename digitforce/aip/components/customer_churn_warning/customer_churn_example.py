from digitforce.aip.common.constants.global_constant import AI_PLATFORM_IMAGE_REPO
from digitforce.aip.components.op_decorator import *


@mount_data_pv
def customer_churn_example(cur_str, output_file, info_log_file, error_log_file, train_duration=90, predict_duration=30,
                           label_column='deal_num', db_name='default', image_tag='latest'):
    arguments = ['main.py', '--cur_str', cur_str, '--output_file', output_file, '--train_duration', train_duration,
                 '--predict_duration', predict_duration, '--label_column', label_column, '--db_name', db_name,
                 '--info_log_file', info_log_file, '--error_log_file', error_log_file]
    op = dsl.ContainerOp(
        name='customer_churn_example',
        image=f"{AI_PLATFORM_IMAGE_REPO}"
              f"/src-customer_churn_warning-sample_construct" + f":{image_tag}",
        command="python",
        arguments=arguments
    )

    return op
