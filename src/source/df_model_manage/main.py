import glob
import json
import os

from digitforce.aip.common.hdfs_helper import dg_hdfs_client


def get_report_save_path(solution_id, instance_id):
    return f"/user/aip/pipeline_model_file/{solution_id}/{instance_id}"


def report_content_to_hdfs(report_content, model_name, solution_id, instance_id):
    report_file_name = f"{model_name}.json"
    with open(report_file_name, "w") as fo:
        fo.write(json.dumps(report_content))
    report_hdfs_path = os.path.join(get_report_save_path(solution_id, instance_id), report_file_name)
    dg_hdfs_client.delete(report_hdfs_path)
    dg_hdfs_client.copy_from_local(report_file_name, report_hdfs_path)


def parse_metrics_from_model_file(model_file):
    file_name = os.path.basename(model_file)
    metrics_info = {}
    values = file_name[0:file_name.rfind(".")].split("__")
    for i, _ in enumerate(values):
        vals = _.split("_")
        if i > 0 and len(vals) == 2:
            metrics_info[vals[0]] = vals[1]
    metrics_info = [{"name": k, "value": v} for k, v in metrics_info.items()]
    return metrics_info


def upload_file_model_to_hdfs(solution_id, instance_id, local_model_file, target_hdfs_file=None, model_name=None):
    if model_name is None:
        model_name = os.path.basename(local_model_file).split("__")[0].split(".")[0]
    if target_hdfs_file is None:
        target_hdfs_file = os.path.join(get_report_save_path(solution_id, instance_id),
                                        os.path.basename(local_model_file))

    _type = os.path.basename(local_model_file).split(".")[-1]
    dg_hdfs_client.delete(target_hdfs_file)
    dg_hdfs_client.copy_from_local(local_model_file, target_hdfs_file)
    report_content = {"model_name": model_name,
                      "type": _type,
                      "model_hdfs_path": target_hdfs_file,
                      "metrics": parse_metrics_from_model_file(local_model_file)}
    report_content_to_hdfs(report_content, model_name, solution_id, instance_id)


def upload_dir_model_to_hdfs(solution_id, instance_id, local_model_dir_path, target_hdfs_dir=None, model_name=None):
    model_files = glob.glob(os.path.join(local_model_dir_path, "*"))
    for _ in model_files:
        model_file_name = os.path.basename(_)
        target_hdfs_file = os.path.join(target_hdfs_dir, model_file_name)
        upload_file_model_to_hdfs(solution_id, instance_id, _, target_hdfs_file, model_name)


def main():
    _type, solution_id, instance_id, local_model_dir_path, target_hdfs_dir, model_name = sys.argv[1:]
    if _type == "dir":
        upload_dir_model_to_hdfs(solution_id, instance_id, local_model_dir_path, target_hdfs_dir, model_name)
    if _type == "one_file":
        upload_file_model_to_hdfs(solution_id, instance_id, local_model_dir_path, target_hdfs_dir, model_name)


if __name__ == '__main__':
    from digitforce.aip.common.logging_config import setup_console_log
    import sys

    setup_console_log()
    main()
    # with open("lgb__auc_0.88__recall_0.78__loss_0.3.pk", "wb") as fo:
    #     pass
    # upload_file_model_to_hdfs(10000, 20000, "lgb__auc_0.88__recall_0.78__loss_0.3.pk",
    #                           "/user/aip/recommend/rank/model/lgb/2022-07-21/lgb__auc_0.88__recall_0.78__loss_0.3.pk",
    #                           "rank_lgb_model")
    # print(dg_hdfs_client.list_dir("/user/aip/pipeline_model_file/10000/20000"))
    # print(dg_hdfs_client.list_dir("/user/aip/recommend/rank/model/lgb/2022-07-21/"))
