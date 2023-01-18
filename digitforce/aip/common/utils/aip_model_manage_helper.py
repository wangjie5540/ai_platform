import json
import os
import uuid

from digitforce.aip.common.utils.hdfs_helper import hdfs_client


def report_to_aip(model_and_metrics_data_hdfs_path,
                  model_hdfs_path,
                  model_name,
                  model_type="pk",
                  accuracy=0,
                  auc=0,
                  precision=0,
                  recall=0,
                  f1_score=0,
                  loss=0):
    # all_score = [s_acc, s_auc, s_pre, s_rec, s_f1, s_loss]
    metrics_info = {
        "accuracy": accuracy,
        "auc": auc,
        "precision": precision,
        "recall": recall,
        "f1_score": f1_score,
        "loss": loss,
    }
    metrics_info = [{"name": k, "value": v} for k, v in metrics_info.items()]
    metrics_data = json.dumps({"model_name": model_name,
                               "type": model_type,
                               "model_hdfs_path": model_hdfs_path,
                               "metrics": metrics_info}, ensure_ascii=False)
    metrics_data_local_path = f"tmp-{uuid.uuid4()}"
    with open(metrics_data_local_path, "w") as fo:
        fo.write(metrics_data)

    metrics_data_hdfs_path = model_and_metrics_data_hdfs_path + "/metrics.json"
    if hdfs_client.exists(metrics_data_hdfs_path):
        hdfs_client.delete(metrics_data_hdfs_path)
    hdfs_client.mkdir_dirs(model_and_metrics_data_hdfs_path.replace("hdfs://", ""))
    hdfs_client.copy_from_local(metrics_data_local_path, metrics_data_hdfs_path)
    os.remove(metrics_data_local_path)


def main():
    model_and_metrics_data_hdfs_path = "/user/ai/aip/tmp"
    model_hdfs_path = "/user/ai/aip/tmp/model.pk"
    metrics_info = {
        "accuracy": 0.85,
        "auc": 0.90,
        "precision": 0.93,
        "recall": 0.80,
        "f1_score": 0.88,
        "loss": 0.001,
    }
    report_to_aip(model_and_metrics_data_hdfs_path,
                  model_hdfs_path,
                  model_name="流失预警",
                  model_type="pk",
                  **metrics_info)


if __name__ == '__main__':
    main()
