import logging
import os
import pickle
import sys

import datatable as dt
import lightgbm as lgb
from sklearn.metrics import classification_report, roc_auc_score


def train_lightgbm(train_dataset_path, test_dataset_path, model_save_dir, label_col, categorical_feature, train_param):
    df = dt.fread(train_dataset_path)
    feature_names = [_ for _ in df.names if _ != label_col]
    train_X = df[:, feature_names]
    train_y = df[:, label_col].to_numpy()
    train_dataset = lgb.Dataset(train_X, label=train_y,
                                feature_name=feature_names,
                                categorical_feature=categorical_feature,
                                free_raw_data=False)

    test_df = dt.fread(test_dataset_path)
    test_X = test_df[:, feature_names]
    test_y = test_df[:, label_col].to_numpy()
    logging.info(train_param)
    lgb_model = lgb.train(train_param, train_dataset)

    y_pred = lgb_model.predict(test_X).tolist()
    y_pred = [1 if _ > 0.5 else 0 for _ in y_pred]
    auc = roc_auc_score(y_pred, test_y)
    logging.info(f"{classification_report(test_y, y_pred)}")
    max_depth = train_param["max_depth"]
    lr = train_param["learning_rate"]
    n = train_param["n_estimators"]
    model_name = os.path.join(model_save_dir, f"lgb_cpu_{max_depth}_{lr}_{n}__{round(auc, 6)}.pk")
    with open(model_name, "wb") as fo:
        pickle.dump(lgb_model, fo)
    logging.info(model_name)


def main():
    train_dataset_path = sys.argv[1]
    test_dataset_path = sys.argv[2]
    model_save_dir = sys.argv[3]
    label_col = sys.argv[4]
    categorical_feature = sys.argv[5].strip().split(",")

    params = []
    for max_depth in [3, 4, 5]:
        for lr in [0.1, 0.2, 0.3, 0.4]:
            for n in [100, 200, 300, 500]:
                params.append({
                    "max_depth": max_depth, "learning_rate": lr, "n_estimators": n,
                    "objective": 'binary', "is_unbalance": True, "metric":
                        'binary_logloss,auc,recall', "num_leaves": 1024, "device": "cpu"})
    for train_param in params:
        train_lightgbm(train_dataset_path, test_dataset_path,
                       model_save_dir, label_col, categorical_feature, train_param)


if __name__ == '__main__':
    from digitforce.aip.common.logging_config import setup_console_log

    setup_console_log()
    main()
