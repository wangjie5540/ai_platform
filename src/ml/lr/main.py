import json
import logging
import os
import pickle
import sys

import datatable as dt
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, roc_auc_score


def train_lr(train_dataset_path, test_dataset_path, label_col, model_save_dir,
             param={}):
    df = dt.fread(train_dataset_path)
    feature_names = [_ for _ in df.names if _ != label_col]
    train_X = df[:, feature_names]
    train_y = df[:, label_col].to_numpy()

    clf = LogisticRegression(**param)
    clf.fit(train_X, train_y)

    test_df = dt.fread(test_dataset_path)
    test_X = test_df[:, feature_names]
    test_y = test_df[:, label_col].to_numpy()
    y_pred = clf.predict(test_X)

    y_pred = [1 if _ > 0.5 else 0 for _ in y_pred]
    auc = roc_auc_score(y_pred, test_y)
    logging.info(f"{classification_report(test_y, y_pred)}")

    model_name = os.path.join(model_save_dir, f"lr__{round(auc, 6)}.pk")
    with open(model_name, "wb") as fo:
        pickle.dump(clf, fo)
    logging.info(model_name)


def main():
    train_dataset_path = sys.argv[1]
    test_dataset_path = sys.argv[2]
    model_save_dir = sys.argv[3]
    label_col = sys.argv[4]
    param = json.loads(sys.argv[5]) if len(sys.argv) > 5 else {}
    train_lr(train_dataset_path, test_dataset_path, label_col, model_save_dir,
             param=param)


if __name__ == '__main__':
    from digitforce.aip.common.logging_config import setup_console_log

    setup_console_log()
    main()
