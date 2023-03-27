from shap import Explainer
import pandas as pd
import numpy as np
from typing import List

import matplotlib.pyplot as plt

plt.rcParams["font.sans-serif"] = ["SimHei"]  # 设置字体
plt.rcParams["axes.unicode_minus"] = False  # 该语句解决图像中的“-”负号的乱码问题


# 测试离散型变量的ALE
def cat_ale(x_test: pd.DataFrame, model, cat_var: str):
    """离散型变量的ale值计算

    Args:
        x_test (pd.DataFrame): 需要与模型需要输入的数据形状相同
        model : 现支持sklearn接口的model
        cat_var (str): 离散型特征名称

    Returns:
        回归返回每个枚举值对应的ale值，[['cat_var','ale_val']]，
        分类返回每个枚举值对应的每个类别的ale值：[['cat_var',class_cols]]
    """
    if hasattr(model, "predict_proba"):
        model_type = "classifier"
    else:
        model_type = "regressor"
    if model_type == "classifier":
        predict_prob = model.predict_proba(x_test)
        num_class = predict_prob.shape[1]
        class_cols = [str(i) for i in range(num_class)]  # 预测的每个类的列名称
        pred_dataframe = pd.DataFrame(predict_prob, columns=class_cols)
        pred_dataframe = pd.concat(
            [x_test.reset_index(drop=True), pred_dataframe], axis=1
        )  # 在预测样本数据后添加每个类别对应的预测概率
        cat_var_sorted = sorted(
            pred_dataframe[cat_var].unique()
        )  # 对离散值进行排序 TODO:离散变量排序方式优化

        cat_mean_probs = np.zeros((len(cat_var_sorted), num_class))
        ale_vals = np.zeros((len(cat_var_sorted), num_class))

        for i in range(len(cat_var_sorted)):  # 对于每个离散变量
            pred_dfi = pred_dataframe.loc[
                       pred_dataframe[cat_var] == cat_var_sorted[i], :
                       ]
            if not len(pred_dfi):  # 预测样本集中该特征未出现该枚举值
                cat_mean_probs[i] = 0
            else:
                cat_mean_probs[i] = np.mean(
                    pred_dataframe.loc[
                        pred_dataframe[cat_var] == cat_var_sorted[i], class_cols
                    ].values,
                    axis=0,
                )  # 每个枚举值预测的平均值
            if not i:  # i是否为0
                ale_vals[i] = cat_mean_probs[i]
            else:
                ale_vals[i] = cat_mean_probs[i] - cat_mean_probs[i - 1]

        class_ale = np.hstack(
            (np.array(cat_var_sorted).reshape(-1, 1), ale_vals)
        )  # 离散变量和ale值合并
        cat_var_ale = pd.DataFrame(class_ale, columns=["feature_value"] + class_cols)
        return cat_var_ale
    elif model_type == "regressor":
        y_pred = model.predict(x_test)
        pred_dataframe = x_test.copy()
        pred_dataframe["price_pred"] = y_pred
        cat_var_sorted = sorted(pred_dataframe[cat_var].unique())
        # 对每个取值计算出其概率值
        cat_var_probs = []
        for val in cat_var_sorted:
            cat_var_probs.append(
                pred_dataframe.loc[pred_dataframe[cat_var] == val, "price_pred"].mean()
            )
        # 计算 ALE 值
        ale_vals = []
        for i in range(len(cat_var_sorted)):
            if i == 0:
                ale_vals.append(cat_var_probs[i])
            else:
                ale_vals.append(
                    cat_var_probs[i] - cat_var_probs[i - 1]
                )  # 对于分类变量，设置每个枚举值为一个bin，该步骤计算bin内的平均值

        # 将 ALE 值与分类变量取值绑定
        cat_var_ale = pd.DataFrame({"feature_value": cat_var_sorted, "ale": ale_vals})
        return cat_var_ale


# 测试连续型变量的ALE
def num_ale(X: pd.DataFrame, feature: str, model, n_bins=20):
    """连续型变量的ale值

    Args:
        X (pd.DataFrame): 需要与模型需要输入的数据形状相同
        feature (str): 连续型特征名称
        model : 现支持sklearn接口的机器学习模型
        n_bins (int, optional): 分块数量，也是折线图数据点数量，数字越大，折线走势越受到数据分布的影响
        ，数字越小越不容易被数据分布影响. Defaults to 20.

    Returns:
        回归返回每个bins的中位数对应的ale值，[['cat_var','ale_val']]
        ，分类返回每个枚举值对应的每个类别的ale值：[['cat_var',class_cols]]
    """
    # 计算端点值，TODO:分箱方式优化
    feat_range = np.linspace(np.min(X[feature]), np.max(X[feature]), n_bins + 1)
    if hasattr(model, "predict_proba"):
        model_type = "classifier"
    else:
        model_type = "regressor"

    if model_type == "regressor":
        # FOR REGRESSOR
        # Compute the ALE values
        ale_values = np.zeros(n_bins)
        feat_range_median = np.zeros(n_bins)
        tmp = 0
        ale_tmp = np.zeros(len(X))  # 和X等长的ale
        left_point = np.zeros(len(X))  # 每个X实例的该特征的bin的left——point值

        for i in range(n_bins):
            if i < n_bins - 1:
                condition_bin = (X[feature] >= feat_range[i]) & (
                        X[feature] < feat_range[i + 1]
                )  # bin 下的数据所在位置，左闭右开
            else:
                condition_bin = (X[feature] >= feat_range[i]) & (
                        X[feature] <= feat_range[i + 1]
                )  # 最后一个区间全闭
            X_bin = X[condition_bin]
            if len(X_bin):
                # Get the average prediction of the samples in the i-th bin
                bin_mean = model.predict(X_bin).mean()
                # 使用左端点替换bin内特征值
                X_bin_repl = X_bin.copy()
                X_bin_repl[feature] = feat_range[i]

                # Get the average prediction of the samples in the i-th bin
                # after replacing the feature values
                bin_mean_repl = model.predict(X_bin_repl).mean()

                # Compute the ALE value for the i-th bin
                ale_values[i] = bin_mean - bin_mean_repl

            else:  # 该bin没有数据置0
                ale_values[i] = 0

            tmp += ale_values[i]
            ale_tmp[condition_bin] = tmp
            left_point[condition_bin] = feat_range[i]
            feat_range_median[i] = (feat_range[i] + feat_range[i + 1]) / 2
        ale_values_acc = np.cumsum(ale_values)
        return pd.DataFrame(
            np.hstack(
                (feat_range_median.reshape(-1, 1), ale_values_acc.reshape(-1, 1))
            ),
            columns=["feature_value", "ale"],
        )
    if model_type == "classifier":
        # FOR CLASSIFIER
        # Compute the ALE values
        num_class = model.predict_proba(X).shape[1]
        ale_values = np.zeros((n_bins, num_class))
        feat_range_median = np.zeros(n_bins)
        tmp = np.zeros(num_class)
        ale_tmp = np.zeros((len(X), num_class))  # 和X等长的ale
        left_point = np.zeros(len(X))  # 每个X实例的该特征的bin的left——point值

        for i in range(n_bins):
            if i < n_bins - 1:
                condition_bin = (X[feature] >= feat_range[i]) & (
                        X[feature] < feat_range[i + 1]
                )  # bin 下的数据所在位置，左闭右开
            else:
                condition_bin = (X[feature] >= feat_range[i]) & (
                        X[feature] <= feat_range[i + 1]
                )  # 最后一个区间全闭
            X_bin = X[condition_bin]

            if len(X_bin):
                # Get the average prediction of the samples in the i-th bin
                bin_mean = np.mean(model.predict_proba(X_bin), axis=0)
                # 使用左端点替换bin内特征值
                X_bin_repl = X_bin.copy()
                X_bin_repl[feature] = feat_range[i]

                # Get the average prediction of the samples in the i-th bin after replacing the feature values
                bin_mean_repl = np.mean(model.predict_proba(X_bin_repl), axis=0)

                # Compute the ALE value for the i-th bin
                ale_values[i] = bin_mean - bin_mean_repl

            else:  # 该bin没有数据置0
                ale_values[i] = 0

            tmp += ale_values[i]
            ale_tmp[condition_bin] = tmp
            left_point[condition_bin] = feat_range[i]
            feat_range_median[i] = (feat_range[i] + feat_range[i + 1]) / 2
        ale_values_acc = np.cumsum(ale_values, axis=0)
        return pd.DataFrame(
            np.hstack((feat_range_median.reshape(-1, 1), ale_values_acc)),
            columns=["feature_value"] + [str(i) for i in range(num_class)],
        )


def hist_max_height(data, bins=20):
    """直方图最高的柱子的高度

    Args:
        data (array_like): 输入数据
        bins (int, optional): 直方图柱子数量. Defaults to 20.

    Returns:
        最高柱子高度
    """
    hist, bin_edges = np.histogram(a=data, bins=bins)
    return np.max(hist)


def ale(X: pd.DataFrame, feature: str, model, feature_type: str):
    """ale值计算，合并连续变量和离散变量

    Args:
        X (pd.DataFrame): 要求和model要求的输入的形状相同
        feature (str): 特征名称
        model : 现支持sklearn接口的机器学习模型
        feature_type (str): 特征类型，["categorical", "cat"]为离散型，["numeric", "num"]表示连续型

    Returns:
       ale值计算结果
    """
    if feature_type in ["categorical", "cat"]:
        return cat_ale(X, model, feature)
    elif feature_type in ["numeric", "num"]:
        return num_ale(X, feature, model)


def ale_main(X: pd.DataFrame, model, cat_cols: List[str]):
    """ale值计算，合并分类和回归

    Args:
        X (pd.DataFrame): 要求和model要求的输入的形状相同
        model : 现支持sklearn接口的机器学习模型
        cat_cols (List[str]): 离散型变量的列表

    Returns:
        返回y_value, feature, feature_value, ale四列
    """
    # 特征列表
    feature_list = []
    if hasattr(model, "feature_names_in_"):
        feature_list = model.feature_names_in_
        if isinstance(feature_list, np.ndarray):
            feature_list = feature_list.tolist()
    elif hasattr(model, "feature_name_"):
        feature_list = model.feature_name_

    # model类型
    if hasattr(model, "predict_proba"):
        MODEL_TYPE = "classifier"
    else:
        MODEL_TYPE = "regressor"

    # 结果计算
    if MODEL_TYPE == "regressor":
        # 回归
        result_df = pd.DataFrame(columns=["feature", "feature_value", "ale"])
        for feature in feature_list:
            print()
            if feature in cat_cols:
                FEATURE_TYPE = "cat"
            else:
                FEATURE_TYPE = "num"
            ale_df = ale(X, feature, model, FEATURE_TYPE)
            ale_df["feature"] = feature
            result_df = pd.concat([result_df, ale_df])

        result_df["y_value"] = "默认目标"  # 回归的多添加一列空值
        result_df = result_df.reset_index(drop=True)
        # 返回y_value, feature，feature_value, ale四列
        return result_df[["y_value", "feature", "feature_value", "ale"]]
    elif MODEL_TYPE == "classifier":
        # 分类
        result_df = pd.DataFrame(columns=["y_value", "feature", "feature_value", "ale"])
        for feature in feature_list:
            if feature in cat_cols:
                FEATURE_TYPE = "cat"
            else:
                FEATURE_TYPE = "num"
            ale_df = ale(X, feature, model, FEATURE_TYPE)

            # 意思对feature_value之后以类名命名的列拆解
            unstack_se = ale_df.set_index("feature_value").unstack()
            unstack_se.index.names = ["y_value", "feature_value"]
            unstack_df = unstack_se.reset_index().rename(columns={0: "ale"})
            unstack_df["feature"] = feature
            result_df = pd.concat([result_df, unstack_df])
        result_df = result_df.reset_index(drop=True)
        # 返回y_value, feature，feature_value, ale四列
        return result_df[["y_value", "feature", "feature_value", "ale"]]


def shap_value(X: pd.DataFrame, model):
    """shap值的计算

    Args:
        X (pd.DataFrame): 要求必须有cust_code列和模型要求的输入列
        model : 现支持sklearn接口的机器学习模型

    Returns:
        返回dataframe，列包括['cust_code','y_value', 'feature', 'shap_value']
    """
    # feature_list
    feature_list = []
    if hasattr(model, "feature_names_in_"):
        feature_list = model.feature_names_in_
        if isinstance(feature_list, np.ndarray):
            feature_list = feature_list.tolist()
    elif hasattr(model, "feature_name_"):
        feature_list = model.feature_name_

    explainer = Explainer(model)
    shap_values = explainer.shap_values(X[feature_list])  # shap值计算
    if isinstance(shap_values, list):  # 分类的会给出list
        shap_df = pd.DataFrame(columns=feature_list + ["y_value", "cust_code"])
        for i in range(len(shap_values)):
            df_t = pd.DataFrame(shap_values[i], columns=feature_list)
            df_t["y_value"] = str(i)
            df_t["cust_code"] = X["cust_code"]
            shap_df = pd.concat(
                [shap_df.reset_index(drop=True), df_t.reset_index(drop=True)]
            )
    else:  # 回归的多添加一列空值
        shap_df = pd.DataFrame(shap_values, columns=feature_list)
        shap_df["cust_code"] = X["cust_code"]
        shap_df["y_value"] = "默认目标"  # 回归的多添加一列空值

    shap_series = shap_df.set_index(["cust_code", "y_value"]).stack()  # 将带特征的列叠起来
    shap_series.index.names = ["cust_code", "y_value", "feature"]  # 赋予前几列名字
    # 最后输出列：['cust_code','y_value', 'feature', 'shap_value']
    result_df = shap_series.reset_index().rename(columns={0: "shap_value"})
    return result_df


def explain_main(X: pd.DataFrame, model, cat_cols: List[str]):
    """计算ale值和shap值，合并
    # user_id, target, feature_contribution
    # 1123, 2, {"age": 0.2, "tall": 0.3}

    Args:
        X (pd.DataFrame): 要求必须有cust_code列和模型要求的输入列
        model : 列现支持sklearn接口的机器学习模型
        cat_cols (List[str]): 离散型变量的list

    Returns:
        返回两个dataframe，ale_df[['target', 'feature', 'feature_trends']],形如
        2, “age”, {0.1: 0.2, 0.2: 0.3}
         shap_df[['cust_code', 'target', 'feature_contribution']]，形如
        1123, 2, {"age": 0.2, "tall": 0.3}
    """
    ale_df = ale_main(X.drop(columns=["cust_code"]), model, cat_cols)
    ale_df["feature_trends"] = ale_df.apply(lambda row: {row[2]: row[3]}, axis=1)
    ale_df = ale_df.rename(columns={"y_value": "target"})
    # ale值和shap值最后两列分别做字典，新成一列
    shap_df = shap_value(X, model)
    shap_df["feature_contribution"] = shap_df.apply(
        lambda row: {row[2]: row[3]}, axis=1
    )
    shap_df = shap_df.rename(columns={"y_value": "target"})
    return (
        ale_df[["target", "feature", "feature_trends"]],
        shap_df[["cust_code", "target", "feature_contribution"]],
    )
