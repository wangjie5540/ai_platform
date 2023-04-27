from pandas import DataFrame
from shap import Explainer
import pandas as pd
import numpy as np
from typing import List, Tuple, Any

import matplotlib.pyplot as plt

plt.rcParams["font.sans-serif"] = ["SimHei"]  # 设置字体
plt.rcParams["axes.unicode_minus"] = False  # 该语句解决图像中的“-”负号的乱码问题

import json


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
        # TODO：目前只按照方法不同分为分类和回归，后续待优化
        model_type = "classifier"
    else:
        model_type = "regressor"
    if model_type == "classifier":
        class_cols = model.classes_.tolist()  # 预测的每个类的列名称
        if not len(x_test):
            # 添加对离散型变量的空值处理
            print("该离散型变量的除none以外的预测样本集为空")
            return pd.DataFrame(columns=["feature_value"] + class_cols)
        predict_prob = model.predict_proba(x_test)
        num_class = predict_prob.shape[1]
        pred_dataframe = pd.DataFrame(predict_prob, columns=class_cols)
        pred_dataframe = pd.concat(
            [x_test.reset_index(drop=True), pred_dataframe], axis=1
        )  # 在预测样本数据后添加每个类别对应的预测概率
        cat_var_sorted = pred_dataframe[cat_var].unique()  # 对离散值进行排序 TODO:离散变量排序方式优化

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
        if not len(x_test):
            # 添加对离散型变量的空值处理
            print("该离散型变量的除none以外的预测样本集为空")
            return pd.DataFrame(columns=["feature_value", "ale"])
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
def num_ale(X: pd.DataFrame, feature: str, model, n_bins=10):
    """连续型变量的ale值

    Args:
        X (pd.DataFrame): 需要与模型需要输入的数据形状相同
        feature (str): 连续型特征名称
        model : 现支持sklearn接口的机器学习模型
        n_bins (int, optional): 分块数量，也是折线图数据点数量，数字越大，折线走势越受到数据分布的影响
        ，数字越小越不容易被数据分布影响. Defaults to 10.

    Returns:
        回归返回每个bins的中位数对应的ale值，[['cat_var','ale_val']]
        ，分类返回每个枚举值对应的每个类别的ale值：[['cat_var',class_cols]]
    """
    # 计算端点值，TODO:分箱方式优化
    seg_len = (np.max(X[feature]) - np.min(X[feature])) / n_bins
    feat_range = (
        np.linspace(np.min(X[feature]), np.max(X[feature]) - seg_len, n_bins)
        # 左端点值
    )
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
        num_class = model.classes_.shape[0]
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
                feat_range_median[i] = (feat_range[i] + feat_range[i + 1]) / 2  # 区间中位数
            else:
                condition_bin = X[feature] >= feat_range[i]  # 最后一个区间
                feat_range_median[i] = (feat_range[i] + np.max(X[feature])) / 2  # 区间中位数
            X_bin = X[condition_bin]

            if len(X_bin):
                # Get the average prediction of the samples in the i-th bin
                bin_mean = np.mean(model.predict_proba(X_bin), axis=0)
                # 使用左端点替换bin内特征值
                X_bin_repl = X_bin.copy()
                X_bin_repl[feature] = feat_range[i]

                # Get the average prediction of the samples in the i-th bin
                # after replacing the feature values
                bin_mean_repl = np.mean(model.predict_proba(X_bin_repl), axis=0)

                # Compute the ALE value for the i-th bin
                ale_values[i] = bin_mean - bin_mean_repl

            else:  # 该bin没有数据置0
                ale_values[i] = 0

            tmp += ale_values[i]
            ale_tmp[condition_bin] = tmp
            left_point[condition_bin] = feat_range[i]
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
        # 去除nan和None,空值不进行离散型变量的ale计算
        X_copy = X.copy().dropna(axis=0, how='any')
        X_copy = X_copy[~(X_copy[feature].isin(['nan', 'None']))]
        return cat_ale(X_copy, model, feature)
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

    result_df = None  # 事先添加结果计算的变量
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
            ale_df = ale(X[feature_list], feature, model, FEATURE_TYPE)  # 计算ale值
            ale_df["feature"] = feature
            result_df = pd.concat([result_df, ale_df])

        result_df["y_value"] = "默认目标"  # 回归的多添加一列空值
    elif MODEL_TYPE == "classifier":
        # 分类
        result_df = pd.DataFrame(columns=["y_value", "feature", "feature_value", "ale"])
        for feature in feature_list:
            if feature in cat_cols:
                FEATURE_TYPE = "cat"
            else:
                FEATURE_TYPE = "num"
            ale_df = ale(X[feature_list], feature, model, FEATURE_TYPE)

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
    result_df = shap_series.reset_index().rename(columns={0: "shap_value"})
    # reindex后输出列：['cust_code','y_value', 'feature', 'shap_value']
    print("result_df.iloc[:10]_0\n", result_df.iloc[:10])
    print("X[X['cust_code']=='10001']\n", X[X['cust_code'] == '10001'])
    result_df = result_df.merge(X, on="cust_code", how="left")
    print("result_df.iloc[:10]_1\n", result_df.iloc[:10])
    result_df["feature_value"] = (
        result_df.apply(lambda x: x[x["feature"]], axis=1)
    )  # 添加feature_value列
    result_df = result_df[["cust_code", "y_value", "feature", "shap_value", "feature_value"]]
    print("result_df.iloc[:10]_2\n", result_df.iloc[:10])
    return result_df


def explain(X: pd.DataFrame, model, cat_cols: List[str], feature_cname_dict: dict) -> Tuple[
    pd.DataFrame, pd.DataFrame]:
    """计算ale值和shap值，合并
    # user_id, target, feature_contribution
    # 1123, 2, {"age": 0.2, "tall": 0.3}

    Args:
        feature_cname_dict: 特征中文名字字典
        X (pd.DataFrame): 要求必须有cust_code列和模型要求的输入列
        model : 列现支持sklearn接口的机器学习模型
        cat_cols (List[str]): 离散型变量的list

    Returns:
        返回两个dataframe，ale_df[['target', 'feature', 'feature_trends']],形如
        2, “age”, {0.1: 0.2, 0.2: 0.3}
         shap_df[['cust_code', 'target', 'feature_contribution']]，形如
        1123, 2, '{"age": 0.2}'
    """
    ale_df = ale_main(X.drop(columns=["cust_code"]), model, cat_cols)
    ale_df["feature_trends"] = ale_df.apply(lambda row: {row[2]: float(row[3])}, axis=1)
    ale_df = ale_df.rename(columns={"y_value": "target"})
    ale_df['feature_cname'] = ale_df['feature'].map(feature_cname_dict)  # 特征中文名字

    # ale值和shap值最后两列分别做字典，新成一列
    shap_df = shap_value(X, model)
    shap_df["feature_contribution"] = shap_df.apply(
        lambda row: {row[2]: float(row[3])}, axis=1
    )
    shap_df = shap_df.rename(columns={"y_value": "target"})
    shap_df['feature_cname'] = shap_df['feature'].map(feature_cname_dict)  # 特征中文名字
    return (
        ale_df[["target", "feature", "feature_trends", "feature_cname"]],
        shap_df[["cust_code", "target", "feature_contribution", "feature_cname", "feature_value"]],
    )


def ale_to_json(df, group_cols):
    """将dataframe转换成json格式

    Args:
        df (pd.DataFrame): 要求必须有target列
        group_cols (List[str]): 要groupby的列

    Returns:
        返回json格式的字符串
    """
    df['target'] = df['target'].astype(int)
    # Group the DataFrame by target and feature
    grouped = df.groupby(group_cols)

    # output
    output = []

    group_cols_d = {}
    feature_method = df.columns[2]

    def ale_description(value: dict) -> str:
        """ale值的"解释说明"

        Args:
            value: 输入value = {
                    "x": [23, 27, 31, 35, 39, 43, 47, 51, 55, 59],
                    "y": [0.1, 0.2, 0.3, 0.4, 0.6, 0.7, 0.5, 0.5, 0.4, 0.3],
                    }

        Returns:
            dict: 返回解释说明的字符串

        """

        def find_consecutive_ranges(numbers: list) -> list:
            """找到连续的递增的间隔为1的序列

            Args:
                numbers (list): _description_

            Returns:
                list: _description_
            """
            ranges = []
            range_start = numbers[0]
            range_end = numbers[0]
            for num in numbers[1:]:
                if num == range_end + 1:
                    range_end = num
                else:
                    ranges.append((range_start, range_end))
                    range_start = range_end = num
            ranges.append((range_start, range_end))
            return ranges

        value_x = value["x"]
        value_y = np.array(value["y"])

        # 异常值
        mean = np.mean(value_y)
        std_deviation = np.std(value_y)
        threshold = 3 * std_deviation
        outliers = [(index, value) for index, value in enumerate(value_y) if
                    abs(value - mean) > threshold]

        # 强趋势点
        # strong_trend_points = []
        # for ix in range(len(y) - 2):
        #     if y[ix] < y[ix + 1] < y[ix + 2]:
        #         strong_trend_points.append((ix + 1, "上涨"))
        #     elif y[ix] > y[ix + 1] > y[ix + 2]:
        #         strong_trend_points.append((ix + 1, "下降"))

        strong_trend_points = []
        for ix in range(len(value_y) - 1):
            if value_y[ix] < value_y[ix + 1]:
                strong_trend_points.append((ix + 1, "上涨"))
            elif value_y[ix] > value_y[ix + 1]:
                strong_trend_points.append((ix + 1, "下降"))

        # 变革点
        change_points = []
        for ix in range(len(value_y) - 2):
            if value_y[ix] < mean and value_y[ix + 1] < mean and value_y[ix + 2] < mean:
                change_points.append((ix + 1, "均值以下"))
            elif value_y[ix] > mean and value_y[ix + 1] > mean and value_y[ix + 2] > mean:
                change_points.append((ix + 1, "均值以上"))

        # 最值点
        max_index = np.argmax(value_y)
        min_index = np.argmin(value_y)
        max_point = (value_x[max_index], value_y[max_index])
        min_point = (value_x[min_index], value_y[min_index])

        # 结论
        conclusion = "结论：\n"

        # 异常值范围
        if outliers:
            high_outliers = [value for index, value in outliers if value > mean]
            low_outliers = [value for index, value in outliers if value < mean]
            if high_outliers:
                conclusion += f"存在高异常值：{high_outliers}\n"
            if low_outliers:
                conclusion += f"存在低异常值：{low_outliers}\n"

        # 强趋势点范围
        up_trends = [index for index, trend in strong_trend_points if trend == "上涨"]
        down_trends = [index for index, trend in strong_trend_points if trend == "下降"]

        if up_trends:  # 检查是否存在上涨强趋势点范围
            up_trend_ranges = find_consecutive_ranges(up_trends)
            for start, end in up_trend_ranges:
                if end - start >= 3:
                    conclusion += f"存在上涨强趋势点范围：{value_x[start - 1]} - {value_x[end - 1]}\n"
                elif end - start == 2:
                    conclusion += f"存在上涨弱趋势点范围：{value_x[start - 1]} - {value_x[end - 1]}\n"

        if down_trends:  # 检查是否存在下降强趋势点范围
            down_trend_ranges = find_consecutive_ranges(down_trends)
            for start, end in down_trend_ranges:
                if end - start >= 3:
                    conclusion += f"存在下降强趋势点范围：{value_x[start - 1]} - {value_x[end - 1]}\n"
                elif end - start == 2:
                    conclusion += f"存在下降弱趋势点范围：{value_x[start - 1]} - {value_x[end - 1]}\n"

        if not up_trends and not down_trends:
            conclusion += "不存在上涨或下降趋势点范围\n"

        # 变革点范围
        below_mean_changes = [index for index, position in change_points if position == "均值以下"]
        above_mean_changes = [index for index, position in change_points if position == "均值以上"]

        if below_mean_changes:
            below_mean_change_ranges = find_consecutive_ranges(below_mean_changes)
            for start, end in below_mean_change_ranges:
                if end - start >= 3:
                    conclusion += f"存在均值以下变革点范围：{value_x[start - 1]} - {value_x[end - 1]}\n"
                elif end - start == 2:
                    conclusion += f"存在均值以下弱变革点范围：{value_x[start - 1]} - {value_x[end - 1]}\n"

        if above_mean_changes:
            above_mean_change_ranges = find_consecutive_ranges(above_mean_changes)
            for start, end in above_mean_change_ranges:
                if end - start >= 3:
                    conclusion += f"存在均值以上变革点范围：{value_x[start - 1]} - {value_x[end - 1]}\n"
                elif end - start == 2:
                    conclusion += f"存在均值以上弱变革点范围：{value_x[start - 1]} - {value_x[end - 1]}\n"

        if not below_mean_changes and not above_mean_changes:
            conclusion += "不存在均值以下或均值以上变革点范围\n"

        if np.unique(value_y).size == 1:
            conclusion += "所有值相同，无法分析\n"
        elif np.unique(value_y).size == 2:
            conclusion += "所有值相近，无法分析\n"
        else:
            # 最值点
            conclusion += f"""
            最大值点：位置 {max_point[0]}，值 {max_point[1]}；
            最小值点：位置 {min_point[0]}，值 {min_point[1]}\n
            """

            # 分析趋势
            trends = []
            if 0.1 * len(value_y) < max_index < 0.9 * len(value_y):
                if max_index == 0 and value_y[max_index] > value_y[max_index + 1]:
                    trends.append("最大值点附近为峰值")
                elif max_index == len(value_y) - 1 and value_y[max_index - 1] < value_y[max_index]:
                    trends.append("最大值点附近为峰值")
                elif value_y[max_index - 1] < value_y[max_index] > value_y[max_index + 1]:
                    trends.append("最大值点附近为峰值")
            elif max_index > 0.1 * len(value_y):
                if value_y[max_index - 1] < value_y[max_index]:
                    trends.append("最大值点左侧上升")
            if max_index < 0.9 * (len(value_y) - 1):
                if value_y[max_index] > value_y[max_index + 1]:
                    trends.append("最大值点右侧下降")

            for trend in trends:
                conclusion += f"{trend}\n"

        return conclusion

    # Loop over the groups and populate the output dictionary
    for (target, feature, feature_cname), data in grouped:

        if target not in group_cols_d:
            class_d = {group_cols[0]: str(target), feature_method: []}
            output.append(class_d)
            group_cols_d[target] = str(feature)

        x_result = []
        y_result = []
        for i in range(len(data)):
            x = list(data[feature_method].iloc[i].keys())[0]
            x_result.append(x)
            y = list(data[feature_method].iloc[i].values())[0]
            y_result.append(y)

        feature_method_d = {
            group_cols[1]: feature,
            group_cols[2]: feature_cname,
            "coordinates": {
                "x": x_result,
                "y": y_result
            },
        }
        feature_method_d["description"] = ale_description(feature_method_d["coordinates"])

        output[target][feature_method].append(feature_method_d)

    for item in output:
        feature_trends = item['feature_trends']
        item['feature_trends'] = (
            sorted(feature_trends, key=lambda ft_: max(list(map(abs, ft_['coordinates']['y']))),
                   reverse=True)
        )  # 按照y的最大值对feature_trends内的每个特征的ale值进行排序
    return output


def shap_agg(df: pd.DataFrame):
    """

    将shape的结果按照user_code进行聚合，

    Args:
        df: shap的结果，必须有cust_code，target，feature_contribution, feature_cname, feature_value列
        ，形如
        [1123, 0, {"age": 0.2}, "年龄", 23]

    Returns:
        返回dataframe, 其中shapley列为json格式的字符串，形如
    [['user_code', 'shapley']']]
    [1123,
    '[{"target": "0", "feature_contributions": {"x": ["age", "tall"], "y": [0.2, 0.3]}},
     {"target": "1", "feature_contributions": {"x": ["age", "tall"], "y": [0.2, 0.3]}}]']

    """
    df = df.sort_values(by=['cust_code', 'target'])

    def user_target_agg(data) -> pd.DataFrame:
        """
        将同一个用户的同一个目标的shapley值进行合并
        Args:
            data:

        Returns:
            dict: 合并后的shapley值,形如{"age": 0.2, "tall": 0.3}
        """
        merge_d = {}
        for feature_shap_d in data['feature_contribution']:
            merge_d = {**merge_d, **feature_shap_d}  # 合并字典
        feature_cname_list = list(data['feature_cname'])  # 中文名列表
        feature_value_list = list(data['feature_value'])  # 特征的值列表
        return pd.DataFrame(
            {
                "feature_contribution": [merge_d],
                "feature_cname_list": [feature_cname_list],
                "feature_value_list": [feature_value_list]
            }
        )

    def shapley_description(value: dict) -> str:
        """生成shapley的描述

        Args:
            value: 传入字典，类似于 value = {
                                "x": ['年龄', '最近一周登录天数', '最近20个交易日银证转入金额',
                                      '持仓股票数量', '省份'],
                                "y": [-0.1, 0.2, 0.3, 0.4, -0.6],
                            }

        Returns:
            str: 返回描述字符串

        """

        x_labels = value['x']
        y_values = value['y']

        # 计算均值
        mean_value = np.mean(y_values)

        # 找到最大和最小点
        max_index = np.argmax(y_values)
        min_index = np.argmin(y_values)

        # 计算与均值同号的贡献最高的数据点
        if mean_value > 0:
            same_sign_indices = np.where(np.array(y_values) > 0)[0]
        else:
            same_sign_indices = np.where(np.array(y_values) < 0)[0]

        highest_contrib_index = same_sign_indices[
            np.argmax(np.abs(np.array(y_values)[same_sign_indices]))]

        result = (
            f"均值为: {mean_value:.2f}\n"
            f"最大点: {x_labels[max_index]} (正向贡献: {y_values[max_index]:.2f})\n"
            f"最小点: {x_labels[min_index]} (负向贡献: {y_values[min_index]:.2f})\n"
            f"与均值同号贡献最高的数据点: {x_labels[highest_contrib_index]} ({y_values[highest_contrib_index]:.2f})"
        )

        return result

    def get_shap_col(data):
        """
        获取shap列的值

        Args:
            data:

        Returns:

        """
        output = []
        for i in range(len(data)):
            feature_contributions_d = {
                "target": str(data["target"].iloc[i]),
                "feature_contributions": {
                    "x": list(data["feature_contribution"].iloc[i].keys()),
                    "y": list(data["feature_contribution"].iloc[i].values()),
                    "feature_cname": data["feature_cname_list"].iloc[i],
                    "feature_value": data["feature_value_list"].iloc[i],
                },
            }
            feature_contributions_d["description"] = shapley_description(
                feature_contributions_d["feature_contributions"])  # 生成描述

            output.append(feature_contributions_d)

        output = json.dumps(output)
        return output

    df = (
        df.groupby(['cust_code', 'target'])
        .apply(lambda x: user_target_agg(x))
        .reset_index()
    )
    df = df.groupby('cust_code').apply(lambda x: get_shap_col(x))
    df = df.reset_index().rename(columns={0: "shapley"})
    return df


def get_explain_result(X, model, cat_cols: List[str], feature_cname_dict: dict):
    """获取explain的结果

    Args:
        feature_cname_dict: 特征中文名字的字典
        X (pd.DataFrame): 要求必须有cust_code列和模型要求的输入列
        model : 列现支持sklearn接口的机器学习模型
        cat_cols (List[str]): 离散型变量的list

    Returns:
        返回ale的json格式和shap的dataframe,,形如
         shap_df[['cust_code', 'shapley']]，形如
        [1123,
    '[{"target": "0", "feature_contributions": {"x": ["age", "tall"], "y": [0.2, 0.3]}},
     {"target": "1", "feature_contributions": {"x": ["age", "tall"], "y": [0.2, 0.3]}}]']
    """
    ale_df, shap_df = explain(X, model, cat_cols, feature_cname_dict)
    ale_json = ale_to_json(ale_df, ["target", "feature", "feature_cname"])
    shap_df = shap_agg(shap_df)
    return ale_json, shap_df[['cust_code', 'shapley']]
