from sklearn.metrics import roc_auc_score
import numpy as np

def twoStep(X, y, base_trainner, second_trainner):
    """
    Two-Step 样本优化，针对PU（positive-unlabeled)问题
    Args:
        X: 特征
        y: 标签（with 1 for positive, 0 for unlabeld)
        base_trainner: 基学习器(with fit, predict, predict_proba)
        second_trainner: PU learning使用的学习器(the same)

    Returns:
        Dataset after PU learning, with 1 for positive, 0 for relable negative; and second_trainner used to train PU dataset
    """

    # Keep the original targets safe for later
    y_orig = y.copy()

    # Unlabel a certain number of data points
    hidden_ratio = 0.1
    y.loc[
        np.random.choice(
            y[y == 1].index,
            replace=False,
            size=hidden_ratio * len(y)
        )
    ] = 0

    # Check the new contents of the set
    print('%d positive out of %d total' % (sum(y), len(y)))

    # Create a new target vector, with 1 for positive, -1 for unlabeled, and
    # 0 for "reliable negative"(there are no reliable negatives to start with)
    ys = 2 * y - 1

    # Get the scores from before
    pred = base_trainner.predict_proba(X)[:, 1]

    # Find the range of scores given to positive data points
    range_P = [min(pred * (ys > 0)), max(pred * (ys > 0))]

    model_list = []

    # Step1
    # If any unlabeled point has a socre above all known positives,
    # or bellow all known positives, label is accordingly
    iP_new = ys[(ys < 0) & (pred >= range_P[1])].index
    iN_new = ys[(ys < 0) & (pred <= range_P[0])].index
    ys.loc[iP_new] = 1
    ys.loc[iN_new] = 0

    # classifier to be used for step2
    # Limit to 10 iterations otherwise this approach will take a long time
    for i in range(15):
        model_list.append(second_trainner)
        # If step1 didn't find new labels, we're done
        if len(iP_new) + len(iN_new) == 0 and i > 0:
            print('break')
            break
        print('step1 labeled %d new positives and %d negatives.' % (len(iP_new), len(iN_new)))

        # print('positive index found:', iP_new)
        print('doing step2 .....')

        # step2
        # retrain on new labels and get new scores
        second_trainner.fit(X, ys)
        pred = second_trainner.predict_proba(X)[:, -1]
        # Find the range of socres given to positive data points
        range_P = [min(pred * (ys > 0)), max(pred * (ys > 0))]

        # repeat step1
        iP_new = ys[(ys < 0) & (pred >= range_P[1])].index
        iN_new = ys[(ys < 0) & (pred <= range_P[0])].index
        ys.loc[iP_new] = 1
        ys.loc[iN_new] = 0

    auc = roc_auc_score(pred, y)
    print(f'auc {auc}')
    return ys[ys>=0], second_trainner