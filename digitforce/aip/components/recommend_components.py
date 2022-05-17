


######################## recall ##############################
def deep_mf_op(input_file, item_embeding_file, user_embeding_file, image_tag="latest"):
    '''
    通过神经网络训练user_emb矩阵和item矩阵
    训练样本格式：
        user_id, item_id, score 分隔符为 ','
        user_id 为 int 类型
        item_id 为 int 类型
        score 为 int 类型
    训练结果会保存成文件
    用户向量结果保存格式：
        user_id, user_vec
    item向量结果保存格式：
        item_id, item_vec

    :param input_file: 训练样本路径
    :param item_embeding_file: item向量保存路径
    :param user_embeding_file: 用户向量保存路径
    :param image_tag: 组件版本
    :return: deep_mf_op
    '''
    return dsl.ContainerOp(name="deep_mf'",
                           image="digit-force-docker.pkg.coding.net/ai-platform/ai-src/src-recommend-recall-mf-deep_mf" + f":{image_tag}",  # todo
                           command="python",
                           arguments=["main.py", input_file, item_embeding_file, user_embeding_file])


def user_profile_calculator_op(input_file, user_profile_file, image_tag="latest"):
    '''
    根据用户的行为计算用户在不同兴趣上的得分
    **用户行为需要按照user_id排序**
    输入文件格式:
        user_id, item_id, profile_id, click_cnt, share_cnt, add_cnt, event_timestamp
    输出文件格式:
        user_id {"profile_1":score_1, ...} 分隔符为 \t

    :param input_file: 用户行为文件
    :param user_profile_file: 用户兴趣得分分布
    :param image_tag: 组件版本
    :return: user_profile_calculator_op
    '''
    return dsl.ContainerOp(name="user_profile_calculator",
                           image="digit-force-docker.pkg.coding.net/ai-platform/ai-src/src-recommend-user_profile-user_profile_caculator" + f":{image_tag}",
                           command="python",
                           arguments=["main.py", input_file, user_profile_file])


def user_profile_recall_op(input_file, output_file, profile_and_hot_item_file, image_tag="latest"):
    '''
    根据用户的兴趣在热门商品中召回
    输入文件格式：
        行 json字符串 {user_id: xxx, profile_scores:{profile_id:score, ...}}
    profile_and_hot_item_file:
       行 json字符串 {profile_id: xxx, hot_item_ids:[item_id1, item_id2, ...] }
    输出文件：
        行 json字符串  {user_id: xxx, recall_item_ids: [item_id1, item_id2, ...] }
    :param input_file: 用户兴趣文件
    :param output_file: 召回结果文件
    :param profile_and_hot_item_file: 各个profile上的热门排序
    :param image_tag: 组件版本
    :return:
    '''
    return dsl.ContainerOp(name="deep_mf'",
                           image="digit-force-docker.pkg.coding.net/ai-platform/ai-src/src-recommend-recall-user_profile-user_profile_recall" + f":{image_tag}",
                           command="python",
                           arguments=["main.py", input_file, output_file, profile_and_hot_item_file],
                           )

a = user_profile_recall_op()
a.add_pvolumes()
######################## hot ########################################
def ctr_hot_op(input_file, output_file):
    pass


def click_hot_op(input_file, output_file):
    pass
