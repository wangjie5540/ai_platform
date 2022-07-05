import fcntl
import json
import math
import random
import sys
import time
from multiprocessing import Pool


class UserEvent:
    def __init__(self, user_id, item_id, event_type, profile_type, profile_id, event_timestamp):
        '''
        用户行为
        :param user_id: 用户id，可以为空
        :param item_id: item_id不能为空
        :param event_type: 行为类型，如：点击 加购 下单
        :param profile_type: profile类型，预先定义的用户兴趣或者标签。如：商品类别 基金种类
        :param profile_id: 具体的兴趣，如：男装 债券基金
        :param event_timestamp: 用户行为发生的时间
        '''
        self.user_id = user_id
        self.item_id = item_id
        self.event_type = event_type
        self.profile_type = profile_type
        self.profile_id = profile_id
        self.event_timestamp = event_timestamp


class ProfileCalculator:
    def __init__(self):
        pass

    def calculate_user_profile(self, user_event):
        pass

    def calculate_user_profile_score(self, user_events):
        pass


class UserProfileV1(ProfileCalculator):
    @staticmethod
    def get_action_weight(user_event: UserEvent):
        user_event_type = user_event.event_type
        if user_event_type == 'click' or user_event_type == 1:
            return 1
        if user_event_type == 'add_cart' or user_event_type == 2:
            return 2
        if user_event_type == 'order' or user_event_type == 10:
            return 5
        return 0

    @staticmethod
    def get_decay_rate(user_event: UserEvent):
        days = time.time() // 86400 - user_event.event_timestamp // 86400
        rate = 1 - math.log2(1 + days) / 6
        if rate > 0:
            return rate
        return 0

    def calculate_user_profile(self, user_event):
        score = UserProfileV1.get_action_weight(user_event) * UserProfileV1.get_decay_rate(user_event)
        score = max([score, 0.01])
        return score

    def calculate_user_profile_score(self, user_events):
        profile_and_score_map = {}
        for user_event in user_events:
            profile_id = user_event.profile_id
            score = profile_and_score_map.get(profile_id, 0)
            score += self.calculate_user_profile(user_event)
            profile_and_score_map[profile_id] = score
        return profile_and_score_map


def test_user_profile_v1():
    user_events = []
    for i in range(1000):
        user_events.append(UserEvent('', -1, i // 500, profile_type="small_category",
                                     profile_id=i // 800,
                                     event_timestamp=time.time() - random.random() * 12 * 24 * 3600))
    user_profile_v1 = UserProfileV1()
    s = user_profile_v1.calculate_user_profile_score(user_events)
    print(s)


def calculate_user_score_and_save(user_id, user_events, output_fo, calculator=None):
    user_and_profile = calculator.calculate_user_profile_score(user_events)
    fcntl.fcntl(output_fo.fileno(), fcntl.LOCK_EX)
    output_fo.write(f"{json.dumps({'user_id':user_id,'profile_scores':user_and_profile})}\n")
    fcntl.fcntl(output_fo.fileno(), fcntl.LOCK_UN)


def get_event_type(click_cnt, save_cnt, order_cnt):
    if order_cnt:
        return 10
    if save_cnt:
        return 2
    if click_cnt:
        return 1
    return 0


def calculate_user_profile(input_file, output_file):
    user_profile_v1_calculator = UserProfileV1()
    fo = open(output_file, "w")
    with open(input_file) as fi:
        cur_user_id = None
        user_events = []
        for line in fi:
            vals = line.strip().split(",")
            user_id = vals[0]
            item_id = vals[1]
            profile_id = vals[2]
            click_cnt = vals[3]
            save_cnt = vals[4]
            order_cnt = vals[5]
            event_timestamp = int(vals[6])
            event_type = get_event_type(click_cnt, save_cnt, order_cnt)
            u = UserEvent(user_id, item_id, event_type, "cid3", profile_id, event_timestamp)
            if cur_user_id is None:
                cur_user_id = user_id
                user_events.append(u)
            if user_id != cur_user_id:
                calculate_user_score_and_save(cur_user_id, user_events, fo, user_profile_v1_calculator)
                cur_user_id = user_id
                user_events = [u]
            else:
                user_events.append(u)
    if user_events:
        calculate_user_score_and_save(cur_user_id, user_events, fo, user_profile_v1_calculator)
    fo.close()


