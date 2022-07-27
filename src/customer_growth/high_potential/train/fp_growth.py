import operator
import time
import json
import logging
import sys

from fpgrowth_py.utils import *


def calculate_rule(input_file, result_file, min_sup=20, min_conf=0.01):
    itemSetList = input_file
    # with open(input_file) as fi:
    #     for line in fi:
    #         json_obj = json.loads(line)
    #         items = json_obj["items"]
    #         items = set(items)
    #         itemSetList.append(items)
    frequency = getFrequencyFromList(itemSetList)
    fpTree, headerTable = constructTree(itemSetList, frequency, min_sup)

    freqItems = []
    mineTree(headerTable, min_sup, set(), freqItems)
    rules = associationRule(freqItems, itemSetList, min_conf)

    fpTree, headerTable = constructTree(itemSetList, frequency, min_sup)

    if fpTree:
        freqItems = []
        mineTree(headerTable, min_sup, set(), freqItems)
        rules = associationRule(freqItems, itemSetList, min_conf)
    print(rules)
    cat_rules = {}
    for rule in rules:
        d = {}
        k = rule[0].pop()
        cat = rule[1].pop()
        confidence = rule[2]  # a -> b置信度
        if cat not in d and cat != -1:
            suport = headerTable[cat][0] / len(itemSetList)  # b 的支持度
            lift = confidence / suport  # 提升度 =   置信度(ab) / 支持度(b)  ,提升度 > 1.
            if 1. < lift:
                confidence = max(d.get(cat, 0.), lift)
                rs = cat_rules.get(k, [])
                rs.append((cat, confidence))
                cat_rules[k] = rs
                d[k] = confidence




    res_rules = dict([(k, sorted(v, key=lambda t: t[1], reverse=True))[:10] for k, v in cat_rules.items()])
    if -1 in res_rules:
        res_rules.pop(-1)  # 去除 cat = -1 的项目
    # with open(result_file, "w") as fo:
    #     for k, v in res_rules.items():
    #         fo.write(f"{json.dumps({'item': k, 'association_items': v})}\n")
    # print(f'cat association rule size is : {len(res_rules)}')
    return res_rules

#加载简单数据集
def loadSimpDat():
    simpDat = [['r', 'z', 'h', 'j', 'p'],
               ['z', 'y', 'x', 'w', 'v', 'u', 't', 's'],
               ['z'],
               ['r', 'x', 'n', 'o', 's'],
               ['y', 'r', 'x', 'z', 'q', 't', 'p'],
               ['y', 'z', 'x', 'e', 'q', 's', 't', 'm']]
    simpDat = [set(itemList) for itemList in simpDat]
    return simpDat

calculate_rule(loadSimpDat(), 'rules.json', min_sup=3, min_conf=0.7)