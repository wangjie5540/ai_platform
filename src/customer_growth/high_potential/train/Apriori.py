import operator
import time

import pandas as pd

"""
Aproiri算法内存消耗较大，对于较大数据量可以考虑PCY算法
除此之外，用于搜索频繁项集的数据应该是有限的，如何选择数据能够覆盖尽可能多的隐藏关联规则？
"""


#加载数据集
def loadDataSet():
    return [['r', 'z', 'h', 'j', 'p'],
               ['z', 'y', 'x', 'w', 'v', 'u', 't', 's'],
               ['z'],
               ['r', 'x', 'n', 'o', 's'],
               ['y', 'r', 'x', 'z', 'q', 't', 'p'],
               ['y', 'z', 'x', 'e', 'q', 's', 't', 'm']]

#选取数据集的非重复元素组成候选集的集合C1
def createC1(dataSet):
    C1=[]
    for transaction in dataSet:
        for item in transaction:
            if [item] not in C1:
                C1.append([item])
    C1.sort()
    return list(map(frozenset,C1))

#由Ck产生Lk：扫描数据集D，计算候选集Ck各元素在D中的支持度，选取支持度大于设定值的元素进入Lk
def scanD(D,Ck,minSupport):
    ssCnt={}
    for tid in D:
        for can in Ck:
            if can.issubset(tid):

                ssCnt[can]=ssCnt.get(can,0)+1
    numItems=float(len(D))
    retList=[]
    supportData={}
    for key in ssCnt.keys():
        support=ssCnt[key]/numItems
        if support>=minSupport:
            retList.insert(0,key)
        supportData[key]=support
    return retList,supportData

#由Lk产生Ck+1
def aprioriGen(Lk,k):
    retList=[]
    lenLk=len(Lk)
    for i in range(lenLk):
        for j in range(i+1,lenLk):
            L1=list(Lk[i])[:k-2];L2=list(Lk[j])[:k-2]
            L1.sort();L2.sort()
            if L1==L2:
                retList.append(Lk[i]|Lk[j])
    return retList

#Apriori算法主函数
def apriori(dataSet,minSupport=0.5):
    C1=createC1(dataSet)
    D=list(map(set,dataSet))
    L1,supportData=scanD(D,C1,minSupport)
    L=[L1]
    k=2
    while len(L[k-2])>0:
        Ck=aprioriGen(L[k-2],k)
        Lk,supK=scanD(D,Ck,minSupport)
        supportData.update(supK)
        L.append(Lk)
        k+=1
    return L,supportData

# 主函数，由频繁项集以及对应的支持度，得到各条规则的置信度，选择置信度满足要求的规则为关联规则
# 为了避免将所有数据都对比一遍，采用与上述相同的逻辑减少计算量——一层一层计算筛选
def generateRules(L,supportData,minConf=0.7):
    bigRuleList=[]
    for i in range(1,len(L)):
        for freqSet in L[i]:
            H1=[frozenset([item]) for item in freqSet] # H1是频繁项集单元素列表，是关联规则中a->b的b项
            if i>1:
                rulesFromConseq(freqSet,H1,supportData,bigRuleList,minConf)
            else:
                calConf(freqSet,H1,supportData,bigRuleList,minConf)
    return bigRuleList

# 置信度计算函数
def calConf(freqSet,H,supportData,brl,minConf=0.7):
    prunedH=[]
    for conseq in H:
        conf=supportData[freqSet]/supportData[freqSet-conseq]
        if conf>=minConf:
            print (freqSet-conseq,'-->',conseq,'conf:',conf)
            brl.append([freqSet-conseq,conseq,conf])
            prunedH.append(conseq)
    return prunedH

# 关联规则合并函数
def rulesFromConseq(freqSet,H,supportData,brl,minConf=0.7):
    m=len(H[0])
    if len(freqSet)>(m+1):
        Hmp1=aprioriGen(H,m+1)
        Hmp1=calConf(freqSet,Hmp1,supportData,brl,minConf)
        if len(Hmp1)>1:
            rulesFromConseq(freqSet,Hmp1,supportData,brl,minConf)

def getRelatedItem(dataset, minSupport, minConf, itemLimits, targetItem):
    C1 = createC1(dataset)
    D = list(map(set, dataset))
    start = time.time()
    L1, supportData0 = scanD(D, C1, minSupport=minSupport)
    L, supportData = apriori(dataset, minSupport=minSupport)
    print(L)
    print(supportData)
    rules = generateRules(L, supportData, minConf=minConf)
    end = time.time()
    items = set()
    for rule in rules:
        print(rule[0])
        if set(targetItem).issubset(rule[1]):
            items = items | rule[0]
    print(f'Aproiri算法运行时间:{end-start}s')
    if itemLimits:
        return list(items)[:itemLimits]
    else:
        return items


def getItemFromDataFrame(dataset: pd.DataFrame, column, samples_num, minSupport, minConf, targetItem, itemLimits):
    if len(dataset) < samples_num:                  #transform to List and sample a number of pruchase records instead of the whole
        list_candidates = dataset[column].tolist()
    else:
        list_candidates = dataset.sample(n=samples_num)[column].tolist()
    return getRelatedItem(list_candidates, minSupport, minConf, itemLimits, targetItem)



""""
miss cold
hand out tickets 
reception 
adapt a novel in the play


"""
dataset=loadDataSet()
res = getRelatedItem(dataset, 0.5, 0.8, 5, 'x')
print(res)
# C1=createC1(dataset)
# D=list(map(set,dataset))
# start = time.time()
# L1,supportData0=scanD(D,C1,0.5)
# L,supportData=apriori(dataset,minSupport=0.5)
# rules=generateRules(L,supportData,minConf=0.8)
# end = time.time()
# i = 1
# for lk in L:
#     print('频繁项集', i, '的个数', len(lk))
#     i += 1
# print('规则数为', len(rules))
# print(rules)
# print('程序运行时间', end-start, 's') # 0.0009970664978027344 s