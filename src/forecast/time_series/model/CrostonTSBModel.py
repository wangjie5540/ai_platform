# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    Croston_tsb
"""

import pandas as pd
import numpy as np
import datetime

class CrostonTSBModel():
    def __init__(self,data,param):
        self.data = data
        self.param = param

        param={
            "curDate":None,
            "extra_periods":None,
            "alpha":0,
            "beta":0,
            "a":None,
            "p":None,
            "f":None,
            "cols":None
        }
        param.update(self.param)

    def fit(self):
        arr_data = np.array(self.data)
        self.param["cols"] = len(arr_data)
        arr_data = np.append(arr_data,[np.nan]*self.param["extra_periods"])
        self.param["a"],self.param["p"],self.param["f"] = np.full((3,self.param["cols"]+self.param["extra_periods"]),np.nan)
        first_occurence = np.argmax(arr_data[:self.param["cols"]]>0)
        self.param["a"][0] = arr_data[first_occurence]
        self.param["p"][0] = 1/(1+first_occurence)
        self.param["f"][0] = self.param["a"][0]*self.param["p"][0]
        for t in range(0,self.param["cols"]):
            if arr_data[t]>0:
                self.param["a"][t+1] = self.param["alpha"]*arr_data[t]+(1-self.param["alpha"])*self.param["a"][t]
                self.param["p"][t+1] = self.param["beta"]*1 + (1-self.param["beta"])*self.param["p"][t]
            else:
                self.param["a"][t+1] = self.param["a"][t]
                self.param["p"][t+1] = self.param["p"][t]*(1-self.param["beta"])
            self.param["f"][t+1] = self.param["p"][t+1]*self.param["a"][t+1]

    def forecast(self,predict_period):
        self.param["a"][self.param["cols"]+1:self.param["cols"]+self.param["extra_periods"]] = self.param["a"][self.param["cols"]]
        self.param["p"][self.param["cols"]+1:self.param["cols"]+self.param["extra_periods"]] = self.param["p"][self.param["cols"]]
        self.param["f"][self.param["cols"]+1:self.param["cols"]+self.param["extra_periods"]] = self.param["f"][self.param["cols"]]
        self.param["f"] = self.param["f"][-7:]
        self.param["f"] = pd.DataFrame(self.param["f"].reshape(7,1),columns=['forecast'])
        dt = pd.DataFrame(pd.date_range(start=self.param["curDate"]+datetime.timedelta(days=1),periods=predict_period),columns=["dt"])
        df = dt.join(self.param["f"])
        return df

