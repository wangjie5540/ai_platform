from Apriori import getRelatedItem
from fp_growth import get_related_item
import pandas as pd
import numpy as np
from common_logging_config import *
from deepfm import train as deepfm_train

def train():
    dataset = pd.DataFrame()
    """
    除了常用的特征，需要添加行为序列list，used to find relevant items by Aproiri or FP-Growth
    """

