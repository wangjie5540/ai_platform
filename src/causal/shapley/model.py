from simplified_shapley_attribution_model import SimplifiedShapleyAttributionModel
from ordered_shapley_attribution_model import OrderedShapleyAttributionModel
import pandas as pd

def readData(input_file):
    data = pd.read_csv(input_file, sep=";")
    journeys = []
    for term in data.values:
        path, count = term[0], term[1]
        for i in range(count):
            journeys.append(path.split(" > "))
    return journeys

def shapley(input_file, output_file, model_type):
    journeys = readData(input_file)
    if model_type == "simply":
        o = SimplifiedShapleyAttributionModel()
    elif model_type == "ordered":
        o = OrderedShapleyAttributionModel()
    result = o.attribute(journeys)
    with open(output_file, "w+") as f:
        for k in result:
            v = result[k]
            if isinstance(v, list):
                f.write("%s\t%s\n" % (k, ','.join([str(x) for x in v])))
            else:
                f.write("%s\t%s\n" % (k, v))