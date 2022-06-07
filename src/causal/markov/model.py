import pandas as pd

from ChannelAttribution import markov_model

def markov(input_file, output_file):
    data = pd.read_csv(input_file,sep=";")
    M = markov_model(data, "path", "total_conversions")
    with open(output_file, "w+") as f:
        for key, value in M.values:
            f.write("%s\t%s\n" % (key, value))