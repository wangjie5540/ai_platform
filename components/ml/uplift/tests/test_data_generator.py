import pickle

import numpy as np, matplotlib as mpl, matplotlib.pyplot as plt, pandas as pd
from pylift import TransformedOutcome
from pylift.generate_data import dgp
import uplift_model

# Generate some data.
df = dgp(N=10000, discrete_outcome=True)
df.to_csv('my_test.csv', index=False)

with open('uplift.model', 'wb') as f:
    pickle.dump(uplift_model.uplift_train(df), f)
