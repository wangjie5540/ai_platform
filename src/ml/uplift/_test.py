# coding: utf-8

from pylift.generate_data import dgp
import uplift_model

sample_path = 'sample.csv'
model_path = 'uplift.model'

# Generate some data.
df = dgp(N=10000, discrete_outcome=True)
df.to_csv(sample_path, index=False)
uplift_model.uplift_train(sample_path, model_path)
