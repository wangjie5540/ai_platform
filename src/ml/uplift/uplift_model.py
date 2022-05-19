# coding: utf-8
from pylift import TransformedOutcome
import pandas as pd
import digitforce.aip.common.utils.file_helper as file_helper


def uplift_train(sample_path, model_path, col_treatment='Treatment', col_outcome='Outcome'):
    df = pd.read_csv(sample_path)
    up = TransformedOutcome(df, col_treatment=col_treatment, col_outcome=col_outcome, stratify=df['Treatment'])
    up.randomized_search(n_iter=10, n_jobs=10, random_state=1, verbose=1)
    up.fit(**up.rand_search_.best_params_, productionize=True)
    print(f'start to save model.[model_path={model_path}]')
    file_helper.save_to_pickle(up, model_path)
