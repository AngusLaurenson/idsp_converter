import pandas as pd
import scipy as sp
from datetime import datetime

df = pd.read_csv("/users/rsg/anla/podcast/country_disease_outbreaks/india/idsp_reporting/IDSP_data.csv")

missing_dict = {}
for col in df.columns:
    print(col)
    missing_dict[col] = df[df[col] == '?'].shape[0]

records_completeness = []
for row in df.index:
    row_completeness = 0
    for col in df.columns:
        if df.loc[row][col] != '?':
            row_completeness += 1

    records_completeness.append(row_completeness)
