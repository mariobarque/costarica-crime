import pandas as pd

inec_df = pd.read_csv('data/inec_2011_distritos.csv')
ids_df = pd.read_csv('data/ids_2016_distritos.csv')

merge_df = pd.merge(inec_df, ids_df, on='ZIPCODE')

merge_df.to_csv('data/distritos.csv', encoding='utf-8-sig', index=False)
