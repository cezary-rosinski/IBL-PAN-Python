from my_functions import gsheet_to_df
import pandas as pd
import re
import regex
import numpy as np

def alphas(x):
    val = ''
    for e in x:
        if e.isalpha() or e == ' ':
            val += e
        else:
            val += ' '
    return val

df = gsheet_to_df('11fniEssbBYpxSt3jEoxvCxtR0Qh3QMKPYOKfZhT4NC8', 'Arkusz1').drop_duplicates().fillna(value=np.nan)
df['skrot'] = df['pbl_tytul'].apply(lambda x: alphas (x).split(' '))
df['skrot'] = df['skrot'].apply(lambda x: [e for e in x if len(e) > 1])
df['skrot'] = df['skrot'].apply(lambda x: ''.join([e[:2] for e in x]))

df.to_excel('skroty_pbl.xlsx', index=False)
