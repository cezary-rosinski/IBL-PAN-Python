from my_functions import gsheet_to_df, cSplit
import pandas as pd

synatpassim = gsheet_to_df('1FnTjH0Y3Sw7-8Dy_wmE_1fOYFp-b4zSpaVdFifTv7pY', 'bez duplikatow')

def kategoria(x):
    if any(c in x['516'].lower() for c in ('serwis', 'portal')):
        val = 'Serwisy, portale'
    elif 'blog' in x['516'].lower():
        val = 'Blogi'
    elif 'czasopism' in x['516'].lower():
        val = 'Czasopisma'
    elif 'biograf' in x['516'].lower():
        val = 'Informacja biograficzna'
    else:
        val = 'Inne'
    return val

synatpassim['kategoria'] = synatpassim.apply(lambda x: kategoria(x), axis=1)

kategorie = ['Serwisy, portale', 'Blogi', 'Czasopisma', 'Informacja biograficzna', 'Inne']

df = {}
for k in kategorie:
    df[k] = synatpassim.copy()[synatpassim['kategoria'] == k]

for name, df in df.items():
    df.to_excel(f"{name}.xlsx", index=False)