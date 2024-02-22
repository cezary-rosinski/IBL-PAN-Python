from my_functions import gsheet_to_df
from collections import defaultdict
import pandas as pd

#%% do raportu rocznego
udzial_help = {'zad. 1': [0.380213904, 0, 0.202391119, 0, 0, 0.098777438, 0, 0.0711],
               'zad. 2': [0, 0, 0.797608881, 0.707397122, 0, 0, 1, 0.2802],
               'zad. 3': [0, 1, 0, 0, 0, 0.740205613, 0, 0.5328],
               'zad. 4': [0.619786096, 0, 0, 0.292602878, 1, 0.161016949, 0, 0.1159]}
df_help = pd.DataFrame(udzial_help)
lista_zadan = list(udzial_help.keys())
lista_udzialow = list(df_help.T.to_dict('list').values())
lista_udzialow_t_f = [[el>0 for el in e] for e in lista_udzialow]

df = gsheet_to_df('1nvJH4itFXu7UKpdP1Rum5vj-VUJU6UnasjVizN7L3zM', 'wynagrodzenia')

lata = ['2023', '2024']

# df = df.loc[(df['status'] == 'zrealizowane') &
#             (df['rok'].isin(lata))]

df = df.loc[(df['rok'].isin(lata))]

zadania_total = {}
for i, row in df.iterrows():
    kwota = float(row['ile brutto-brutto'])
    zadania = {i:True if isinstance(e, str) else False for i, e in row.items() if i in ['zad. 1', 'zad. 2', 'zad. 3', 'zad. 4']}
    right_index = [i for i, e in enumerate(lista_udzialow_t_f) if e == list(zadania.values())][0]
    zadania = {i:e*kwota for i, e in zip(lista_zadan, lista_udzialow[right_index])} 
    # ile_zadan = len(zadania)
    # zadania = {k:kwota/ile_zadan for k,v in zadania.items()}
    for key, value in zadania.items():
        if key in zadania_total:
            zadania_total[key] += value
        else:
            zadania_total[key] = value

wydana_suma = sum(zadania_total.values())

udzial_zadan_w_wydanej_sumie = {k:v/wydana_suma for k,v in zadania_total.items()}

#%% zarobki osób w 2023
df = gsheet_to_df('1nvJH4itFXu7UKpdP1Rum5vj-VUJU6UnasjVizN7L3zM', 'wynagrodzenia')

lata = ['2023']

df = df.loc[(df['status'] == 'zrealizowane') &
            (df['rok'].isin(lata))]

osoby = {}
for i, row in df.iterrows():
    osoba = row['kto']
    kwota = float(row['ile brutto-brutto'])
    if osoba in osoby:
        osoby[osoba] += kwota
    else:
        osoby[osoba] = kwota
    

#%% kombinacje
df_zad = df[['zad. 1', 'zad. 2', 'zad. 3', 'zad. 4']].drop_duplicates()
