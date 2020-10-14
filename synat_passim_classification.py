from my_functions import gsheet_to_df, cSplit
import pandas as pd
import regex as re

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
    
#statystyki
    
full_synat = pd.DataFrame()
arkusze = ['serwisy, portale [OGARNIA CR]', 'blogi_synat [OGARNIA PCL w pliku Sprawdzanie...]', 'czasopisma [OGARNIA KP]', 'info. bibliograficzna [CR]', 'Inne [OGARNIA CR]']

for el in arkusze:
    df = gsheet_to_df('1FnTjH0Y3Sw7-8Dy_wmE_1fOYFp-b4zSpaVdFifTv7pY', el)
    full_synat = full_synat.append(df, ignore_index=True)
    
uwagi = full_synat['uwagi'].value_counts()

liczba_festiwali = len(full_synat.copy()[full_synat['520'].str.lower().str.contains('festiwal|konkurs|nagrod|plebiscyt')])

liczba_tworcow = len(full_synat.copy()[full_synat['uwagi'].str.lower().str.contains('biograf')])

liczba_instytucji = len(full_synat.copy()[(full_synat['710'] != '') &
                                          (~full_synat['516'].str.lower().str.contains('wydawnictw'))])

liczba_wydawnictw = len(full_synat.copy()[full_synat['516'].str.lower().str.contains('wydawnictw')])

#porządkowanie serwisów i czasopism

serwisy_portale = gsheet_to_df('1EWzb9mCsTVxYDqj_CzKW5EcqJy4z3a_-GyNGAbAjGH0', 'Serwisy, portale (finalny)')
s_p_adres = serwisy_portale.copy()[['id', 'adres']]
s_p_adres = cSplit(s_p_adres, 'id', 'adres', '❦')

s_p_adres = s_p_adres[s_p_adres['adres'].str.contains('http')]
s_p_adres['adres'] = s_p_adres['adres'].apply(lambda x: re.sub('(.+?u |^u |^3 )(h.+$)', r'\2', x))
s_p_adres['adres'] = s_p_adres['adres'].apply(lambda x: re.sub('(.+?)( .+)', r'\1', x))
s_p_adres['len'] = s_p_adres['adres'].apply(lambda x: len(x))
s_p_adres['adres'] = s_p_adres['adres'].apply(lambda x: x.replace('http://www.', 'http://'))
s_p_adres['id'] = s_p_adres['id'].astype(int)
s_p_adres = s_p_adres.sort_values(['id', 'len']).drop_duplicates()

test = s_p_adres.groupby('id').head(1).reset_index(drop=True).drop(columns='len')

serwis_nazwa = serwisy_portale.copy()[['id', 'nazwa']]

podpola_marc = []
for e in serwis_nazwa['nazwa']:
    val = re.findall('(?<=^| ).(?= )', e)
    for el in val:
        if el.isalpha():
            podpola_marc.append(el)

podpola_marc = list(set(podpola_marc))

serwis_nazwa['nazwa'] = serwis_nazwa['nazwa'].str.replace('b |p |c |a ', '')

czasopisma = gsheet_to_df('1EWzb9mCsTVxYDqj_CzKW5EcqJy4z3a_-GyNGAbAjGH0', 'Czasopisma (finalny)')
czasopisma_fin = czasopisma.copy()[['id', 'adres']]
czasopisma_fin = cSplit(czasopisma_fin, 'id', 'adres', '❦')
czasopisma_fin = czasopisma_fin[czasopisma_fin['adres'].notnull()]

czasopisma_fin = czasopisma_fin[czasopisma_fin['adres'].str.contains('http')]
czasopisma_fin['adres'] = czasopisma_fin['adres'].apply(lambda x: re.sub('(.+?u |^u |^3 )(h.+$)', r'\2', x))
czasopisma_fin['adres'] = czasopisma_fin['adres'].apply(lambda x: re.sub('(.+?)( .+)', r'\1', x))
czasopisma_fin['len'] = czasopisma_fin['adres'].apply(lambda x: len(x))
czasopisma_fin['adres'] = czasopisma_fin['adres'].apply(lambda x: x.replace('http://www.', 'http://'))
czasopisma_fin['id'] = czasopisma_fin['id'].astype(int)
czasopisma_fin = czasopisma_fin.sort_values(['id', 'len']).drop_duplicates()

test2 = czasopisma_fin.groupby('id').head(1).reset_index(drop=True).drop(columns='len')

czasopisma_nazwa = czasopisma.copy()[['id', 'nazwa']]
czasopisma_nazwa['nazwa'] = czasopisma_nazwa['nazwa'].str.replace('b |p |c |a ', '')
