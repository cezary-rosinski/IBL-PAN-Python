import pandas as pd
import numpy as np
from my_functions import marc_parser_1_field
import re
import pandasql
from my_functions import cSplit
import json
import requests
from my_functions import df_to_mrc
import io

bn_articles1 = pd.read_excel("F:/Cezary/Documents/IBL/Migracja z BN/bn_articles_1.xlsx")
bn_articles2 = pd.read_excel("F:/Cezary/Documents/IBL/Migracja z BN/bn_articles_2.xlsx")

bn_articles = pd.concat([bn_articles1, bn_articles2])
bn_articles['id'] = bn_articles['X009']
bn_cz_mapping = pd.read_excel('F:/Cezary/Documents/IBL/Pliki python/bn_cz_mapping.xlsx')
gatunki_pbl = pd.DataFrame({'gatunek': ["aforyzm", "album", "antologia", "autobiografia", "dziennik", "esej", "felieton", "inne", "kazanie", "list", "miniatura prozą", "opowiadanie", "poemat", "powieść", "proza", "proza poetycka", "reportaż", "rozmyślanie religijne", "rysunek, obraz", "scenariusz", "szkic", "tekst biblijny", "tekst dramatyczny", "dramat", "wiersze", "wspomnienia", "wypowiedź", "pamiętniki", "poezja", "literatura podróżnicza", "satyra", "piosenka"]})
gatunki_pbl['gatunek'] = gatunki_pbl['gatunek'].apply(lambda x: f"$a{x}")

test = bn_articles[(bn_articles['X650'].isnull()) &
                   (bn_articles['X655'].isnull()) &
                   (bn_articles['X651'].isnull()) &
                   (bn_articles['X600'].isnull()) &
                   (bn_articles['X610'].isnull()) &
                   (bn_articles['slowa_literackie'].isnull())]

test2 = bn_articles[bn_articles['X245'].str.contains('Zaliczka albo dodatek')==True]

test = bn_articles.head(10000)
bn_articles = bn_articles[bn_articles['X001'] == 'b0000003184267']


test = pbl_enrichment.copy()[pbl_enrichment.index == '991027974299705066']

n = 10000
list_df = [bn_articles[i:i+n] for i in range(0, bn_articles.shape[0],n)]

pbl_enrichment_full = pd.DataFrame()
for i, group in enumerate(list_df):
    print(str(i) + '/' + str(len(list_df)))
    pbl_enrichment = group.copy()[['id', 'dziedzina_PBL', 'rodzaj_ksiazki', 'DZ_NAZWA', 'X650', 'X655']]
    pbl_enrichment['DZ_NAZWA'] = pbl_enrichment['DZ_NAZWA'].str.replace(' - .*?$', '', regex=True)
    pbl_enrichment = cSplit(pbl_enrichment, 'id', 'X655', '|')
    pbl_enrichment['jest x'] = pbl_enrichment['X655'].str.contains('\$x')
    pbl_enrichment['nowe650'] = pbl_enrichment.apply(lambda x: x['X655'] if x['jest x'] == True else np.nan, axis=1)
    pbl_enrichment['X655'] = pbl_enrichment.apply(lambda x: x['X655'] if x['jest x'] == False else np.nan, axis=1)
    pbl_enrichment['X650'] = pbl_enrichment[['X650', 'nowe650']].apply(lambda x: '❦'.join(x.dropna().astype(str)), axis=1)
    pbl_enrichment = pbl_enrichment.drop(['jest x', 'nowe650'], axis=1)
    
    query = "select * from pbl_enrichment a join gatunki_pbl b on lower(a.X655) like '%'||b.gatunek||'%'"
    gatunki1 = pandasql.sqldf(query)
    query = "select * from pbl_enrichment a join gatunki_pbl b on lower(a.X650) like '%'||b.gatunek||'%'"
    gatunki2 = pandasql.sqldf(query)
    gatunki = pd.concat([gatunki1, gatunki2]).drop_duplicates()
    gatunki['gatunek'] = gatunki['gatunek'].apply(lambda x: ''.join([x[:2], x[2].upper(), x[2 + 1:]]).strip())
    X655_field = marc_parser_1_field(gatunki, 'id', 'X655', '\$')[['id', '$y']].drop_duplicates()
    X655_field = X655_field[X655_field['$y'] != '']
    gatunki = pd.merge(gatunki, X655_field, how='left', on='id')
    gatunki['gatunek'] = gatunki['gatunek'].apply(lambda x: f"\\7{x.strip()}")
    gatunki['gatunek+data'] = gatunki.apply(lambda x: f"{x['gatunek']}$y{x['$y']}" if pd.notnull(x['$y']) else np.nan, axis=1)
    gatunki['nowe655'] = gatunki[['X655', 'gatunek', 'gatunek+data']].apply(lambda x: '❦'.join(x.dropna().astype(str)), axis=1)
    gatunki['nowe655'] = gatunki.groupby('id')['nowe655'].transform(lambda x: '❦'.join(x))
    gatunki = gatunki[['id', 'nowe655']].drop_duplicates()
    gatunki['nowe655'] = gatunki['nowe655'].str.split('❦').apply(set).str.join('❦')
    
    pbl_enrichment = pd.merge(pbl_enrichment, gatunki, how ='left', on='id')
    pbl_enrichment['nowe650'] = pbl_enrichment.apply(lambda x: x['X655'] if pd.isnull(x['nowe655']) else np.nan, axis=1)
    pbl_enrichment['DZ_NAZWA'] = pbl_enrichment['DZ_NAZWA'].apply(lambda x: f"\\7$a{x}" if 'do ustalenia' not in x else np.nan)
    pbl_enrichment['X650'] = pbl_enrichment['X650'].replace(r'^\s*$', np.nan, regex=True)
    pbl_enrichment['650'] = pbl_enrichment[['X650', 'nowe650', 'DZ_NAZWA']].apply(lambda x: '❦'.join(x.dropna().astype(str)), axis=1)
    pbl_enrichment['655'] = pbl_enrichment['nowe655'].replace(np.nan, '', regex=True)
    pbl_enrichment['655'] = pbl_enrichment.apply(lambda x: f"{x['655']}❦\\7$aOpracowanie" if x['rodzaj_ksiazki'] == 'przedmiotowa' else f"{x['655']}❦\\7$aDzieło literackie", axis=1)
    pbl_enrichment = pbl_enrichment[['id', '650', '655']].replace(r'^\❦', '', regex=True)
    pbl_enrichment['650'] = pbl_enrichment.groupby('id')['650'].transform(lambda x: '❦'.join(x.dropna().astype(str)))
    pbl_enrichment['655'] = pbl_enrichment.groupby('id')['655'].transform(lambda x: '❦'.join(x.dropna().astype(str)))
    pbl_enrichment = pbl_enrichment.drop_duplicates().reset_index(drop=True)
    pbl_enrichment['650'] = pbl_enrichment['650'].str.split('❦').apply(set).str.join('❦')
    pbl_enrichment['655'] = pbl_enrichment['655'].str.split('❦').apply(set).str.join('❦')
    pbl_enrichment_full = pbl_enrichment_full.append(pbl_enrichment)

position_of_LDR = bn_articles.columns.get_loc("LDR")
bn_articles_marc = bn_articles.iloc[:,position_of_LDR:]

bn_articles_marc = bn_articles_marc.set_index('X009', drop=False)
pbl_enrichment = pbl_enrichment.set_index('id').rename(columns={'650':'X650', '655':'X655'})
bn_articles_marc = pbl_enrichment.combine_first(bn_articles_marc)

fields_to_remove = bn_cz_mapping[bn_cz_mapping['cz'] == 'del']['bn'].to_list()
bn_articles_marc = bn_articles_marc.loc[:, ~bn_articles_marc.columns.isin(fields_to_remove)]

merge_500s = [col for col in bn_articles_marc.columns if 'X5' in col]

bn_articles_marc['500'] = bn_articles_marc[merge_500s].apply(lambda x: '❦'.join(x.dropna().astype(str)), axis = 1)
bn_articles_marc = bn_articles_marc.loc[:, ~bn_articles_marc.columns.isin(merge_500s)]
bn_articles_marc.rename(columns={'X260':'X264'}, inplace=True)
bn_articles_marc.drop(['X852', 'X856'], axis = 1, inplace=True) 
bn_new_column_names = bn_articles_marc.columns.to_list()
bn_new_column_names = [column.replace('X', '') for column in bn_new_column_names]
bn_articles_marc.columns = bn_new_column_names
bn_articles_marc['240'] = bn_articles_marc['246'].apply(lambda x: x if pd.notnull(x) and 'Tyt. oryg.:' in x else np.nan)
bn_articles_marc['246'] = bn_articles_marc['246'].apply(lambda x: x if pd.notnull(x) and 'Tyt. oryg.:' not in x else np.nan)
bn_articles_marc['995'] = '\\\\$aPBL 2004-2019: czasopisma'