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
from my_functions import mrc_to_mrk

bn_cz_mapping = pd.read_excel('F:/Cezary/Documents/IBL/Pliki python/bn_cz_mapping.xlsx')
gatunki_pbl = pd.DataFrame({'gatunek': ["aforyzm", "album", "antologia", "autobiografia", "dziennik", "esej", "felieton", "inne", "kazanie", "list", "miniatura prozą", "opowiadanie", "poemat", "powieść", "proza", "proza poetycka", "reportaż", "rozmyślanie religijne", "rysunek, obraz", "scenariusz", "szkic", "tekst biblijny", "tekst dramatyczny", "dramat", "wiersze", "wspomnienia", "wypowiedź", "pamiętniki", "poezja", "literatura podróżnicza", "satyra", "piosenka", 'opowiadania i nowele']})
gatunki_pbl['gatunek'] = gatunki_pbl['gatunek'].apply(lambda x: f"$a{x}")

# polona full text url

# =============================================================================
# wzbogacić o linki na podstawie 852 i wpisać je do 856, uprzednio czyszcząc 856
# usunąć 856
# będzie trzeba przeszukiwać, czy tytuł w jsonie "title" jest taki, jak w 245
# =============================================================================

years = range(2013,2020)
# =============================================================================
# bn_full_text = pd.DataFrame()   
# for index, year in enumerate(years):
#     print(str(index) + '/' + str(len(years)))
#     path = f"C:/Users/User/Desktop/BN_books/{year}_bn_ks_do_libri.xlsx"
#     #path = f"F:/Cezary/Documents/IBL/Pliki python/{year}_bn_ks_do_libri.xlsx"
#     bn_books = pd.read_excel(path)
#     X245 = marc_parser_1_field(bn_books, 'id', 'X245', '\$')[['id', '$a']]
#     links_bn = marc_parser_1_field(bn_books, 'id', 'X852', '\$')[['id', '$h']]
#     links_bn['link_code'] = links_bn['$h'].apply(lambda x: re.findall('\d[\d\.]+', x)).apply(lambda x: ''.join([i for i in x if i != '.']))
#     links_bn = links_bn[links_bn['link_code'] != ''][['id', 'link_code']].values.tolist()
#     bn_full_text_links = []
#     for i, (bn_id, link) in enumerate(links_bn):
#         print('    ' + str(i) + '/' + str(len(links_bn)))
#         api_url = f"https://polona.pl/api/entities/?format=json&from=0&highlight=1&public=1&query={link}"
#         json_data = requests.get(api_url)
#         json_data = json.loads(json_data.text)
#         try:                     
#             full_text = [''.join([elem['url'] for elem in hit['resources'] if 'archive' in elem['url']]) for hit in json_data['hits']]
#             json_title = [hit['title'] for hit in json_data['hits']]
#             json_record = list(zip(json_title, full_text))
#             json_record = [elem for elem in json_record if len(elem[1])>0]
#             json_record = [(bn_id, e, f) for e, f in json_record]
#             bn_full_text_links += json_record
#         except:
#             pass  
#     links_bn = pd.DataFrame(bn_full_text_links, columns=['id', 'json_title', 'full_text_url'])
#     links_bn = pd.merge(links_bn, X245, how='left', on='id')
#     links_bn['match'] = links_bn[['json_title', '$a']].apply(lambda x: x['json_title'] in x['$a'], axis=1)
#     links_bn = links_bn[links_bn['match']==True].drop(columns=['json_title', '$a', 'match'])
#     bn_full_text = bn_full_text.append(links_bn)
# 
# bn_full_text.to_excel('bn_full_text.xlsx', index=False)
# =============================================================================
    


bn_books_marc_total = pd.DataFrame()

for i, year in enumerate(years):
    print(str(i) + '/' + str(len(years)))
    path = f"F:/Cezary/Documents/IBL/Pliki python/{year}_bn_ks_do_libri.xlsx"
    bn_books = pd.read_excel(path)
    if bn_books['X005'].dtype == np.float64:
        bn_books['X005'] = bn_books['X005'].astype(np.int64)
    to_remove = bn_books[(bn_books['X655'].isnull()) &
                    (bn_books['rodzaj_ksiazki'] == 'podmiotowa') &
                    (bn_books['gatunek'].isnull()) &
                    ((bn_books['X080'].str.contains('\$a94') == False) &
                     (bn_books['X080'].str.contains('\$a316') == False) &
                     (bn_books['X080'].str.contains('\$a929') == False) &
                     (bn_books['X080'].str.contains('\$a821') == False) |
                     (bn_books['X080'].isnull()))][['id', 'dziedzina_PBL', 'gatunek', 'X080', 'X100', 'X245', 'X650', 'X655']]
    X100_field = marc_parser_1_field(to_remove, 'id', 'X100', '\$')
    X100_field['year'] = X100_field['$d'].apply(lambda x: re.findall('\d+', x)[0] if x!='' else np.nan)
    X100_field = X100_field[X100_field['year'].notnull()]
    X100_field = X100_field[X100_field['year'].astype(int) <= 1700]
    to_remove = to_remove[~to_remove['id'].isin(X100_field['id'])]
    
    bn_books = bn_books[~bn_books['id'].isin(to_remove['id'])]
    pbl_enrichment = bn_books[['id', 'dziedzina_PBL', 'rodzaj_ksiazki', 'DZ_NAZWA', 'X650', 'X655']]
    pbl_enrichment['DZ_NAZWA'] = pbl_enrichment['DZ_NAZWA'].str.replace(' - .*?$', '', regex=True)
    pbl_enrichment = cSplit(pbl_enrichment, 'id', 'X655', '❦')
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
    
    position_of_LDR = bn_books.columns.get_loc("LDR")
    bn_books_marc = bn_books.iloc[:,position_of_LDR:]
    
    bn_books_marc = bn_books_marc.set_index('X009', drop=False)
    pbl_enrichment = pbl_enrichment.set_index('id').rename(columns={'650':'X650', '655':'X655'})
    bn_books_marc = pbl_enrichment.combine_first(bn_books_marc)
    
    fields_to_remove = bn_cz_mapping[bn_cz_mapping['cz'] == 'del']['bn'].to_list()
    bn_books_marc = bn_books_marc.loc[:, ~bn_books_marc.columns.isin(fields_to_remove)]
    
    merge_500s = [col for col in bn_books_marc.columns if 'X5' in col and col != 'X505']
    bn_books_marc['X500'] = bn_books_marc[merge_500s].apply(lambda x: '❦'.join(x.dropna().astype(str)), axis = 1)
    merge_500s = [x for x in merge_500s if x != 'X500']
    bn_books_marc = bn_books_marc.loc[:, ~bn_books_marc.columns.isin(merge_500s)]
    bn_books_marc.rename(columns={'X260':'X264'}, inplace=True)
    bn_books_marc.drop(['X852', 'X856'], axis = 1, inplace=True) 
    bn_new_column_names = bn_books_marc.columns.to_list()
    bn_new_column_names = [column.replace('X', '') for column in bn_new_column_names]
    bn_books_marc.columns = bn_new_column_names
    bn_books_marc['240'] = bn_books_marc['246'].apply(lambda x: x if pd.notnull(x) and 'Tyt. oryg.:' in x else np.nan)
    bn_books_marc['246'] = bn_books_marc['246'].apply(lambda x: x if pd.notnull(x) and 'Tyt. oryg.:' not in x else np.nan)
    bn_books_marc['995'] = '\\\\$aPBL 2013-2019: książki'
    
    bn_books_marc_total = bn_books_marc_total.append(bn_books_marc)

bn_books_marc_total = bn_books_marc_total.replace(r'^\❦', '', regex=True).replace(r'\❦$', '', regex=True)
subfield_list= bn_books_marc_total.columns.tolist()
subfield_list.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
bn_books_marc_total = bn_books_marc_total.reindex(columns=subfield_list)
bn_books_marc_total = bn_books_marc_total.reset_index(drop=True)
bn_books_marc_total['008'] = bn_books_marc_total['008'].str.replace('\\', ' ')
if bn_books_marc_total['009'].dtype == np.float64:
        bn_books_marc_total['009'] = bn_books_marc_total['009'].astype(np.int64)

df_to_mrc(bn_books_marc_total, '❦', 'libri_marc_bn_books.mrc')
mrc_to_mrk('libri_marc_bn_books.mrc', 'libri_marc_bn_books.mrk')

print('Done')


# nadpisać główny plik
# wybrać właściwe kolumny
# zastosować mapowanie Vojty
# przerobić na marca

# =============================================================================
# BN
# 100 d - usunąć nawiasy
# 830 usunąć v
# =============================================================================








