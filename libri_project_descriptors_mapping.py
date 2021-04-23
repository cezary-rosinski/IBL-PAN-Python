import pandas as pd
import io
from google_drive_research_folders import PBL_folder
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import gspread as gs
from my_functions import cSplit, df_to_mrc, mrk_to_df
import datetime
import regex as re
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from tqdm import tqdm
import glob

#%% date
now = datetime.datetime.now()
year = now.year
month = now.month
day = now.day

#%% google authentication & google drive
#autoryzacja do tworzenia i edycji plików
gc = gs.oauth()
#autoryzacja do penetrowania dysku
gauth = GoogleAuth()
gauth.LocalWebserverAuth()
drive = GoogleDrive(gauth)

file_list = drive.ListFile({'q': f"'{PBL_folder}' in parents and trashed=false"}).GetList() 
file_list = drive.ListFile({'q': "'0B0l0pB6Tt9olWlJVcDFZQ010R0E' in parents and trashed=false"}).GetList()
file_list = drive.ListFile({'q': "'1tPr_Ly9Lf0ZwgRQjj_iNVt-T15FSWbId' in parents and trashed=false"}).GetList()
file_list = drive.ListFile({'q': "'1xzqGIfZllmXXTh2dJABeHbRPFAM34nbw' in parents and trashed=false"}).GetList()
#[print(e['title'], e['id']) for e in file_list]
mapping_files_655 = [file['id'] for file in file_list if file['title'] == 'mapowanie BN-Oracle - 655'][0]
mapping_files_650 = [file['id'] for file in file_list if file['title'].startswith('mapowanie BN-Oracle') if file['id'] != mapping_files_655]

#%% deskryptory z mapowania
df_650 = pd.DataFrame()
for file in tqdm(mapping_files_650):
    sheet = gc.open_by_key(file)
    df = get_as_dataframe(sheet.worksheet('deskryptory_650'), evaluate_formulas=True).dropna(how='all').dropna(how='all', axis=1)
    df_650 = df_650.append(df)

sheet = gc.open_by_key(mapping_files_655)
df_655 = get_as_dataframe(sheet.worksheet('deskryptory_655'), evaluate_formulas=True).dropna(how='all').dropna(how='all', axis=1)

df_650 = df_650[df_650['decyzja'].isin(['margines', 'zmapowane'])][['X650', 'decyzja', 'dzial_PBL_1', 'dzial_PBL_2', 'dzial_PBL_3', 'haslo_przedmiotowe_PBL_1', 'haslo_przedmiotowe_PBL_2', 'haslo_przedmiotowe_PBL_3']].reset_index(drop=True)

dict_650 = {}
for i, row in tqdm(df_650.iterrows(), total=df_650.shape[0]):
    
    lista_dzialow = row[['dzial' in i for i in row.index]].to_list()
    lista_dzialow = [e for e in lista_dzialow if pd.notnull(e)]
    lista_hasel = row[['haslo' in i for i in row.index]].to_list()
    lista_hasel = [e for e in lista_hasel if pd.notnull(e)]
    dict_650[row['X650']] = {'decyzja': row['decyzja'], 'działy PBL': lista_dzialow, 'hasła PBL': lista_hasel}

# df_655 = df_655[df_655['decyzja'].isin(['margines', 'zmapowane'])][['X655', 'decyzja', 'dzial_PBL_1', 'dzial_PBL_2', 'dzial_PBL_3', 'dzial_PBL_4', 'dzial_PBL_5', 'haslo_przedmiotowe_PBL_1', 'haslo_przedmiotowe_PBL_2', 'haslo_przedmiotowe_PBL_3', 'haslo_przedmiotowe_PBL_4', 'haslo_przedmiotowe_PBL_5']].reset_index(drop=True)

df_655 = df_655[df_655['decyzja'].isin(['zmapowane'])][['X655', 'decyzja', 'dzial_PBL_1', 'dzial_PBL_2', 'dzial_PBL_3', 'dzial_PBL_4', 'dzial_PBL_5', 'haslo_przedmiotowe_PBL_1', 'haslo_przedmiotowe_PBL_2', 'haslo_przedmiotowe_PBL_3', 'haslo_przedmiotowe_PBL_4', 'haslo_przedmiotowe_PBL_5']].reset_index(drop=True)

dict_655 = {}
for i, row in tqdm(df_655.iterrows(), total=df_655.shape[0]):
    
    lista_dzialow = row[['dzial' in i for i in row.index]].to_list()
    lista_dzialow = [e for e in lista_dzialow if pd.notnull(e)]
    lista_hasel = row[['haslo' in i for i in row.index]].to_list()
    lista_hasel = [e for e in lista_hasel if pd.notnull(e)]
    dict_655[row['X655']] = {'decyzja': row['decyzja'], 'działy PBL': lista_dzialow, 'hasła PBL': lista_hasel}

# dict_650['Poradnik']    
# dict_655['Poradnik'] 
    
#lista deskryptorów do wzięcia - szeroka (z mapowania)
BN_descriptors_mapping = list(dict_650.keys())
BN_descriptors_mapping.extend(dict_655.keys())
BN_descriptors_mapping = list(set([re.sub('\$y.*', '', e) for e in BN_descriptors_mapping]))

#dydaktyka
dydaktyka = get_as_dataframe(sheet.worksheet('dydaktyka'), evaluate_formulas=True).dropna(how='all').dropna(how='all', axis=1)
dydaktyka_650 = [e.split('❦') for e in dydaktyka[650] if pd.notnull(e)]
dydaktyka_650 = list(set([re.sub('\$y.*', '', e[4:]).replace('$2DBN', '') for sub in dydaktyka_650 for e in sub]))

dydaktyka_655 = [e.split('❦') for e in dydaktyka[655] if pd.notnull(e)]
dydaktyka_655 = list(set([re.sub('\$y.*', '', e[4:]).replace('$2DBN', '') for sub in dydaktyka_655 for e in sub]))

#%% deskryptory do harvestowania BN
#lista deskryptorów do wzięcia - wąska (z selekcji Karoliny)
deskryptory_do_filtrowania = [file['id'] for file in file_list if file['title'] == 'deskryptory_do_filtrowania'][0]
deskryptory_do_filtrowania = gc.open_by_key(deskryptory_do_filtrowania)
deskryptory_do_filtrowania = get_as_dataframe(deskryptory_do_filtrowania.worksheet('deskryptory_do_filtrowania'), evaluate_formulas=True).dropna(how='all').dropna(how='all', axis=1)
BN_descriptors = deskryptory_do_filtrowania[deskryptory_do_filtrowania['deskryptor do filtrowania'] == 'tak']['deskryptory'].to_list()
def uproszczenie_nazw(x):
    try:
        if x.index('$') == 0:
            return x[2:]
        elif x.index('$') == 1:
            return x[4:]
    except ValueError:
        return x
BN_descriptors = list(set([e.strip() for e in BN_descriptors]))
BN_descriptors2 = list(set(uproszczenie_nazw(e) for e in BN_descriptors))
roznica = list(set(BN_descriptors2) - set(BN_descriptors))
BN_descriptors.extend(roznica)

#%% harvesting BN

#zakres lat 
years = range(2013,2020)
   
path = 'F:/Cezary/Documents/IBL/Migracja z BN/bn_all/2021-02-08/'
files = [file for file in glob.glob(path + '*.mrk', recursive=True)]

encoding = 'utf-8'
new_list = []
for file_path in tqdm(files):
    marc_list = io.open(file_path, 'rt', encoding = encoding).read().splitlines()

    mrk_list = []
    for row in marc_list:
        if row.startswith('=LDR'):
            mrk_list.append([row])
        else:
            if row:
                mrk_list[-1].append(row)
                               
    for sublist in mrk_list:
        try:
            year_biblio = int(''.join([ele for ele in sublist if ele.startswith('=008')])[13:17])
            bibliographic_level = ''.join([ele for ele in sublist if ele.startswith('=LDR')])[13]
            if year_biblio in years and bibliographic_level == 'm':
                for el in sublist:
                    if el.startswith('=650') or el.startswith('=655'):
                        el = re.sub('\$y.*', '', el[10:]).replace('$2DBN', '').strip()
                        if any(desc == el for desc in BN_descriptors):
                            new_list.append(sublist)
                            break
        except ValueError:
            pass

final_list = []
for lista in new_list:
    slownik = {}
    for el in lista:
        if el[1:4] in slownik:
            slownik[el[1:4]] += f"❦{el[6:]}"
        else:
            slownik[el[1:4]] = el[6:]
    final_list.append(slownik)

df = pd.DataFrame(final_list).drop_duplicates().reset_index(drop=True)
fields = df.columns.tolist()
fields = [i for i in fields if 'LDR' in i or re.compile('\d{3}').findall(i)]
df = df.loc[:, df.columns.isin(fields)]
fields.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
df = df.reindex(columns=fields)   
df_original = df.copy()

# =============================================================================
# BN_desc_and_decision_650 = [[re.sub('\$y.*', '', k), v['decyzja']] for k, v in dict_650.items()]
# BN_desc_and_decision_655 = [[re.sub('\$y.*', '', k), v['decyzja']] for k, v in dict_655.items()]
# BN_desc_and_decision = BN_desc_and_decision_650 + BN_desc_and_decision_655
# BN_desc_and_decision = list(set(BN_desc_and_decision))
# BN_desc_and_decision = [list(item) for item in set(tuple(row) for row in BN_desc_and_decision)]
# BN_desc_and_decision_df = pd.DataFrame(BN_desc_and_decision, columns=['deskryptor', 'decyzja'])
# 
# marc_df_simple = marc_df[['001', '650', '655']]
# 
# query = "select * from marc_df_simple a join BN_desc_and_decision_df b on a.'650' like '%'||b.deskryptor||'%'"
# query1 = pandasql.sqldf(query)
# query = "select * from marc_df_simple a join BN_desc_and_decision_df b on a.'655' like '%'||b.deskryptor||'%'"
# query2 = pandasql.sqldf(query)
# 
# marc_df_simple = pd.concat([query1, query2]).drop_duplicates()[['001', 'decyzja', 'deskryptor']].rename(columns={'deskryptor': 'deskryptor z mapowania'})
# marc_df_simple['deskryptor z mapowania'] = marc_df_simple.groupby('001')['deskryptor z mapowania'].transform(lambda x: '❦'.join(x.dropna().drop_duplicates().astype(str)))
# marc_df_simple['decyzja'] = marc_df_simple.groupby('001')['decyzja'].transform(lambda x: '❦'.join(x.dropna().drop_duplicates().astype(str)))
# marc_df_simple = marc_df_simple.drop_duplicates().reset_index(drop=True)
# 
# marc_df = pd.merge(marc_df, marc_df_simple, on='001', how='left')
# =============================================================================

#jakimi warunkami zmniejszyć liczbę rekordów?

#%% dalsze filtrowanie
#odsianie rekordów, które nie mają nic wspólnego z PL

def czy_polonik(x):
    polish_language = x['008'][35:38] == 'pol'
    published_in_Poland = x['008'][15:17] == 'pl'
    try:
        x041 = 'pol' in x['041']
    except TypeError:
        x041 = False
    try:
        x044 = 'pol' in x['044'] or 'pl' in x['044']
    except TypeError:
        x044 = False
    if any('pol' in e.lower() for e in [x['500'], x['501'], x['546']] if pd.notnull(e)):
        pol_in_remarks = True
    else:
        pol_in_remarks = False
    if any('polsk' in e.lower() for e in [x['650'], x['655']] if pd.notnull(e)):
        pol_descriptor = True
    else:
        pol_descriptor = False
    if any([polish_language, published_in_Poland, x041, x044, pol_in_remarks, pol_descriptor]):
        return True
    else:
        return False
    
df['czy polonik'] = df.apply(lambda x: czy_polonik(x), axis=1)
df = df[df['czy polonik'] == True]

#odsianie druków ulotnych
druki_ulotne = df[(df['380'].str.contains('Druki ulotne')) & (df['380'].notnull())]['001'].to_list()
df = df[~df['001'].isin(druki_ulotne)]

#gatunki literackie
deskryptory_spoza_centrum = get_as_dataframe(sheet.worksheet('deskryptory_spoza_centrum'), evaluate_formulas=True).dropna(how='all').dropna(how='all', axis=1)['deskryptor'].to_list()
deskryptory_spoza_centrum = list(set([re.sub('\$y.*', '', e) for e in deskryptory_spoza_centrum if pd.notnull(e)]))
dodatek_do_centrum = get_as_dataframe(sheet.worksheet('dodatek_do_centrum'), evaluate_formulas=True).dropna(how='all').dropna(how='all', axis=1)
dodatek_do_centrum = dodatek_do_centrum[dodatek_do_centrum['decyzja'] == 'PRAWDA']['deskryptor'].to_list()
dodatek_do_centrum = list(set([re.sub('\$y.*', '', e) for e in dodatek_do_centrum]))

list_655 = list(set([re.sub('\$y.*', '', e) for e in dict_655.keys()]))
list_655 = [e for e in list_655 if e not in deskryptory_spoza_centrum]
list_655.extend(dodatek_do_centrum)
list_655 = list(set(list_655))

def gatunki_literackie(x):
    for field in x:
        if pd.notnull(field):
            for element in field.split('❦'):
                element = re.sub('\$y.*', '', element[4:]).replace('$2DBN', '')
                if any(desc == element for desc in list_655):
                    return True
    return False

# def gatunki_literackie(x):
#     match = []
#     for field in x:
#         if pd.notnull(field):
#             for element in field.split('❦'):
#                 element = re.sub('\$y.*', '', element[4:]).replace('$2DBN', '')
#                 if any(desc == element for desc in list_655):
#                     match.append(True)
#                 else:
#                     match.append(False)
#     if any(match):
#         return True
#     else:
#         return False

df['gatunki literackie'] = df[['650', '655']].apply(lambda x: gatunki_literackie(x), axis=1)
#zostawiamy tylko książki lub dobre gatunki literackie lub przynależność do literatury

#TUTAJ!!!!!!!!!!! podjąć decyzję

# df2 = df[(df['380'].str.lower().str.contains('książ|book', regex=True)) | (df['380'].isnull())]
# rekordy_id_bez_gatunkow_literackich = df2['001'].drop_duplicates().to_list()

df = df[(df['380'].str.lower().str.contains('książ|book', regex=True)) | 
        (df['380'].isnull()) | 
        (df['gatunki literackie'] == True)]
# rekordy_id_z_gatunkami_literackimi = df3['001'].drop_duplicates().to_list()

# roznica = list(set(rekordy_id_z_gatunkami_literackimi) - set(rekordy_id_bez_gatunkow_literackich))
# roznica_df = df_original[df_original['001'].isin(roznica)]
# roznica_df.to_excel('roznica.xlsx', index=False)

#%% dobre gatunki literackie
#jeśli coś ma gatunek literacki, to jest z automatu dobre
dobre1_df = df[(df['gatunki literackie'] == True) |
               ((df['650'].str.lower().str.contains('filolog|literatur|literac|pisar|poezj')) & df['650'].notnull()) |
               ((df['655'].str.lower().str.contains('filolog|literatur|literac|pisar|poezj')) & df['655'].notnull())]

#pozostałe rekordy przetwarzamy
df = df[~df['001'].isin(dobre1_df['001'])]

#%% frekwencje

deskryptory_z_df = df[['001', '655', '650']].reset_index(drop=True)

deskryptory_dict = {}
for i, row in tqdm(deskryptory_z_df.iterrows(), total=deskryptory_z_df.shape[0]):
    lista_deskryptorow = [e for e in f"{row['650']}❦{row['655']}".split('❦') if e != 'nan']
    deskryptory_dict[row['001']] = {'deskryptory w rekordzie': lista_deskryptorow, 'liczba deskryptorów': len(lista_deskryptorow), 'liczba dobrych deskryptorów': 0}
    
    for el in deskryptory_dict[row['001']]['deskryptory w rekordzie']:
        el = re.sub('\$y.*', '', el[4:]).replace('$2DBN', '')
        if any(desc == el for desc in BN_descriptors):
            deskryptory_dict[row['001']]['liczba dobrych deskryptorów'] += 1
            
    deskryptory_dict[row['001']]['procent dobrych deskryptorów'] = deskryptory_dict[row['001']]['liczba dobrych deskryptorów']/deskryptory_dict[row['001']]['liczba deskryptorów']

dobre2_lista = [k for k, v in deskryptory_dict.items() if deskryptory_dict[k]['procent dobrych deskryptorów'] > 0.5]
dobre2_df = df[df['001'].isin(dobre2_lista)]

df = df[~df['001'].isin(dobre2_lista)]

#%% puste 655 z dobrymi 650

dobre3_df = df[df['655'].isnull()]

df = df[~df['001'].isin(dobre3_df['001'])]

# czy sprawdzić frekwencyjnie deskryptory w df? czy czegoś nie uwzględniliśmy? - podjąć decyzję
# dodać starych ludzi 1. wziąć nazwy bn zmapowane na twórców pbl, 2. wyfiltrować tych, którzy zmarli do 1700, 3. przeszukać bazę pod kątem obecności tych ludzi w polu 100
# czy brać pod uwagę UKD?
#df.to_excel('niski_odsetek_deskryptorow.xlsx', index=False)


#%% filtrowanie BN po roku zgonu

file_list = drive.ListFile({'q': f"'{PBL_folder}' in parents and trashed=false"}).GetList() 
file_list = drive.ListFile({'q': "'0B0l0pB6Tt9olWlJVcDFZQ010R0E' in parents and trashed=false"}).GetList()
file_list = drive.ListFile({'q': "'1tPr_Ly9Lf0ZwgRQjj_iNVt-T15FSWbId' in parents and trashed=false"}).GetList()
file_list = drive.ListFile({'q': "'1OwlXSNuKdrnB9qZDvM-UMh5ul1n5SeyL' in parents and trashed=false"}).GetList()
file_list = drive.ListFile({'q': "'1BT4mZ40m_M1NyYYUiMFOSMtwA8krkrYT' in parents and trashed=false"}).GetList()
#[print(e['title'], e['id']) for e in file_list]
mapowanie_osob = [file['id'] for file in file_list if file['title'].startswith('mapowanie_osob_bn_pbl')]

mapowanie_osob_df = pd.DataFrame()
for file in tqdm(mapowanie_osob):
    sheet = gc.open_by_key(file)
    df_osoby = get_as_dataframe(sheet.worksheet('pbl_bn'), evaluate_formulas=True).dropna(how='all').dropna(how='all', axis=1).drop_duplicates()
    df_osoby = df_osoby[df_osoby['czy_ten_sam'] != 'nie'][['pbl_id', 'BN_id', 'BN_name']]
    df_osoby['BN_name'] = df_osoby['BN_name'].str.replace('\|\(', ' (').str.replace('\;\|', '; ').str.replace('\|$', '')
    df_osoby['index'] = df_osoby.index + 1
    df_osoby = cSplit(df_osoby, 'index', 'BN_name', '\|').drop(columns='index')
    mapowanie_osob_df = mapowanie_osob_df.append(df_osoby)

mapowanie_osob_df = mapowanie_osob_df.drop_duplicates().reset_index(drop=True)
def rok_zgonu(x):
    try:
        return int(re.search('(?<=\- ca |\-ca |\-ok\. |\-|po )(\d+)', x).group(0))
    except (TypeError, AttributeError):
        return None
mapowanie_osob_df['rok zgonu'] = mapowanie_osob_df['BN_name'].apply(lambda x: rok_zgonu(x))
mapowanie_osob_lista = mapowanie_osob_df[(mapowanie_osob_df['rok zgonu'].notnull()) & (mapowanie_osob_df['rok zgonu'] <= 1800)]['BN_name'].drop_duplicates().to_list()

#ujednolicenie nazw z BN 100 z tabelą wzorcową
def marc_parser_dict_for_field(string, subfield_code):
    subfield_list = re.findall(f'{subfield_code}.', string)
    dictionary_field = {}
    for subfield in subfield_list:
        subfield_escape = re.escape(subfield)
        string = re.sub(f'({subfield_escape})', r'❦\1', string)
    for subfield in subfield_list:
        subfield_escape = re.escape(subfield)
        regex = f'(^)(.*?\❦{subfield_escape}|)(.*?)(\,{{0,1}})((\❦{subfield_code})(.*)|$)'
        value = re.sub(regex, r'\3', string)
        dictionary_field[subfield] = value
    return dictionary_field

#zakres lat 
years = range(2013,2020)
   
path = 'F:/Cezary/Documents/IBL/Migracja z BN/bn_all/2021-02-08/'
files = [file for file in glob.glob(path + '*.mrk', recursive=True)]

encoding = 'utf-8'
new_list = []
for file_path in tqdm(files):
    marc_list = io.open(file_path, 'rt', encoding = encoding).read().splitlines()

    mrk_list = []
    for row in marc_list:
        if row.startswith('=LDR'):
            mrk_list.append([row])
        else:
            if row:
                mrk_list[-1].append(row)
            
    for sublist in mrk_list:
        try:
            year_biblio = int(''.join([ele for ele in sublist if ele.startswith('=008')])[13:17])
            bibliographic_level = ''.join([ele for ele in sublist if ele.startswith('=LDR')])[13]
            if year_biblio in years and bibliographic_level == 'm':
                for el in sublist:
                    if el.startswith('=100'):
                        el = el[6:]
                        el = marc_parser_dict_for_field(el, '\$')
                        el = ' '.join([v for k, v in el.items() if k in ['$a', '$c', '$d']])
                        if any(person == el for person in mapowanie_osob_lista):
                            new_list.append(sublist)
                            break
        except ValueError:
            pass

final_list = []
for lista in new_list:
    slownik = {}
    for el in lista:
        if el[1:4] in slownik:
            slownik[el[1:4]] += f"❦{el[6:]}"
        else:
            slownik[el[1:4]] = el[6:]
    final_list.append(slownik)

dobre4_df = pd.DataFrame(final_list).drop_duplicates().reset_index(drop=True)
fields = dobre4_df.columns.tolist()
fields = [i for i in fields if 'LDR' in i or re.compile('\d{3}').findall(i)]
dobre4_df = dobre4_df.loc[:, dobre4_df.columns.isin(fields)]
fields.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
dobre4_df = dobre4_df.reindex(columns=fields)   
df_original2 = dobre4_df.copy()

dobre4_df['czy polonik'] = dobre4_df.apply(lambda x: czy_polonik(x), axis=1)
dobre4_df = dobre4_df[dobre4_df['czy polonik'] == True]

dobre4_df = dobre4_df[(dobre4_df['380'].str.lower().str.contains('książ|book', regex=True)) | 
                      (dobre4_df['380'].isnull())]

#%% dydaktyka

#zakres lat 
years = range(2013,2020)
   
path = 'F:/Cezary/Documents/IBL/Migracja z BN/bn_all/2021-02-08/'
files = [file for file in glob.glob(path + '*.mrk', recursive=True)]

encoding = 'utf-8'
new_list = []
for file_path in tqdm(files):
    marc_list = io.open(file_path, 'rt', encoding = encoding).read().splitlines()

    mrk_list = []
    for row in marc_list:
        if row.startswith('=LDR'):
            mrk_list.append([row])
        else:
            if row:
                mrk_list[-1].append(row)
                               
    for sublist in mrk_list:
        try:
            year_biblio = int(''.join([ele for ele in sublist if ele.startswith('=008')])[13:17])
            bibliographic_level = ''.join([ele for ele in sublist if ele.startswith('=LDR')])[13]
            if year_biblio in years and bibliographic_level == 'm':
                x650s = []
                x655s = []
                for el in sublist:
                    if el.startswith('=650'):
                        el = re.sub('\$y.*', '', el[10:]).replace('$2DBN', '').strip()
                        x650s.append(el)
                    elif el.startswith('=655'):
                        el = re.sub('\$y.*', '', el[10:]).replace('$2DBN', '').strip()
                        x655s.append(el)
                if any(desc in x650s for desc in dydaktyka_650) and any(desc in x655s for desc in dydaktyka_655):
                    new_list.append(sublist)                    
        except ValueError:
            pass

final_list = []
for lista in new_list:
    slownik = {}
    for el in lista:
        if el[1:4] in slownik:
            slownik[el[1:4]] += f"❦{el[6:]}"
        else:
            slownik[el[1:4]] = el[6:]
    final_list.append(slownik)

dobre5_df = pd.DataFrame(final_list).drop_duplicates().reset_index(drop=True)
fields = dobre5_df.columns.tolist()
fields = [i for i in fields if 'LDR' in i or re.compile('\d{3}').findall(i)]
dobre5_df = dobre5_df.loc[:, dobre5_df.columns.isin(fields)]
fields.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
dobre5_df = dobre5_df.reindex(columns=fields)   
df_original3 = dobre5_df.copy()

test = dobre5_df[['650', '655']]
#%% połączenie zbiorów
dobre1_df.columns.values
dobre2_df.columns.values
dobre3_df.columns.values
dobre4_df.columns.values

bn_harvested = pd.concat([dobre1_df.drop(columns=['czy polonik', 'gatunki literackie']), dobre2_df.drop(columns=['czy polonik', 'gatunki literackie']), dobre3_df.drop(columns=['czy polonik', 'gatunki literackie']), dobre4_df.drop(columns='czy polonik')]).drop_duplicates().reset_index(drop=False)

test = bn_harvested.head(100)

df_to_mrc(bn_harvested, '❦', f'bn_harvested_{year}_{month}_{day}.mrc', f'bn_harvested_errors_{year}_{month}_{day}.txt')

#%% porównanie dwóch metod harvestowania BN

df_stare = mrk_to_df('F:/Cezary/Documents/IBL/Libri/Iteracja 2021-02/libri_marc_bn_books_2021-2-9.mrk')
sheet = gc.create(f'porównanie_podejść_{year}_{month}_{day}', '1xzqGIfZllmXXTh2dJABeHbRPFAM34nbw')

worksheet = sheet.worksheet('Arkusz1')
worksheet.update_title('jest_a_nie_bylo')
jest_a_nie_bylo = bn_harvested[~bn_harvested['001'].isin(df_stare['001'])][['LDR', '001', '008', '080', '100', '245', '650', '655', '380', '386']]
set_with_dataframe(worksheet, jest_a_nie_bylo)

jest_tu_i_tu = bn_harvested[bn_harvested['001'].isin(df_stare['001'])][['LDR', '001', '008', '080', '100', '245', '650', '655', '380', '386']]
try:
    set_with_dataframe(sheet.worksheet('jest_tu_i_tu'), jest_tu_i_tu)
except gs.WorksheetNotFound:
    sheet.add_worksheet(title='jest_tu_i_tu', rows="100", cols="20")
    set_with_dataframe(sheet.worksheet('jest_tu_i_tu'), jest_tu_i_tu)

bylo_a_nie_ma = df_stare[~df_stare['001'].isin(bn_harvested['001'])]['001'].to_list()

years = range(2013,2020)
   
path = 'F:/Cezary/Documents/IBL/Migracja z BN/bn_all/2021-02-08/'
files = [file for file in glob.glob(path + '*.mrk', recursive=True)]

encoding = 'utf-8'
new_list = []
for file_path in tqdm(files):
    marc_list = io.open(file_path, 'rt', encoding = encoding).read().splitlines()

    mrk_list = []
    for row in marc_list:
        if row.startswith('=LDR'):
            mrk_list.append([row])
        else:
            if row:
                mrk_list[-1].append(row)
            
    for sublist in mrk_list:
        try:
            year_biblio = int(''.join([ele for ele in sublist if ele.startswith('=008')])[13:17])
            bibliographic_level = ''.join([ele for ele in sublist if ele.startswith('=LDR')])[13]
            if year_biblio in years and bibliographic_level == 'm':
                for el in sublist:
                    if el.startswith('=001'):
                        el = el[6:]
                        if el in bylo_a_nie_ma:
                            new_list.append(sublist)
                            break
        except ValueError:
            pass

final_list = []
for lista in new_list:
    slownik = {}
    for el in lista:
        if el[1:4] in slownik:
            slownik[el[1:4]] += f"❦{el[6:]}"
        else:
            slownik[el[1:4]] = el[6:]
    final_list.append(slownik)

bylo_a_nie_ma = pd.DataFrame(final_list).drop_duplicates().reset_index(drop=True)
fields = bylo_a_nie_ma.columns.tolist()
fields = [i for i in fields if 'LDR' in i or re.compile('\d{3}').findall(i)]
bylo_a_nie_ma = bylo_a_nie_ma.loc[:, bylo_a_nie_ma.columns.isin(fields)]
fields.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
bylo_a_nie_ma = bylo_a_nie_ma.reindex(columns=fields)[['LDR', '001', '008', '080', '100', '245', '650', '655', '380', '386']]

try:
    set_with_dataframe(sheet.worksheet('bylo_a_nie_ma'), bylo_a_nie_ma)
except gs.WorksheetNotFound:
    sheet.add_worksheet(title='bylo_a_nie_ma', rows="100", cols="20")
    set_with_dataframe(sheet.worksheet('bylo_a_nie_ma'), bylo_a_nie_ma)
    
worksheets = sheet.worksheets()
for worksheet in worksheets:
    sheet.batch_update({
        "requests": [
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": worksheet._properties['sheetId'],
                        "dimension": "ROWS",
                        "startIndex": 0,
                        #"endIndex": 100
                    },
                    "properties": {
                        "pixelSize": 20
                    },
                    "fields": "pixelSize"
                }
            }
        ]
    })
    
    worksheet.freeze(rows=1)
    worksheet.set_basic_filter()

#%% analiza jakościowa

list_of_dfs = [dobre1_df, dobre2_df, dobre3_df, dobre4_df]
list_of_dfs_names = ['dobre1_df', 'dobre2_df', 'dobre3_df', 'dobre4_df']

for name, data_frame in zip(list_of_dfs_names, list_of_dfs):
    if 'b1000000245117' in data_frame['001'].to_list():
        print(name)

test = dobre1_df[dobre1_df['001'] == 'b1000000245117']

test = df[df['001'] == 'b0000005818158'].squeeze()

'b0000005818158'

#%% notatki

test = pd.DataFrame(BN_descriptors, columns=['deskryptory'])
test.to_excel('deskryptory_do_filtrowania.xlsx', index=False)



#Karolina - udokumentować proces!!!

df_stare = mrk_to_df('F:/Cezary/Documents/IBL/Libri/Iteracja 2021-02/libri_marc_bn_books_2021-2-9.mrk')

#%% odsiewanie po deskryptorach
sheet = gc.open_by_key('1a_jLhXHmAI4YitAyG1e8_8uHC0n07gjRDWC4VbHsnrY')
df = get_as_dataframe(sheet.worksheet('jest_a_nie_bylo'), evaluate_formulas=True).dropna(how='all').dropna(how='all', axis=1)

podejrzane_deskryptory = ['Muzyka (przedmiot szkolny)', 'Edukacja artystyczna',  'Fotografia teatralna', 'Odbitka barwna', 'Korespondencja handlowa', 'Książka kucharska', 'Gatunek zagrożony', 'Reportaż radiowy', 'Przewodnik po wystawie', 'Turystyka dziecięca', 'Słownik frazeologiczny', 'Rozważania i rozmyślania religijne', 'Pedagogika$x', 'Budownictwo$xprojekty$xprzekłady', 'Język środowiskowy$xprzekłady', 'Drama (pedagogika)']

ids = df[(df[650].str.contains('Opera (przedstawienie)', regex=False)) & (df[655].str.contains('Program teatralny'))][1].to_list()

ids += 

test = df[(df['LDR'].str[6] == 'g') & ((~df[386].str.contains('Film polski', regex=False, na=False)) & (~df[655].str.contains('polsk', na=False)))][[1, 80, 245, 650, 655, 380, 386]]
test2 = df[(df['LDR'].str[6] == 'g') & ((~df[386].str.contains('Film polski', regex=False, na=False)) | (~df[655].str.contains('polsk', na=False)))][[1, 80, 245, 650, 655, 380, 386]]
test2 = test2[~test2[1].isin(test[1])]

for el in podejrzane_deskryptory:
    test = df[(df[650].str.contains(el, regex=False)) | 
              (df[655].str.contains(el, regex=False))][1].to_list()
    ids += test
ids = list(set(ids))    

df = df[df[1].isin(ids)][[80, 245, 650, 655, 380, 386]]


#następne - reportaż radiowy
test = df[(df[650].str.contains(podejrzane_deskryptory[-1], regex=False)) | 
          (df[655].str.contains(podejrzane_deskryptory[-1], regex=False))][[80, 245, 650, 655, 380, 386]]



# LDR g to 386 Przynależność kulturowa film polski - to ma zostać z grupy "g"
# \7$aImage$2DBN❦\7$aSemiotyka$2DBN









df1 = get_as_dataframe(sheet.worksheet('jest_a_nie_bylo'), evaluate_formulas=True).dropna(how='all').dropna(how='all', axis=1)
df2 = get_as_dataframe(sheet.worksheet('jest_tu_i_tu'), evaluate_formulas=True).dropna(how='all').dropna(how='all', axis=1)
df = pd.concat([df1, df2])

df = df[df['LDR'].str[6] == 'g']
df.to_excel('filmy do wywalenia.xlsx', index=False)
#wywalić filmy ze wszystkich tabel
#sprawdzić reguły wywalania z jest a nie było dla jest tu i tu
























