#%% import
import pandas as pd
import numpy as np
from my_functions import marc_parser_1_field, cSplit, df_to_mrc, gsheet_to_df, mrc_to_mrk
import pandasql
import json
import requests
import io
import cx_Oracle
import regex as re
from functools import reduce
import glob
from json.decoder import JSONDecodeError
from urllib.parse import urlparse
import datetime
import ast
from SPUB_importer_read_data import read_MARC21
from tqdm import tqdm
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import gspread as gs
from google_drive_research_folders import PBL_folder
from gspread_dataframe import get_as_dataframe, set_with_dataframe

#%% date
now = datetime.datetime.now()
year = now.year
month = '{:02}'.format(now.month)
day = '{:02}'.format(now.day)

#%% def

def dziedzina_PBL(x):
    try:
        if bool(re.search('(?<=\$a|:|\[|\+|\()(82)', x)):
            val = 'ukd_lit'
        elif bool(re.search('(?<=\$a|:|\[|\+)(791)', x)) or bool(re.search('(?<=\$a|:)(792)', x)) or bool(re.search('\$a7\.09', x)):
            val = 'ukd_tfrtv'
        elif bool(re.search('(?<=\$a01)(\(|\/|2|4|5|9)', x)) or bool(re.search('(?<=\$a|\[])(050)', x)) or bool(re.search('(?<=\$a|:|\[|\+|\()(811\.162)', x)):
            val = 'ukd_biblio'
        elif bool(re.search('\$a002', x)) or bool(re.search('(?<=\$a|:)(305)', x)) or bool(re.search('(?<=\$a39|:39)(\(438\)|8\.2)', x)) or bool(re.search('(?<=\$a|:)(929[^\.]051)', x)) or bool(re.search('(?<=\$a|:)(929[^\.]052)', x)):
            val = 'ukd_pogranicze'
        else:
            val = 'bez_ukd_PBL'
    except TypeError:
        val = 'bez_ukd_PBL'
    return val

def position_of_dash(x):
    try:
        if bool(re.search('(\\\\\$a|:)(821\.)', x)):
            val = x.index('-')
        else: 
            val = np.nan
    except (TypeError, ValueError):
        val = np.nan
    return val

def position_of_091(x):
    try:
        val = x.index('(091)')
    except (AttributeError, ValueError):
        val = np.nan
    return val

def rodzaj_zapisu(x):
    if pd.isnull(x['dash_position']) and pd.notnull(x['655']) and '$x' in x['655'] and '$y' in x['655']:
        val = 'przedmiotowy'
    elif pd.isnull(x['dash_position']) and pd.notnull(x['655']) and '$x' not in x['655'] and '$y' in x['655']:
        val = 'podmiotowy'
    elif pd.isnull(x['dash_position']):
        val = 'przedmiotowy'
    elif pd.notnull(x['dash_position']) and pd.notnull(x['091_position']) and int(x['dash_position']) > int(x['091_position']):
        val = 'przedmiotowy'
    elif pd.notnull(x['dash_position']) and pd.notnull(x['655']) and '$y' not in x['655']:
        val = 'przedmiotowy'
    elif pd.notnull(x['091_position']):
        val = 'przedmiotowy'
    else:
        val = 'podmiotowy'
    return val
#%%mode
#with people OR without people
# preferable = without people
# mode = input('Enter the mode: ')

#%% download of the list of journals
# trzeba zwrócić uwagę, czy po 25.01.2021 nie były przeprowadzane update'y mapowania czasopism między PBL i BN

bn_magazines_spreadsheets = ['Sheet1', 'update 25.01.2021']

bn_magazines = pd.DataFrame()
for sheet in bn_magazines_spreadsheets:
    df = gsheet_to_df('1V6BreA4_cEb3FRvv53E5ri2Yl1u3Z2x-yQxxBbAzCeo', sheet)
    bn_magazines = bn_magazines.append(df)

bn_magazines = bn_magazines[bn_magazines['decyzja'] == 'tak']['bn_magazine'].tolist()

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

#%% BN data extraction

path = 'F:/Cezary/Documents/IBL/BN/bn_all/2021-02-08/'
files = [file for file in glob.glob(path + '*.mrk', recursive=True)]
years = range(2004,2022)
encoding = 'utf-8'
new_list = []
for file_path in tqdm(files):
    mrk_list = read_MARC21(file_path)
                
    for sublist in mrk_list:
        try:
            year_biblio = int(''.join([ele for ele in sublist if ele.startswith('=008')])[13:17])
            if year_biblio in years:
                score1, score2 = 0, 0
                for el in sublist:
                    if el.startswith('=773'):
                        val = re.search('(\$t)(.+?)(\$|$)', el).group(2)
                        if val in bn_magazines:
                            score1 += 1
                    if el.startswith('=650') or el.startswith('=655'):
                        el = re.sub('\$y.*', '', el[10:]).replace('$2DBN', '').strip()
                        if any(desc == el for desc in BN_descriptors):
                            score2 +=1
                if score1 > 0 and score2 > 0:
                    new_list.append(sublist)
        except (ValueError, AttributeError):
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

marc_df = pd.DataFrame(final_list)
fields = marc_df.columns.tolist()
fields = [i for i in fields if 'LDR' in i or re.compile('\d{3}').findall(i)]
marc_df = marc_df.loc[:, marc_df.columns.isin(fields)]
fields.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
marc_df = marc_df.reindex(columns=fields)

marc_df.to_excel(f'bn_harvested_articles_{year}_{month}_{day}.xlsx', index=False)

df_to_mrc(marc_df, '❦', f'marc_df_articles_{year}_{month}_{day}.mrc', f'marc_df_articles_errors_{year}_{month}_{day}.txt')

#%% porównanie zasobów
nowe = read_MARC21('marc_df_articles_2021_05_25.mrk')
nowe_id = [[f[6:] for f in e if f.startswith('=001')] for e in nowe]
nowe_id = [e for sub in nowe_id for e in sub]
stare = read_MARC21('F:/Cezary/Documents/IBL/Libri/Iteracja 2021-02/libri_marc_bn_articles_2021-2-9.mrk')
stare_id = [[f[6:] for f in e if f.startswith('=001')] for e in stare]
stare_id = [e for sub in stare_id for e in sub]

bylo_a_nie_ma = list(set(stare_id) - set(nowe_id))
final_list = []
for lista in stare:
    slownik = {}
    for el in lista:
        if el[1:4] in slownik:
            slownik[el[1:4]] += f"❦{el[6:]}"
        else:
            slownik[el[1:4]] = el[6:]
    final_list.append(slownik)
bylo_a_nie_ma_df = pd.DataFrame(final_list).drop_duplicates().reset_index(drop=True)
bylo_a_nie_ma_df = bylo_a_nie_ma_df[bylo_a_nie_ma_df['001'].isin(bylo_a_nie_ma)][['001', '245', '650', '655', '773']]

test = [e.split('❦') for e in bylo_a_nie_ma_df['650'] if pd.notnull(e)]
test = list(set([e for sub in test for e in sub]))
test = pd.DataFrame(test)
test.to_excel('test.xlsx', index=False)
        


jest_a_nie_bylo = list(set(nowe_id) - set(stare_id))

































#%% PBL SQL connection

dsn_tns = cx_Oracle.makedsn('pbl.ibl.poznan.pl', '1521', service_name='xe')
connection = cx_Oracle.connect(user='IBL_SELECT', password='CR333444', dsn=dsn_tns, encoding='windows-1250')

#%% processing

bn_cz_mapping = pd.read_excel('F:/Cezary/Documents/IBL/Pliki python/bn_cz_mapping.xlsx')
bn_articles = marc_df.copy()  
bn_articles['id'] = bn_articles['001']
bn_articles['005'] = bn_articles['005'].astype(str)
bn_articles['LDR'] = bn_articles['LDR'].apply(lambda x: x.replace('naa', 'nab') if x[5:8] == 'naa' else x)

pbl_viaf_links = ['1cEz73dGN2r2-TTc702yne9tKfH9PQ6UyAJ2zBSV6Jb0', '1_Bhwzo0xu4yTn8tF0ZNAZq9iIAqIxfcrjeLVCm_mggM', '1L-7Zv9EyLr5FeCIY_s90rT5Hz6DjAScCx6NxfuHvoEQ']
pbl_viaf = pd.DataFrame()
for elem in pbl_viaf_links:
    df = gsheet_to_df(elem, 'pbl_bn').drop_duplicates()
    df = df[df['czy_ten_sam'] != 'nie'][['pbl_id', 'BN_id', 'BN_name']]
    df['BN_name'] = df['BN_name'].str.replace('\|\(', ' (').str.replace('\;\|', '; ').str.replace('\|$', '')
    df['index'] = df.index + 1
    df = cSplit(df, 'index', 'BN_name', '\|').drop(columns='index')
    pbl_viaf = pbl_viaf.append(df)
pbl_viaf = pbl_viaf.drop_duplicates()

if mode == "with people":
    
    # doesn't work for articles (still good for books)
    
    tworca_i_dzial = """select tw.tw_tworca_id "pbl_id", dz.dz_dzial_id||'|'||dz.dz_nazwa "osoba_pbl_dzial_id_name"
                        from pbl_tworcy tw
                        full join pbl_dzialy dz on dz.dz_dzial_id=tw.tw_dz_dzial_id"""
    tworca_i_dzial = pd.read_sql(tworca_i_dzial, con=connection).fillna(value = np.nan)
    tworca_i_dzial['pbl_id'] = tworca_i_dzial['pbl_id'].apply(lambda x: '{:4.0f}'.format(x))
    
    X100 = marc_parser_1_field(bn_articles, 'id', '100', '\$')[['id', '$a', '$c', '$d']].replace(r'^\s*$', np.NaN, regex=True)
    X100['name'] = X100[['$a', '$d', '$c']].apply(lambda x: ' '.join(x.dropna().astype(str)), axis=1)
    X100 = X100[['id', 'name']]
    X100['name'] = X100['name'].str.replace("(\))(\.$)", r"\1").apply(lambda x: re.sub('(\p{Ll})(\.$)', r'\1', x))
    X100 = pd.merge(X100, pbl_viaf, how='inner', left_on='name', right_on='BN_name')[['id', 'name', 'pbl_id']]
    X100 = pd.merge(X100, tworca_i_dzial, how='left', on='pbl_id')[['id', 'osoba_pbl_dzial_id_name']]
    X100['osoba_bn_autor'] = X100.groupby('id')['osoba_pbl_dzial_id_name'].transform(lambda x: '❦'.join(x.astype(str)))
    X100 = X100.drop(columns='osoba_pbl_dzial_id_name').drop_duplicates()
    
    X600 = marc_parser_1_field(bn_articles, 'id', '600', '\$')[['id', '$a', '$c', '$d']].replace(r'^\s*$', np.NaN, regex=True)
    X600['name'] = X600[['$a', '$d', '$c']].apply(lambda x: ' '.join(x.dropna().astype(str)), axis=1)
    X600 = X600[['id', 'name']]
    X600['name'] = X600['name'].str.replace("(\))(\.$)", r"\1").apply(lambda x: re.sub('(\p{Ll})(\.$)', r'\1', x))
    X600 = pd.merge(X600, pbl_viaf, how='inner', left_on='name', right_on='BN_name')[['id', 'name', 'pbl_id']]
    X600 = pd.merge(X600, tworca_i_dzial, how='left', on='pbl_id')[['id', 'osoba_pbl_dzial_id_name']]
    X600['osoba_bn_temat'] = X600.groupby('id')['osoba_pbl_dzial_id_name'].transform(lambda x: '❦'.join(x.astype(str)))
    X600 = X600.drop(columns='osoba_pbl_dzial_id_name').drop_duplicates()

    bn_articles = [bn_articles, X100, X600]
    bn_articles = reduce(lambda left,right: pd.merge(left,right,on='id', how = 'outer'), bn_articles)

bn_articles['dziedzina_PBL'] = bn_articles['080'].apply(lambda x: dziedzina_PBL(x))

literary_words = 'literat|literac|pisar|bajk|dramat|epigramat|esej|felieton|film|komedi|nowel|opowiadani|pamiętnik|poemiks|poezj|powieść|proza|reportaż|satyr|wspomnieni|Scenariusze zajęć|Podręczniki dla gimnazjów|teatr|aforyzm|baśń|baśnie|polonijn|dialogi|fantastyka naukowa|legend|pieśń|poemat|przypowieś|honoris causa|filologi|kino polskie|pieśni|interpretacj|poet|liryk'

if mode == "with people":   
    literary_word_keys = bn_articles.copy()[['id', '080', '245', '600', '610', '630', '648', '650', '651', '655', '658', 'osoba_bn_autor', 'osoba_bn_temat', 'dziedzina_PBL']]
elif mode == 'without people':
    literary_word_keys = bn_articles.copy()[['id', '080', '245', '600', '610', '630', '648', '650', '651', '655', '658', 'dziedzina_PBL']]
literary_word_keys['literary_word_keys'] = literary_word_keys['245'].str.contains(literary_words, flags=re.IGNORECASE) | literary_word_keys['600'].str.contains(literary_words, flags=re.IGNORECASE) | literary_word_keys['610'].str.contains(literary_words, flags=re.IGNORECASE) | literary_word_keys['630'].str.contains(literary_words, flags=re.IGNORECASE) | literary_word_keys['648'].str.contains(literary_words, flags=re.IGNORECASE) | literary_word_keys['650'].str.contains(literary_words, flags=re.IGNORECASE) | literary_word_keys['651'].str.contains(literary_words, flags=re.IGNORECASE) | literary_word_keys['655'].str.contains(literary_words, flags=re.IGNORECASE) | literary_word_keys['658'].str.contains(literary_words, flags=re.IGNORECASE)
literary_word_keys = literary_word_keys[literary_word_keys['literary_word_keys'] == True][['id', 'literary_word_keys']]
bn_articles = pd.merge(bn_articles, literary_word_keys, how='left', on='id')

memories_words = 'pamiętniki i wspomnienia|literatura podróżnicza|pamiętniki|reportaż|relacja z podróży'
memories = bn_articles.copy()[['id', '245', '600', '610', '630', '648', '650', '651', '655', '658']]
memories['wspomnienia'] = memories['245'].str.contains(memories_words, flags=re.IGNORECASE) | memories['600'].str.contains(memories_words, flags=re.IGNORECASE) | memories['600'].str.contains(memories_words, flags=re.IGNORECASE) | memories['610'].str.contains(memories_words, flags=re.IGNORECASE) | memories['630'].str.contains(memories_words, flags=re.IGNORECASE) | memories['648'].str.contains(memories_words, flags=re.IGNORECASE) | memories['650'].str.contains(memories_words, flags=re.IGNORECASE) | memories['655'].str.contains(memories_words, flags=re.IGNORECASE) | memories['658'].str.contains(memories_words, flags=re.IGNORECASE)
memories = memories[memories['wspomnienia'] == True][['id', 'wspomnienia']]
bn_articles = pd.merge(bn_articles, memories, how='left', on='id')

bible_words = "biblia|analiza i interpretacja|edycja krytyczna|materiały konferencyjne"
bible = bn_articles.copy()[['id', '245', '650', '655']]
bible['biblia'] = bible['245'].str.contains(bible_words, flags=re.IGNORECASE) | bible['650'].str.contains(bible_words, flags=re.IGNORECASE) | bible['650'].str.contains(bible_words, flags=re.IGNORECASE)
bible = bible[bible['biblia'] == True][['id', 'biblia']]
bn_articles = pd.merge(bn_articles, bible, how='left', on='id')

zle = bn_articles.copy()
if mode == "with people":   
    zle = zle[(zle['osoba_bn_autor'].isnull()) & 
            (zle['osoba_bn_temat'].isnull()) & 
            (zle['dziedzina_PBL'] == 'bez_ukd_PBL') & 
            (zle['literary_word_keys'].isnull()) & 
            (zle['wspomnienia'].isnull()) & 
            (zle['biblia'].isnull())]
    zle = zle[['id', '080', '773', '245', '600', '610', '630', '648', '650', '651', '655', '658', 'osoba_bn_autor', 'osoba_bn_temat', 'dziedzina_PBL', 'literary_word_keys', 'wspomnienia', 'biblia']]
elif mode == 'without people':
    zle = zle[(zle['dziedzina_PBL'] == 'bez_ukd_PBL') & 
            (zle['literary_word_keys'].isnull()) & 
            (zle['wspomnienia'].isnull()) & 
            (zle['biblia'].isnull())]
    zle = zle[['id', '080', '773', '245', '600', '610', '630', '648', '650', '651', '655', '658', 'dziedzina_PBL', 'literary_word_keys', 'wspomnienia', 'biblia']]

dobre = bn_articles.copy()
dobre = dobre[~dobre['id'].isin(zle['id'])]
if mode == "with people":     
    dobre = dobre[['id', '080', '773', '245', '600', '610', '630', '648', '650', '651', '655', '658', 'osoba_bn_autor', 'osoba_bn_temat', 'dziedzina_PBL', 'literary_word_keys', 'wspomnienia', 'biblia']]
elif mode == "without people":
    dobre = dobre[['id', '080', '773', '245', '600', '610', '630', '648', '650', '651', '655', '658', 'dziedzina_PBL', 'literary_word_keys', 'wspomnienia', 'biblia']]
dobre['decision'] = 'OK'
dobre = dobre[['id', 'decision']]
bn_articles = pd.merge(bn_articles, dobre, 'left', 'id')

# =============================================================================
# bn_articles.to_excel('bn_magazines_to_statistics.xlsx', index=False)
# bn_articles = pd.read_excel('bn_magazines_to_statistics.xlsx')
# =============================================================================

bn_articles = bn_articles[bn_articles['decision'] == 'OK']

fields_to_remove = bn_cz_mapping[bn_cz_mapping['cz'] == 'del']['bn'].to_list()
fields_to_remove = [x[1:] if x[0] == 'X' else x for x in fields_to_remove]

bn_articles = bn_articles.loc[:, ~bn_articles.columns.isin(fields_to_remove)]

#UKD
bn_articles['czy_ma_ukd'] = bn_articles['080'].apply(lambda x: 'tak' if pd.notnull(x) else 'nie')   
bn_articles['dash_position'] = bn_articles['080'].apply(lambda x: position_of_dash(x))
bn_articles['091_position'] = bn_articles['080'].apply(lambda x: position_of_091(x))
bn_articles['rodzaj'] = bn_articles.apply(lambda x: rodzaj_zapisu(x), axis=1)

# wczytanie danych pbl
pbl_dzialy = pd.read_sql('select * from PBL_DZIALY', con=connection).iloc[:, [0,2,5]]
pbl_dzialy_path = pd.merge(pbl_dzialy, pbl_dzialy, how='left', left_on = 'DZ_DZ_DZIAL_ID', right_on='DZ_DZIAL_ID').drop(columns='DZ_DZIAL_ID_y')
pbl_dzialy_path.columns = ['DZ_DZIAL_ID', 'DZ_NAZWA', 'NAD_DZ_DZIAL_ID', 'NAD_DZ_NAZWA', 'NAD_NAD_DZ_DZIAL_ID']
pbl_dzialy_path = pd.merge(pbl_dzialy_path, pbl_dzialy, how='left', left_on = 'NAD_NAD_DZ_DZIAL_ID', right_on='DZ_DZIAL_ID').drop(columns='DZ_DZIAL_ID_y')
pbl_dzialy_path.columns = ['DZ_DZIAL_ID', 'DZ_NAZWA', 'NAD_DZ_DZIAL_ID', 'NAD_DZ_NAZWA', 'NAD_NAD_DZ_DZIAL_ID', 'NAD_NAD_DZ_NAZWA', 'NAD_NAD_NAD_DZ_DZIAL_ID']
pbl_dzialy_path = pd.merge(pbl_dzialy_path, pbl_dzialy, how='left', left_on = 'NAD_NAD_NAD_DZ_DZIAL_ID', right_on='DZ_DZIAL_ID').drop(columns='DZ_DZIAL_ID_y')
pbl_dzialy_path.columns = ['DZ_DZIAL_ID', 'DZ_NAZWA', 'NAD_DZ_DZIAL_ID', 'NAD_DZ_NAZWA', 'NAD_NAD_DZ_DZIAL_ID', 'NAD_NAD_DZ_NAZWA', 'NAD_NAD_NAD_DZ_DZIAL_ID', 'NAD_NAD_NAD_DZ_NAZWA', 'NAD_NAD_NAD_NAD_DZ_DZIAL_ID']
pbl_dzialy_path = pd.merge(pbl_dzialy_path, pbl_dzialy, how='left', left_on = 'NAD_NAD_NAD_NAD_DZ_DZIAL_ID', right_on='DZ_DZIAL_ID').drop(columns='DZ_DZIAL_ID_y')
pbl_dzialy_path.columns = ['DZ_DZIAL_ID', 'DZ_NAZWA', 'NAD_DZ_DZIAL_ID', 'NAD_DZ_NAZWA', 'NAD_NAD_DZ_DZIAL_ID', 'NAD_NAD_DZ_NAZWA', 'NAD_NAD_NAD_DZ_DZIAL_ID', 'NAD_NAD_NAD_DZ_NAZWA', 'NAD_NAD_NAD_NAD_DZ_DZIAL_ID', 'NAD_NAD_NAD_NAD_DZ_NAZWA', 'NAD_NAD_NAD_NAD_NAD_DZ_DZIAL_ID']
pbl_dzialy_path = pd.merge(pbl_dzialy_path, pbl_dzialy, how='left', left_on = 'NAD_NAD_NAD_NAD_NAD_DZ_DZIAL_ID', right_on='DZ_DZIAL_ID').drop(columns='DZ_DZIAL_ID_y')
pbl_dzialy_path.columns = ['DZ_DZIAL_ID', 'DZ_NAZWA', 'NAD_DZ_DZIAL_ID', 'NAD_DZ_NAZWA', 'NAD_NAD_DZ_DZIAL_ID', 'NAD_NAD_DZ_NAZWA', 'NAD_NAD_NAD_DZ_DZIAL_ID', 'NAD_NAD_NAD_DZ_NAZWA', 'NAD_NAD_NAD_NAD_DZ_DZIAL_ID', 'NAD_NAD_NAD_NAD_DZ_NAZWA', 'NAD_NAD_NAD_NAD_NAD_DZ_DZIAL_ID', 'NAD_NAD_NAD_NAD_NAD_DZ_NAZWA', 'NAD_NAD_NAD_NAD_NAD_NAD_DZ_DZIAL_ID']
pbl_dzialy_path = pbl_dzialy_path[pbl_dzialy_path['DZ_DZIAL_ID'] != 0]

pbl_dz_osob = pbl_dzialy_path[(pbl_dzialy_path['DZ_NAZWA'].str.contains('osobowe')) |
                              (pbl_dzialy_path['NAD_DZ_NAZWA'].str.contains('osobowe')) |
                              (pbl_dzialy_path['NAD_NAD_DZ_NAZWA'].str.contains('osobowe')) |
                              (pbl_dzialy_path['NAD_NAD_NAD_DZ_NAZWA'].str.contains('osobowe')) |
                              (pbl_dzialy_path['NAD_NAD_NAD_NAD_DZ_NAZWA'].str.contains('osobowe')) |
                              (pbl_dzialy_path['NAD_NAD_NAD_NAD_NAD_DZ_NAZWA'].str.contains('osobowe'))].iloc[:, :8]

df_osoby_dzial = pd.DataFrame()
col_set = [[0,1,2,3], [0,1,4,5], [0,1,6,7]]
for s in col_set:
    df_set = pbl_dz_osob.iloc[:, s]
    df_set.columns = ['DZ_DZIAL_ID', 'DZ_NAZWA', 'DZ_DZ_DZIAL_ID', 'DZ_DZ_NAZWA']
    df_osoby_dzial = df_osoby_dzial.append(df_set)
df_osoby_dzial = df_osoby_dzial[df_osoby_dzial['DZ_DZ_DZIAL_ID'].notnull()]    
listOfSeries = [pd.Series([15043, "Hasła osobowe(luksemburska)",15043, "Hasła osobowe(luksemburska)"], index=df_osoby_dzial.columns),
                pd.Series([430, "Hasła osobowe (Ludzie teatru i filmu)",430, "Hasła osobowe (Ludzie teatru i filmu)"], index=df_osoby_dzial.columns)]
df_osoby_dzial = df_osoby_dzial.append(listOfSeries, ignore_index=True).reset_index(drop=True)

df_osoby_dzial_bez_teatru = df_osoby_dzial.copy()[df_osoby_dzial['DZ_DZIAL_ID'] != 430]
pbl_tworcy = pd.read_sql('select * from PBL_TWORCY', con=connection).iloc[:, :4]
tworca_i_dzial = pd.merge(pbl_tworcy, df_osoby_dzial_bez_teatru, how='inner', left_on='TW_DZ_DZIAL_ID', right_on='DZ_DZ_DZIAL_ID')
def tworca_litera_dzial_pl(x):
    if x['DZ_DZ_DZIAL_ID'] == 148 and x['TW_NAZWISKO'][0] == x['DZ_NAZWA'][-1]:
        val = True
    elif x['DZ_DZ_DZIAL_ID'] == 148 and x['TW_NAZWISKO'][0] != x['DZ_NAZWA'][-1]:
        val = False
    else:
        val = True
    return val

tworca_i_dzial['select'] = tworca_i_dzial.apply(lambda x: tworca_litera_dzial_pl(x), axis=1)
tworca_i_dzial = tworca_i_dzial[tworca_i_dzial['select'] == True].drop(columns=['select'])
tworca_i_dzial['DZ_NAZWA'] = tworca_i_dzial.apply(lambda x: x['DZ_DZ_NAZWA'] if x['DZ_DZ_DZIAL_ID'] == 148 else x['DZ_NAZWA'], axis=1)
tworca_i_dzial = tworca_i_dzial[['TW_TWORCA_ID', 'TW_NAZWISKO', 'TW_IMIE', 'DZ_NAZWA']]
tworca_i_dzial['name'] = tworca_i_dzial['TW_NAZWISKO'].str.lower().str.replace("\\W", "") + tworca_i_dzial['TW_IMIE'].str.lower().str.replace("\\W", "") 

# przypisanie działu pbl
# przedmiotowe
przedmiotowe = bn_articles.copy()
przedmiotowe = przedmiotowe[przedmiotowe['rodzaj'] == 'przedmiotowy']

X600 = marc_parser_1_field(przedmiotowe, 'id', '600', '\$')[['id', '$a', '$c', '$d']].replace(r'^\s*$', np.NaN, regex=True)
X600['name'] = X600[['$a', '$d', '$c']].apply(lambda x: ' '.join(x.dropna().astype(str)), axis=1)
X600 = X600[['id', 'name']]
X600['name'] = X600['name'].str.replace("(\))(\.$)", r"\1").apply(lambda x: re.sub('(\p{Ll})(\.$)', r'\1', x))
X600 = pd.merge(X600, pbl_viaf, how='inner', left_on='name', right_on='BN_name')[['id', 'name', 'pbl_id']]
X600['pbl_id'] = X600['pbl_id'].astype(np.int64)
X600 = pd.merge(X600, tworca_i_dzial, how='left', left_on='pbl_id', right_on='TW_TWORCA_ID')[['id', 'DZ_NAZWA']]
X600 = X600[X600['DZ_NAZWA'].notnull()]

przedmiotowe = przedmiotowe[~przedmiotowe['id'].isin(X600['id'])]
X655 = marc_parser_1_field(przedmiotowe, 'id', '655', '\$')[['id', '$a']]
pbl_literatury = pbl_dzialy_path.copy()[pbl_dzialy_path['NAD_DZ_DZIAL_ID'] == 30].iloc[:, :2]
pbl_literatury['nazwa'] = pbl_literatury['DZ_NAZWA'].str.replace("(.*?)( )(.*?)", r"\3")
pbl_literatury.loc[pbl_literatury['DZ_NAZWA'].str.contains('romsk') == True, 'nazwa'] = 'romsk|cygańsk'

dodane_literatury = pd.DataFrame({'DZ_DZIAL_ID':[32,32,32,32,59,86,107,149,67,69,34,34,34,34,34,55,32,99,34,148], 'DZ_NAZWA': ["Literatura brytyjska i irlandzka","Literatura brytyjska i irlandzka","Literatura brytyjska i irlandzka","Literatura brytyjska i irlandzka","Literatura grecka starożytna","Literatura łacińska średniowieczna","Literatura syryjska","Literatura esperanto","Literatura holenderska","Literatury Indii","Literatury Afryki Subsaharyjskiej","Literatury Afryki Subsaharyjskiej","Literatury Afryki Subsaharyjskiej","Literatury Afryki Subsaharyjskiej","Literatury Afryki Subsaharyjskiej", "Literatura egipsko-arabska", "Literatura brytyjska i irlandzka","Literatura palestyńsko-arabska","Literatury Afryki Subsaharyjskiej", "Literatura polska"], 'nazwa':["angielsk","szkock","irlandzk","walijsk","greck","łacińsk","syryjsk","esperanck","niderlandzk","indyjsk","południowoafryka","senegalsk","nigeryjsk","afrykańsk","ruandyjsk","egipsk. nowożytn","celtyck","palestyńsk","somalijsk","polsk"]})
pbl_literatury = pbl_literatury.append(dodane_literatury).drop_duplicates()

query = "select * from X655 a left join pbl_literatury b on a.`$a` like ('%'||b.nazwa||'%')"
X655 = pandasql.sqldf(query)
X655 = X655[X655['DZ_NAZWA'].notnull()][['id', 'DZ_NAZWA']]
X655 = X655[X655['DZ_NAZWA'].notnull()]

przedmiotowe = przedmiotowe[~przedmiotowe['id'].isin(X655['id'])]
X650 = marc_parser_1_field(przedmiotowe, 'id', '650', '\$')[['id', '$a']]
query = "select * from X650 a left join pbl_literatury b on a.`$a` like ('%'||b.nazwa||'%')"
X650 = pandasql.sqldf(query)
X650 = X650[X650['DZ_NAZWA'].notnull()][['id', 'DZ_NAZWA']]
X650 = X650[X650['DZ_NAZWA'].notnull()]

przedmiotowe = pd.concat([X600, X655, X650])

# do wymyślenia po statystyce
# =============================================================================
# przedmiotowe = przedmiotowe[~przedmiotowe['id'].isin(X650['id'])]
# query = """select z.za_zapis_id, dz.dz_dzial_id, dz.dz_nazwa, z.za_status_imp, z.za_uwagi
#         from pbl_zapisy z
#         join pbl_dzialy dz on dz.dz_dzial_id=z.za_dz_dzial1_id
#         where z.za_uwagi like '%import%'"""
# imports = pd.read_sql(query, con=connection).fillna(value = np.nan)
# 
# last_imports = ['1s22ClRxlrPHaAXi_n_JJH3mX6vKYzKjsKbpJpQztx_8', '1Vjeg0JsYI-8v9B-x_yyujIhmw7UR7Lk7poexrHNdVzM', '1Gc4gQSm9b4NDTQysiauzW9Jac6yP0oNuFbB8utO4kS4', '1HkWkX61sQWktSXf0v0uPV8j2DwuTocesyCJuKTdisIU', '1zeMx_Idsum8JmlM6G7Eufx9LxloHoAHv8V-My71VZf4', '19iL7YoD8ug-rLnpzS6FD46aS2J1BRf4qL5VxllywCGE', '1RshTeWdXBE7OzOEfoGpL9Ljb_GXDlGDePNjV1HKmuOo', '1RmDia97s4B8F74sS7Wbpnv_A9zMfr4xTvcD9leukAfM']
# imports_bn = pd.DataFrame()
# columns = ['pracownik', 'ZA_ZAPIS_ID', 'typ_ksiazki', 'link', 'link', 'rok', 'status', 'blad_w_imporcie_tytulu', 'X100', 'X245', 'X650', 'X655', 'X246', 'X250', 'X260', 'X300', 'X380', 'X490', 'X500', 'X501', 'X546', 'X600', 'X700', 'X041', 'X080']
# for elem in last_imports:
#     if elem != '1RmDia97s4B8F74sS7Wbpnv_A9zMfr4xTvcD9leukAfM':
#         df_year = gsheet_to_df(elem, 'lista_ksiazek')
#         df_year.columns = columns
#     else:
#         df_year = gsheet_to_df(elem, 'lista_książek')
#         df_year['typ_ksiazki'], df_year['link'], df_year['link_1'], df_year['status'], df_year['blad_w_imporcie_tytulu'] = [np.nan, np.nan, np.nan, np.nan, np.nan]
#         df_year = df_year[imports_bn.columns]
#         df_year.columns = columns
#     imports_bn = imports_bn.append(df_year)
#     
# imports_bn['X655'] = imports_bn['X655'].str.replace("(\$a)","|\1").str.replace("^\|", "")
# imports_bn['X650'] = imports_bn['X650'].str.replace("(\$a)","|\1").str.replace("^\|", "")
# imports_bn['X650'] = imports_bn['X650'].str.replace("(\$a)","\\❦7\1").str.replace("❦", "")
# imports_bn['X655'] = imports_bn['X655'].str.replace("(\$a)","\\❦7\1").str.replace("❦", "")
# 
# imports['ZA_ZAPIS_ID'] = imports['ZA_ZAPIS_ID'].astype(np.int64)
# imports_bn = imports_bn[imports_bn['ZA_ZAPIS_ID'] != '#N/A']
# imports_bn['ZA_ZAPIS_ID'] = imports_bn['ZA_ZAPIS_ID'].astype(np.int64)
# imports = pd.merge(imports_bn, imports, how='inner', on='ZA_ZAPIS_ID')
# imports = imports[imports['ZA_STATUS_IMP'].isin(['IOK'])][['ZA_ZAPIS_ID', 'DZ_NAZWA', 'X650', 'X655']]
# imports = imports.replace(r'^\s*$', np.nan, regex=True).fillna(value=np.nan)
# imports_655 = imports.copy()[['X655', 'DZ_NAZWA']]
# imports_655['DZ_NAZWA'] = imports_655['DZ_NAZWA'].str.replace(' - .$', '')
# imports_655 = imports_655[imports_655['X655'].notnull()]
# imports_655['X655'] = imports_655['X655'].str.replace(r'\7|', '',regex=False).str.replace('\$2DBN', '').str.replace("\$.|\|", " ").str.replace(" ", ".*").str.replace('(\.\*)+', '.*').str.replace('(\.)+', '.')
# imports_655['X655'] = imports_655['X655'].apply(lambda x: re.sub("[^\p{L}\d\*\.\-\s]", "", x)).apply(lambda x: re.sub('^', '.*', x)).str.replace('(\.\*)+', '.*')
# imports_655['test'] = imports_655.index+1
# imports_655 = imports_655.groupby(['X655', 'DZ_NAZWA']).count().reset_index(level=['X655', 'DZ_NAZWA']).groupby('X655').max().reset_index(level=['X655']).sort_values(['X655'])
# imports_655 = imports_655[imports_655['X655'].ne(imports_655['X655'].shift())]
# # ?
# to_repl = imports_655['X655'].values.tolist()
# vals = imports_655['DZ_NAZWA'].values.tolist()
# test = bn_articles.copy().head(1000)
# test['test'] = test['655'].replace(to_repl, vals, regex=True)
# test = test[['655', 'test']]
# =============================================================================

# podmiotowe
podmiotowe = bn_articles.copy()
podmiotowe = podmiotowe[podmiotowe['rodzaj'] == 'podmiotowy']  

X100 = marc_parser_1_field(podmiotowe, 'id', '100', '\$')[['id', '$a', '$c', '$d']].replace(r'^\s*$', np.NaN, regex=True)
X100['name'] = X100[['$a', '$d', '$c']].apply(lambda x: ' '.join(x.dropna().astype(str)), axis=1)
X100 = X100[['id', 'name']]
X100['name'] = X100['name'].str.replace("(\))(\.$)", r"\1").apply(lambda x: re.sub('(\p{Ll})(\.$)', r'\1', x))
X100 = pd.merge(X100, pbl_viaf, how='inner', left_on='name', right_on='BN_name')[['id', 'name', 'pbl_id']]
X100['pbl_id'] = X100['pbl_id'].astype(np.int64)
X100 = pd.merge(X100, tworca_i_dzial, how='left', left_on='pbl_id', right_on='TW_TWORCA_ID')[['id', 'DZ_NAZWA']]
X100 = X100[X100['DZ_NAZWA'].notnull()]   

podmiotowe = podmiotowe[~podmiotowe['id'].isin(X100['id'])]
X100_names = marc_parser_1_field(podmiotowe, 'id', '100', '\$')[['id', '$a', '$c', '$d']].replace(r'^\s*$', np.NaN, regex=True)[['id', '$a']]  
X100_names['name'] = X100_names['$a'].str.lower().str.replace("\\W", "") 
X100_names = pd.merge(X100_names, tworca_i_dzial, how='inner', on='name')[['id', 'DZ_NAZWA']]

podmiotowe = podmiotowe[~podmiotowe['id'].isin(X100_names['id'])]
bn_pbl_lista_literatur = gsheet_to_df('1zbwjnrtWGvbjrQTLMavJcWSu7HfP0I-USNgZ_KWiXjc', 'lista ukd bn')
bn_pbl_lista_literatur = bn_pbl_lista_literatur[(bn_pbl_lista_literatur['ukd_ogolne'].notnull()) &
                                                (bn_pbl_lista_literatur['pbl_id'] != '#N/A')].iloc[:,2:]
query = "select * from podmiotowe a join bn_pbl_lista_literatur b on a.'080' like ('%'||b.ukd_ogolne||'%')"
X080 = pandasql.sqldf(query)[['id', 'pbl_nazwa']]
X080['pbl_nazwa'] = X080['pbl_nazwa'].str.replace('(^.+?)(\|.+$)', r'\1').str.replace(' - .$', '')
X080.rename(columns={'pbl_nazwa':'DZ_NAZWA'}, inplace=True)

podmiotowe = podmiotowe[~podmiotowe['id'].isin(X080['id'])]
X655 = marc_parser_1_field(podmiotowe, 'id', '655', '\$')[['id', '$a']].replace(r'^\s*$', np.NaN, regex=True)
pbl_hasla_osobowe = gsheet_to_df('1zbwjnrtWGvbjrQTLMavJcWSu7HfP0I-USNgZ_KWiXjc', 'pbl_hasla_osobowe').iloc[:,[2,4]]
pbl_hasla_osobowe['DZ_NAZWA'] = pbl_hasla_osobowe['DZ_NAZWA'].str.replace(' - .$', '')
pbl_hasla_osobowe = pbl_hasla_osobowe.drop_duplicates()
query = "select * from X655 a left join pbl_hasla_osobowe b on a.`$a` like ('%'||b.nazwa||'%')"
X655 = pandasql.sqldf(query)[['id', 'DZ_NAZWA']].drop_duplicates().reset_index(drop=True)
X655 = X655[X655['DZ_NAZWA'].notnull()]

podmiotowe = podmiotowe[~podmiotowe['id'].isin(X655['id'])]
X650 = marc_parser_1_field(podmiotowe, 'id', '650', '\$')[['id', '$a']].replace(r'^\s*$', np.NaN, regex=True)
query = "select * from X650 a left join pbl_hasla_osobowe b on a.`$a` like ('%'||b.nazwa||'%')"
X650 = pandasql.sqldf(query)[['id', 'DZ_NAZWA']].drop_duplicates().reset_index(drop=True)
X650 = X650[X650['DZ_NAZWA'].notnull()]

podmiotowe = pd.concat([X100, X100_names, X080, X655, X650])
pbl_dzial_enrichment = pd.concat([przedmiotowe, podmiotowe])

bn_articles = pd.merge(bn_articles, pbl_dzial_enrichment, 'left', 'id')
bn_articles = bn_articles.drop_duplicates().reset_index(drop=True).dropna(how='all', axis=1)

gatunki_pbl = pd.DataFrame({'gatunek': ["aforyzm", "album", "antologia", "autobiografia", "dziennik", "esej", "felieton", "inne", "kazanie", "list", "miniatura prozą", "opowiadanie", "poemat", "powieść", "proza", "proza poetycka", "reportaż", "rozmyślanie religijne", "rysunek, obraz", "scenariusz", "szkic", "tekst biblijny", "tekst dramatyczny", "dramat", "wiersze", "wspomnienia", "wypowiedź", "pamiętniki", "poezja", "literatura podróżnicza", "satyra", "piosenka"]})
gatunki_pbl['gatunek'] = gatunki_pbl['gatunek'].apply(lambda x: f"$a{x}")

n = 10000
list_df = [bn_articles[i:i+n] for i in range(0, bn_articles.shape[0],n)]

pbl_enrichment_full = pd.DataFrame()
for i, group in enumerate(list_df):
    print(str(i) + '/' + str(len(list_df)))
    pbl_enrichment = group.copy()[['id', 'dziedzina_PBL', 'rodzaj', 'DZ_NAZWA', '650', '655']]
    pbl_enrichment = cSplit(pbl_enrichment, 'id', '655', '❦')
    pbl_enrichment['jest x'] = pbl_enrichment['655'].str.contains('\$x')
    pbl_enrichment['nowe650'] = pbl_enrichment.apply(lambda x: x['655'] if x['jest x'] == True else np.nan, axis=1)
    pbl_enrichment['655'] = pbl_enrichment.apply(lambda x: x['655'] if x['jest x'] == False else np.nan, axis=1)
    pbl_enrichment['650'] = pbl_enrichment[['650', 'nowe650']].apply(lambda x: '❦'.join(x.dropna().astype(str)), axis=1)
    pbl_enrichment = pbl_enrichment.drop(['jest x', 'nowe650'], axis=1)
    
    query = "select * from pbl_enrichment a join gatunki_pbl b on lower(a.'655') like ('%'||b.gatunek||'%') where a.rodzaj like 'podmiotowy'"
    gatunki1 = pandasql.sqldf(query)
    query = "select * from pbl_enrichment a join gatunki_pbl b on lower(a.'650') like ('%'||b.gatunek||'%') where a.rodzaj like 'podmiotowy'"
    gatunki2 = pandasql.sqldf(query)
    gatunki3 = pbl_enrichment[(pbl_enrichment['655'].str.contains('Artykuł') == True) & (pbl_enrichment['rodzaj'] == 'przedmiotowy')]
    gatunki4 = pbl_enrichment[(pbl_enrichment['650'].str.contains('Artykuł') == True) & (pbl_enrichment['rodzaj'] == 'przedmiotowy')]
    gatunki = pd.concat([gatunki1, gatunki2, gatunki3, gatunki4]).drop_duplicates()
    gatunki['gatunek'] = gatunki['gatunek'].apply(lambda x: ''.join([x[:2], x[2].upper(), x[2 + 1:]]).strip() if pd.notnull(x) else '$aArtykuł')
    try:
        X655_field = marc_parser_1_field(gatunki, 'id', '655', '\$')[['id', '$y']].drop_duplicates()
        X655_field = X655_field[X655_field['$y'] != '']   
        gatunki = pd.merge(gatunki, X655_field, how='left', on='id')
    except (IndexError, KeyError):
        pass
    gatunki['gatunek'] = gatunki['gatunek'].apply(lambda x: f"\\7{x.strip()}")
    try:
        gatunki['gatunek+data'] = gatunki.apply(lambda x: f"{x['gatunek']}$y{x['$y']}" if pd.notnull(x['$y']) else np.nan, axis=1)
        gatunki['nowe655'] = gatunki[['655', 'gatunek', 'gatunek+data']].apply(lambda x: '❦'.join(x.dropna().astype(str)), axis=1)
    except (ValueError, KeyError):
        gatunki['nowe655'] = gatunki[['655', 'gatunek']].apply(lambda x: '❦'.join(x.dropna().astype(str)), axis=1)
    gatunki['nowe655'] = gatunki.groupby('id')['nowe655'].transform(lambda x: '❦'.join(x))
    gatunki = gatunki[['id', 'nowe655']].drop_duplicates()
    if len(gatunki) > 0:
        gatunki['nowe655'] = gatunki['nowe655'].str.split('❦').apply(set).str.join('❦')
    
    pbl_enrichment = pd.merge(pbl_enrichment, gatunki, how ='left', on='id')
    pbl_enrichment['nowe650'] = pbl_enrichment.apply(lambda x: x['655'] if pd.isnull(x['nowe655']) else np.nan, axis=1)
    pbl_enrichment['DZ_NAZWA'] = pbl_enrichment['DZ_NAZWA'].apply(lambda x: f"\\7$a{x}" if pd.notnull(x) else np.nan)
    pbl_enrichment['650'] = pbl_enrichment['650'].replace(r'^\s*$', np.nan, regex=True)
    pbl_enrichment['650'] = pbl_enrichment[['650', 'nowe650', 'DZ_NAZWA']].apply(lambda x: '❦'.join(x.dropna().astype(str)), axis=1)
    pbl_enrichment['655'] = pbl_enrichment['nowe655'].replace(np.nan, '', regex=True)
    pbl_enrichment['655'] = pbl_enrichment.apply(lambda x: f"{x['655']}❦\\7$aOpracowanie" if x['rodzaj'] == 'przedmiotowy' else f"{x['655']}❦\\7$aDzieło literackie", axis=1)
    pbl_enrichment = pbl_enrichment[['id', '650', '655']].replace(r'^\❦', '', regex=True)
    pbl_enrichment['650'] = pbl_enrichment.groupby('id')['650'].transform(lambda x: '❦'.join(x.dropna().astype(str)))
    pbl_enrichment['655'] = pbl_enrichment.groupby('id')['655'].transform(lambda x: '❦'.join(x.dropna().astype(str)))
    pbl_enrichment = pbl_enrichment.drop_duplicates().reset_index(drop=True)
    pbl_enrichment['650'] = pbl_enrichment['650'].str.split('❦').apply(set).str.join('❦')
    pbl_enrichment['655'] = pbl_enrichment['655'].str.split('❦').apply(set).str.join('❦')
    pbl_enrichment_full = pbl_enrichment_full.append(pbl_enrichment)

regex_match = re.compile('^\d{3}$')
idxs = [i for i, item in enumerate(bn_articles.columns.tolist()) if re.search(regex_match, item)][-1]+2

bn_articles_marc = bn_articles.iloc[:,:idxs].drop_duplicates()
bn_articles_marc = bn_articles_marc.set_index('id', drop=False)
pbl_enrichment_full = pbl_enrichment_full.set_index('id')
bn_articles_marc = pbl_enrichment_full.combine_first(bn_articles_marc)

merge_500s = [col for col in bn_articles_marc.columns if re.findall('^5', col) and col != '505']
bn_articles_marc['500'] = bn_articles_marc[merge_500s].apply(lambda x: '❦'.join(x.dropna().astype(str)), axis = 1)
merge_500s = [x for x in merge_500s if x != '500']
bn_articles_marc = bn_articles_marc.loc[:, ~bn_articles_marc.columns.isin(merge_500s)].drop(columns='264')
bn_articles_marc.rename(columns={'260':'264'}, inplace=True)

# =============================================================================
# bn_articles_marc = pd.read_excel('bn_articles_to_link.xlsx')
# full_text = marc_parser_1_field(bn_articles_marc, '001', '856', '\$')
# full_text['sygnatura'] = full_text['$u'].str.replace('http://polona.pl/item/', '', regex=False).str.strip()
# for i, row in full_text.iterrows():
#     print(str(i) + '/' + str(len(full_text)))
#     api_url = f"https://polona.pl/api/entities/?format=json&from=0&highlight=1&public=1&query={row['sygnatura']}"
#     try:
#         json_data = requests.get(api_url)
#         json_data = json.loads(json_data.text)
#         try:
#             rights = ''.join([''.join([elem for elem in hit['rights']]) for hit in json_data['hits']])
#         except:
#             rights = 'brak praw'
#     except JSONDecodeError:
#         rights = 'brak praw'
#     full_text.at[i, 'rights'] = rights
# =============================================================================

bn_articles_marc.drop(['852', 'id', '856'], axis = 1, inplace=True) 
bn_articles_marc['240'] = bn_articles_marc['246'].apply(lambda x: x if pd.notnull(x) and 'Tyt. oryg.:' in x else np.nan)
bn_articles_marc['246'] = bn_articles_marc['246'].apply(lambda x: x if pd.notnull(x) and 'Tyt. oryg.:' not in x else np.nan)
bn_articles_marc['008'] = bn_articles_marc['008'].str.replace('\\', ' ')
if bn_articles_marc['009'].dtype == np.float64:
        bn_articles_marc['009'] = bn_articles_marc['009'].astype(np.int64)
bn_articles_marc['995'] = '\\\\$aPBL 2004-2020: czasopisma'
bn_articles_marc = bn_articles_marc.drop_duplicates().reset_index(drop=True).dropna(how='all', axis=1)

# linki do pełnych tekstów i relacje

bazhum_links = gsheet_to_df('1KUoEQTYCrspK1t2gFvGOQPgSodv8ikzZt8f-oNzZe5g', 'Main')
bazhum_links['is_link'] = bazhum_links['full_text'].apply(lambda x: urlparse(x)[0])
bazhum_links = bazhum_links[bazhum_links['is_link'] == 'http'].drop(columns='is_link').rename(columns={'full_text':'856'})
bazhum_links['856'] = bazhum_links['856'].apply(lambda x: f"40$u{x}$yonline$4N")

bn_art_full_text = gsheet_to_df('1ewU50T08ZVNUP-U3dTTrb5VDDfWqT4TeYrMLrYns7c8', 'Sheet1')
bn_art_full_text = bn_art_full_text[(bn_art_full_text['rights'].notnull()) & (bn_art_full_text['rights'] != 'brak praw')][['001', '856']]

bn_relations = gsheet_to_df('1WPhir3CwlYre7pw4e76rEnJq5DPvVZs3_828c_Mqh9c', 'relacje_rev_book').drop(columns='typ').rename(columns={'id':'001'})

X856 = pd.concat([bn_art_full_text, bn_relations, bazhum_links])
X856['856'] = X856.groupby('001')['856'].transform(lambda x: '❦'.join(x))
X856 = X856[X856['856'].notnull()].drop_duplicates()

bn_articles_marc = pd.merge(bn_articles_marc, X856, on='001', how='left')

field_list = bn_articles_marc.columns.tolist()
field_list.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
bn_articles_marc = bn_articles_marc.reindex(columns=field_list)
bn_articles_marc = bn_articles_marc.reset_index(drop=True)

#%% Nikodem - dzielenie wierszy

multiple_poems = bn_articles_marc[(bn_articles_marc['245'].str.contains(' ;$b', regex=False)) &
                                  (bn_articles_marc['245'].notnull()) &
                                  (bn_articles_marc['655'].str.lower().str.contains('poezja', regex=False)) &
                                  (bn_articles_marc['505'].isnull())]

bn_articles_marc = bn_articles_marc[~bn_articles_marc['001'].isin(multiple_poems['001'])]

field_245 = marc_parser_1_field(multiple_poems, '001', '245', '\$')
field_245['titles'] = [' '.join((a, b)).replace(' /', '') for a, b in zip(field_245['$a'], field_245['$b'])]
field_245['titles'] = field_245['titles'].str.split(' ; ')
field_245 = field_245.explode("titles").reset_index(drop=True)
field_245['new_245'] = '10$a' + field_245['titles'] + ' /$c' + field_245['$c']
field_245 = field_245[['001', 'new_245']].rename(columns={'new_245':'245'})

multiple_poems = pd.merge(field_245, multiple_poems.drop(columns='245'), on='001')

def remove_wrong_700(x):
    try:
        val = x['700'].split('❦')
        result = ''
        for elem in val:
            if '$t' not in x['700']:
                result += elem  
        if result == '':
            result = np.nan
    except AttributeError: 
        result = np.nan
    return result

multiple_poems['700'] = multiple_poems[['245', '700']].apply(lambda x: remove_wrong_700(x), axis=1)
multiple_poems['idx'] = multiple_poems.groupby('001').cumcount()+1
multiple_poems['001'] = multiple_poems[['idx', '001']].apply(lambda x: x['001'][0] + '{:02d}'.format(x['idx']) + x['001'][3:], axis=1)
multiple_poems = multiple_poems.drop(columns='idx')

bn_articles_marc = pd.concat([bn_articles_marc, multiple_poems]).reset_index(drop=True).sort_values('001')

identifiers_freq = bn_articles_marc['001'].value_counts()

bn_articles_marc.to_excel('bn_articles_marc.xlsx', index=False)

now = datetime.datetime.now()
year = now.year
month = now.month
day = now.day

df_to_mrc(bn_articles_marc, '❦', f'libri_marc_bn_articles_{year}-{month}-{day}.mrc', f'libri_bn_articles_errors_{year}-{month}-{day}.txt')
mrc_to_mrk(f'libri_marc_bn_articles_{year}-{month}-{day}.mrc', f'libri_marc_bn_articles_{year}-{month}-{day}.mrk')

#errors
errors_txt = io.open(f'libri_bn_articles_errors_{year}-{month}-{day}.txt', 'rt', encoding='UTF-8').read().splitlines()
errors_txt = [e for e in errors_txt if e]

new_list = []

for sublist in errors_txt:
    sublist = ast.literal_eval(sublist)
    di = {x:y for x,y in sublist}
    new_list.append(di)
    
for dictionary in new_list:
    for key in dictionary.keys():
        dictionary[key] = '❦'.join([e for e in dictionary[key] if e])

df = pd.DataFrame(new_list)
df['LDR'] = '-----nab---------4u-----'
#investigate the file thoroughly if more errors

df_to_mrc(df, '❦', f'libri_marc_bn_articles_vol_2_{year}-{month}-{day}.mrc', f'libri_marc_bn_articles_vol_2_{year}-{month}-{day}.txt')
mrc_to_mrk(f'libri_marc_bn_articles_vol_2_{year}-{month}-{day}.mrc', f'libri_marc_bn_articles_vol_2_{year}-{month}-{day}.mrk')































