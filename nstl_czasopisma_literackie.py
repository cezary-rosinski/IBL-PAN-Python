import pandas as pd
import io
import requests
from sickle import Sickle
import xml.etree.ElementTree as et
import lxml.etree
import pdfplumber
from google_drive_research_folders import PBL_folder
import gspread_pandas as gp
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from google_drive_credentials import gc, credentials
import json
from my_functions import cSplit, cluster_strings
import datetime
import regex as re
from collections import OrderedDict

now = datetime.datetime.now()
year = now.year
month = now.month
day = now.day

gauth = GoogleAuth()
gauth.LocalWebserverAuth()

drive = GoogleDrive(gauth)


#%% lista czasopism:
# =============================================================================
# "Teksty Drugie"
# "Pamiętnik Literacki"
# "Napis"
# "XIX wiek"
# "Prace Filologiczne. Literaturoznawstwo"
# "Dzieciństwo. Kultura i Literatura"
# "Colloquia Litteraria"
# "Jednak Książki"
# "Przestrzenie Teorii"
# "Poznańskie SP. S. Literaturoznawcza"
# "Porównania"
# "Forum Poetyki"
# "Prace Literaturoznawcze"
# "Stylistyka"
# "Fabrica Litterarum Polono-Italica"
# "Narracje o Zagładzie"
# "Paidia i Literatura"
# "Postscriptum Polonistyczne"
# "Śląskie Studia Polonistyczne"
# "Wortfolge. Szyk słów"
# "Zoophilologica"
# "Białostockie Studia Literaturoznawcze"
# Acta Universitatis Lodziensis. Folia Linguistica
# Acta Universitatis Lodziensis. Folia Linguistica Polonica
# Collectanea Philologica
# "Czytanie Literatury"
# "Zagadnienia Rodzajów Literackich"
# "Litteraria Copernicana"
# "Góry, Literatura, Kultura"
# "Prace Literackie"
# "Literatura i Kultura Popularna"
# =============================================================================

file_list = drive.ListFile({'q': "'103h2kWIAKDBG2D6KydU4NjTyldBfxBAK' in parents and trashed=false"}).GetList() 
#[print(e['title'], e['id']) for e in file_list]
nstl_folder = [file['id'] for file in file_list if file['title'] == 'STL'][0]
file_list = drive.ListFile({'q': f"'{nstl_folder}' in parents and trashed=false"}).GetList() 
#[print(e['title'], e['id']) for e in file_list]
nstl_sheet = [file['id'] for file in file_list if file['title'] == 'biblioteki cyfrowe i repozytoria PL'][0]
s_journals = gp.Spread(nstl_sheet, creds=credentials)
s_journals.sheets
journals_df = s_journals.sheet_to_df(sheet='Czasopisma', index=0)
journals_df = journals_df[journals_df['OAI-PMH'] != ''].reset_index(drop=True)

list_of_dicts = []
for i, row in journals_df.iterrows():
    print(f"{i+1}/{len(journals_df)}")
    oai_url = row['OAI-PMH']
    tytul_czasopisma = row['Czasopisma'].replace('"', '')
    sickle = Sickle(oai_url)
    if tytul_czasopisma == 'Czytanie Literatury':
        records = sickle.ListRecords(metadataPrefix='oai_dc', set='com_11089_5783')
    else:
        records = sickle.ListRecords(metadataPrefix='oai_dc')

# tree_pretty = lxml.etree.parse('test.xml')
# pretty = lxml.etree.tostring(tree_pretty, encoding="unicode", pretty_print=True)
    journal_list_of_dicts = []
    for record in records:
        with open('test.xml', 'wt', encoding='UTF-8') as file:
            file.write(str(record))
        tree = et.parse('test.xml')
        root = tree.getroot()
        article = root.findall('.//{http://www.openarchives.org/OAI/2.0/}metadata/{http://www.openarchives.org/OAI/2.0/oai_dc/}dc/')
        article_dict = {}
        for node in article:
            try:
                if f"{node.tag.split('}')[-1]}❦{[v[:2] for k,v in node.attrib.items()][0]}" in article_dict:
                    try:
                        article_dict[f"{node.tag.split('}')[-1]}❦{[v[:2] for k,v in node.attrib.items()][0]}"] += f"❦{node.text}"
                    except TypeError:
                        pass
                else:
                    article_dict[f"{node.tag.split('}')[-1]}❦{[v[:2] for k,v in node.attrib.items()][0]}"] = node.text
            except IndexError:
                if node.tag.split('}')[-1] in article_dict:
                    try:
                        article_dict[node.tag.split('}')[-1]] += f"❦{node.text}"
                    except TypeError:
                        pass
                else:
                    article_dict[node.tag.split('}')[-1]] = node.text  
        article_dict['tytuł czasopisma'] = tytul_czasopisma            
        journal_list_of_dicts.append(article_dict)
        list_of_dicts.append(article_dict)
        journal_list_of_dicts = [e for e in journal_list_of_dicts if e]
        
        with open(f"{tytul_czasopisma}_{year}-{month}-{day}.json", 'w', encoding='utf-8') as f: 
            json.dump(journal_list_of_dicts, f, ensure_ascii=False, indent=4)

sickle = Sickle('https://rcin.org.pl/ibl/dlibra/oai-pmh-repository.xml?')
records = sickle.ListRecords(metadataPrefix='oai_dc')
journal_list_of_dicts = []
for record in records:
    with open('test.xml', 'wt', encoding='UTF-8') as file:
        file.write(str(record))
    tree = et.parse('test.xml')
    root = tree.getroot()
    if re.findall('Napis|Wiek XIX|Pamiętnik Literacki|Teksty Drugie', root.findall('.//{http://www.openarchives.org/OAI/2.0/}metadata/{http://www.openarchives.org/OAI/2.0/oai_dc/}dc/{http://purl.org/dc/elements/1.1/}title')[-1].text):
        article = root.findall('.//{http://www.openarchives.org/OAI/2.0/}metadata/{http://www.openarchives.org/OAI/2.0/oai_dc/}dc/')
        article_dict = {}
        for node in article:
            try:
                if f"{node.tag.split('}')[-1]}❦{[v for k,v in node.attrib.items()][0]}" in article_dict:
                    try:
                        article_dict[f"{node.tag.split('}')[-1]}❦{[v for k,v in node.attrib.items()][0]}"] += f"❦{node.text}"
                    except TypeError:
                        pass
                else:
                    article_dict[f"{node.tag.split('}')[-1]}❦{[v for k,v in node.attrib.items()][0]}"] = node.text
            except IndexError:
                if node.tag.split('}')[-1] in article_dict:
                    try:
                        article_dict[node.tag.split('}')[-1]] += f"❦{node.text}"
                    except TypeError:
                        pass
                else:
                    article_dict[node.tag.split('}')[-1]] = node.text  
        article_dict['tytuł czasopisma'] = root.findall('.//{http://www.openarchives.org/OAI/2.0/}metadata/{http://www.openarchives.org/OAI/2.0/oai_dc/}dc/{http://purl.org/dc/elements/1.1/}title')[-1].text            
        journal_list_of_dicts.append(article_dict)
        list_of_dicts.append(article_dict)
        journal_list_of_dicts = [e for e in journal_list_of_dicts if e]
        
        with open(f"rcin_{year}-{month}-{day}.json", 'w', encoding='utf-8') as f: 
            json.dump(journal_list_of_dicts, f, ensure_ascii=False, indent=4)

#jak zdefiniować tytuł jako:  Napis|Wiek XIX|Pamiętnik Literacki|Teksty Drugie?

list_of_dicts = [e for e in list_of_dicts if e]

key_word_sheet = gc.create(f'nstl_key_words_{year}-{month}-{day}', nstl_folder)
s_key_words = gp.Spread(key_word_sheet.id, creds=credentials)
   
df = pd.DataFrame(list_of_dicts)
print(f'liczba artykułów = {len(df)}')
#liczba artykułów = 18175

wsh = key_word_sheet.get_worksheet(0)
wsh.update_title('oai-pmh')
s_key_words.df_to_sheet(df, sheet='oai-pmh', index=0)
wsh.freeze(rows=1)    
wsh.set_basic_filter()

df_articles_with_key_words = df.copy()[df['subject❦pl'].notnull()][['creator', 'title❦pl', 'subject❦pl', 'tytuł czasopisma']].reset_index(drop=True)
s_key_words.df_to_sheet(df_articles_with_key_words, sheet='słowa kluczowe dla artykułów', index=0)
worksheet = key_word_sheet.worksheet('słowa kluczowe dla artykułów')
worksheet.freeze(rows=1)    
worksheet.set_basic_filter()

print(f'artykuły ze słowami kluczowymi = {len(df_articles_with_key_words)}')
#artykuły ze słowami kluczowymi = 8270
df_articles_with_key_words['indeks'] = df_articles_with_key_words.index+1
df_articles_with_key_words = cSplit(df_articles_with_key_words, 'indeks', 'subject❦pl', '❦')


df_key_words = df_articles_with_key_words['subject❦pl'].str.lower().value_counts().reset_index().rename(columns={'index':'słowo kluczowe', 'subject❦pl':'frekwencja'})
s_key_words.df_to_sheet(df_key_words, sheet='słowa kluczowe - statystyki', index=0)
worksheet = key_word_sheet.worksheet('słowa kluczowe - statystyki')
#worksheet.clear()
#key_word_sheet.del_worksheet(worksheet)
worksheet.freeze(rows=1)    
worksheet.set_basic_filter()

similarity_levels = [0.9, 0.8, 0.7, 0.6, 0.5]
for level in similarity_levels:
    key_words_cluster = cluster_strings(df_key_words['słowo kluczowe'], level)
    print(f"{level}: {len(key_words_cluster)} klastrów")
    sorted_items = sorted(key_words_cluster.items(), key = lambda item : len(item[1]), reverse=True)
    newd = dict(sorted_items)
    with open(f"key_words_clusters_similarity_level_{level}.json", 'w', encoding='utf-8') as f: 
            json.dump(newd, f, ensure_ascii=False, indent=4)


#1. dorzucić "najważniejsze" do puli
#2. wyrzucić nazwy własne ze słów kluczowych? lepiej nie - bo bachtiny, derridy
#clustrować podobieństwo




# =============================================================================
# sickle = Sickle('https://apcz.umk.pl/czasopisma/index.php/LC/oai')
# records = sickle.ListRecords(metadataPrefix='oai_dc')
# 
# with open('test.xml', 'wt', encoding='UTF-8') as file:
#     for record in records:
#         file.write(str(record))
# =============================================================================






















# =============================================================================
# OAI-PMH - 3 serwisy: pressto, journals.us, ejournals
# wydobyć art + metadane
# słowa kluczowe - lematyzacje, synonimy + frekwencja
# modelowanie tematyczne dla abstraktów
# =============================================================================
