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
import difflib
import spacy
from collections import Counter

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

#%% klastrowanie - stemming
file_list = drive.ListFile({'q': "'103h2kWIAKDBG2D6KydU4NjTyldBfxBAK' in parents and trashed=false"}).GetList() 
#[print(e['title'], e['id']) for e in file_list]
nstl_folder = [file['id'] for file in file_list if file['title'] == 'STL'][0]
file_list = drive.ListFile({'q': f"'{nstl_folder}' in parents and trashed=false"}).GetList() 
#[print(e['title'], e['id']) for e in file_list]
nstl_sheet = [file['id'] for file in file_list if file['title'] == 'nstl_key_words_2021-2-24'][0]
key_words_sheet = gp.Spread(nstl_sheet, creds=credentials)
key_words_sheet.sheets
df_clusters_stemming = key_words_sheet.sheet_to_df(sheet='klucze klastrów', index=0)
df_key_words_stats = key_words_sheet.sheet_to_df(sheet='słowa kluczowe - statystyki', index=0)
df_key_words_for_article = key_words_sheet.sheet_to_df(sheet='słowa kluczowe dla artykułów', index=0)
 
clusters_stemming = [e.strip().lower() for e in df_clusters_stemming['hasła'].to_list()]
key_words_list = [e.split(' ') for e in df_key_words_stats['słowo kluczowe'].to_list()]
clusters = {}
for word in clusters_stemming:
    key_elements = word.split('|')
    for key_element in key_elements:
        for one_keyword_list in key_words_list:
            for k_word in one_keyword_list:
                if re.search(f"^{key_element}", k_word.lower()):
                    if word in clusters:
                        clusters[word].append(' '.join(one_keyword_list))
                    else:
                        clusters[word] = [' '.join(one_keyword_list)]

missing_keys = list(set(clusters_stemming) - set(clusters.keys()))
key_words_list = df_key_words_stats['słowo kluczowe'].to_list()
for word in missing_keys:
    for one_keyword in key_words_list:
        if re.search(word, one_keyword.lower()):
            if word in clusters:
                clusters[word].append(one_keyword)
            else:
                clusters[word] = [one_keyword]       
                                                                       
clusters_stemming - clusters.keys()

clusters = dict(sorted(clusters.items(), key = lambda item : len(item[1]), reverse=True))

with open("nstl_clusters.json", 'w', encoding='utf-8') as f: 
    json.dump(clusters, f, ensure_ascii=False, indent=4)
    
clusters_df = pd.DataFrame.from_dict(clusters, orient='index').stack().reset_index(level=0).rename(columns={'level_0':'cluster', 0:'słowo kluczowe'}).reset_index(drop=True)

df_key_words_stats = df_key_words_stats.merge(clusters_df, how='left', on='słowo kluczowe')

cluster_frequency = {}
for cluster in clusters:
    frequence = df_key_words_stats[df_key_words_stats['cluster'] == cluster]['frekwencja'].astype(int).sum()
    cluster_frequency[cluster] = frequence
    
cluster_frequency_df = pd.DataFrame.from_dict(cluster_frequency, orient='index').stack().reset_index(level=0).rename(columns={'level_0':'cluster', 0:'frekwencja w słowach kluczowych'}).reset_index(drop=True).sort_values('frekwencja w słowach kluczowych', ascending=False)

for i, row in df_key_words_for_article.iterrows():
    print(f"{i+1}/{df_key_words_for_article.shape[0]}")
    list_of_keys = []
    for k, v in clusters.items():
        if any(word in row['subject❦pl'].lower().strip().split('❦') for word in v):
            list_of_keys.append(k)
    df_key_words_for_article.at[i,'clusters'] = list_of_keys
    
df_key_words_for_article['clusters'][0] = [df_key_words_for_article['clusters'][0]]
    
key_words_for_article = [e for sublist in df_key_words_for_article['clusters'] for e in sublist if e]
key_words_for_article = pd.DataFrame.from_dict(Counter(key_words_for_article), orient='index').reset_index().rename(columns={'index':'cluster', 0:'frekwencja w artykułach'}).reset_index(drop=True).sort_values('frekwencja w artykułach', ascending=False)

cluster_frequency_df = cluster_frequency_df.merge(key_words_for_article, how='left', on='cluster')

for k, v in clusters.items():
    v = '❦'.join(v)
    clusters[k] = v
clusters_grouped_df = pd.DataFrame.from_dict(clusters, orient='index').reset_index().rename(columns={'index':'cluster', 0:'zawartość'})

for i, row in clusters_grouped_df.iterrows():
    x = row['zawartość'].split('❦')
    clusters_grouped_df.at[i, 'zawartość'] = x
    
cluster_frequency_df = cluster_frequency_df.merge(clusters_grouped_df, how='left', on='cluster')

key_words_sheet.df_to_sheet(cluster_frequency_df, sheet='frekwencja klastrów', index=0)
    
#liczba artykułów suma + bez słów kluczowych + słowa kluczowe
articles_oai_pmh = key_words_sheet.sheet_to_df(sheet='oai-pmh', index=0)
articles_oai_pmh = articles_oai_pmh[articles_oai_pmh['identifier'] != ''][['tytuł czasopisma', 'subject❦pl']]
czasopisma = articles_oai_pmh['tytuł czasopisma'].drop_duplicates().to_list()
liczba_artykulow = articles_oai_pmh['tytuł czasopisma'].value_counts().reset_index().rename(columns={'index':'tytuł czasopisma', 'tytuł czasopisma':'liczba artykulow'})
liczba_artykulow_bez_slow_kluczowych = articles_oai_pmh[articles_oai_pmh['subject❦pl'] == ''].groupby(['tytuł czasopisma']).size().reset_index(name='liczba artykulow bez slow kluczowych')
liczba_artykulow = liczba_artykulow.merge(liczba_artykulow_bez_slow_kluczowych, on='tytuł czasopisma', how='left').reset_index(drop=True)
liczba_artykulow['liczba artykulow bez slow kluczowych'] = liczba_artykulow['liczba artykulow bez slow kluczowych'].apply(lambda x: 0 if pd.isnull(x) else x)
liczba_artykulow['liczba artykulow ze slowami kluczowymi'] = liczba_artykulow['liczba artykulow'] - liczba_artykulow['liczba artykulow bez slow kluczowych']

key_words_sheet.df_to_sheet(liczba_artykulow, sheet='czasopisma - statystyka słów kluczowych', index=0)

#inne dane
articles_oai_pmh = key_words_sheet.sheet_to_df(sheet='oai-pmh', index=0)

years_df = articles_oai_pmh[['tytuł czasopisma', 'source❦pl', 'date❦pl']]

def pick_year(x):
    try:
        if x['date❦pl'] != '':
            year = re.findall('\d{4}', x['date❦pl'])[0]
        else:
            year = re.findall('\d{4}', x['source❦pl'])[0]
    except IndexError:
        year = "no year"
    return year
years_df['year'] = years_df.apply(lambda x: pick_year(x), axis=1)
years = years_df[years_df['year'] != 'no year']['year'].drop_duplicates().to_list()
years = sorted(years)

test = years_df[years_df['year'] == '2020']



  

#%% word embedding
texts = open("word_embedding_test.txt", encoding='utf8').read().splitlines()

from sklearn.feature_extraction.text import CountVectorizer
from sklearn.preprocessing import Normalizer, FunctionTransformer
from sklearn.cluster import AgglomerativeClustering
from sklearn.pipeline import make_pipeline
model = make_pipeline(
    CountVectorizer(), 
    Normalizer(), 
    FunctionTransformer(lambda x: x.todense(), accept_sparse=True),
    AgglomerativeClustering(distance_threshold=1.0, n_clusters=None),
)
clusters = model.fit_predict(texts)
print(clusters)  # [0 0 0 1 1]

import spacy
import numpy as np
nlp = spacy.load('pl_core_news_lg')

model = make_pipeline(
    FunctionTransformer(lambda x: np.stack([nlp(t).vector for t in x])),
    Normalizer(), 
    AgglomerativeClustering(distance_threshold=0.5, n_clusters=None),
)
clusters = model.fit_predict(texts)
print(clusters)  # [2 0 2 0 1]

from laserembeddings import Laser
laser = Laser()

model = make_pipeline(
    FunctionTransformer(lambda x: laser.embed_sentences(x, lang='en')),
    Normalizer(), 
    AgglomerativeClustering(distance_threshold=0.8, n_clusters=None),
)
clusters = model.fit_predict(texts)
print(clusters)  # [1 1 1 0 0]


#results for each model
from collections import defaultdict
cluster2words = defaultdict(list)
for text, cluster in zip(texts, clusters):
    for word in text.split():
        if word not in cluster2words[cluster]:
            cluster2words[cluster].append(word)
            
            
test = [wordlist for wordlist in cluster2words.values()]
result = [' '.join(wordlist) for wordlist in cluster2words.values()]
print(result)  # ['yellow color looks like bright', 'red color okay blood']


#_____________________________________________________________

import numpy as np
from sklearn.cluster import AffinityPropagation
import distance
    
words = open("word_embedding_test.txt", encoding='utf8').read().splitlines()
words = np.asarray(words) #So that indexing with a list will work
lev_similarity = -1*np.array([[distance.levenshtein(w1,w2) for w1 in words] for w2 in words])

affprop = AffinityPropagation(affinity="precomputed", damping=0.5)
affprop.fit(lev_similarity)
slownik = {}
for cluster_id in np.unique(affprop.labels_):
    exemplar = words[affprop.cluster_centers_indices_[cluster_id]]
    cluster = np.unique(words[np.nonzero(affprop.labels_==cluster_id)])
    cluster_str = ", ".join(cluster)
    slownik[exemplar] = cluster_str
    print(" - *%s:* %s" % (exemplar, cluster_str))


# =============================================================================
# from string_grouper import group_similar_strings
# test = group_similar_strings(texts)
# =============================================================================

from gensim.models import Word2Vec
from sklearn.manifold import TSNE
import matplotlib.pyplot as plt
sentences = open("word_embedding_test.txt", encoding='utf8').read().splitlines()
sentences = [s.split(' ') for s in sentences]
sentences = [[e.strip() for e in s] for s in sentences]
model = Word2Vec(sentences, min_count=1)
print (model.most_similar(positive=['intertekstualność'], negative=[], topn=2))
print(model[model.wv.vocab])
print(list(model.wv.vocab))

vocab = list(model.wv.vocab)
X = model[vocab]
tsne = TSNE(n_components=2)
X_tsne = tsne.fit_transform(X)
df = pd.DataFrame(X_tsne, index=vocab, columns=['x', 'y'])

fig = plt.figure()
ax = fig.add_subplot(1, 1, 1)

ax.scatter(df['x'], df['y'])

for word, pos in df.iterrows():
    ax.annotate(word, pos)

fig.savefig('test.jpg')








# =============================================================================
# OAI-PMH - 3 serwisy: pressto, journals.us, ejournals
# wydobyć art + metadane
# słowa kluczowe - lematyzacje, synonimy + frekwencja
# modelowanie tematyczne dla abstraktów
# =============================================================================
