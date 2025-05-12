import requests
from tqdm import tqdm
import numpy as np
from my_functions import gsheet_to_df, simplify_string, cluster_strings, marc_parser_to_dict
from concurrent.futures import ThreadPoolExecutor
import json
import glob
import random
import time
from datetime import datetime
import Levenshtein as lev
import io
import pandas as pd
from bs4 import BeautifulSoup
import pickle

#%% webscraping list of books

urls = {'https://www.perlentaucher.de/buchKSL/deutsche-romane.html': ['Deutsche Romane', 512],
        'https://www.perlentaucher.de/buchKSL/deutsche-literatur-20-jahrhundert-romane.html': ['Deutshe Lit. XX w. powieści', 162],
        'https://www.perlentaucher.de/buchKSL/deutsche-literatur-20-jahrhundert-erinnerungen.html': ['XX w. wspomnienia', 95],
        'https://www.perlentaucher.de/buchKSL/deutsche-literatur-20-jahrhundert-reisereportagen.html': ['XX w. reportaże podróżnicze', 7],
        'https://www.perlentaucher.de/buchKSL/stichwort-mauerfall.html': ['Stichwort Mauerfall', 22]}

urls = {'https://www.perlentaucher.de/buchKSL/deutsche-literatur-20-jahrhundert-romane.html': ['Deutshe Lit. XX w. powieści', 162]}

books_links = []

for url, l in tqdm(urls.items()):
    # url = list(urls.keys())[0]
    # l = urls.get(url)
    no_of_pages = range(1, l[-1]+1)
    
    html_text = requests.get(url).text
    soup = BeautifulSoup(html_text, "html.parser")
    page_books = set(['https://www.perlentaucher.de' + e['href'] for e in soup.find('div', {'class': 'box content-matter'}).find_all('a')])
    books_links.extend(page_books)
    
    for n in tqdm(no_of_pages):
        # n = list(no_of_pages)[0]
        link = f"{url}?q=&p={n}"
        html_text = requests.get(link).text
        soup = BeautifulSoup(html_text, "html.parser")
        page_books = set(['https://www.perlentaucher.de' + e['href'] for e in soup.find('div', {'class': 'box content-matter'}).find_all('a')])
        books_links.extend(page_books)
        
unique_books = set(books_links)

with open('data/gortych_perlentaucher_books.p', 'wb') as fp:
    pickle.dump(unique_books, fp, protocol=pickle.HIGHEST_PROTOCOL)

#%% webscraping books content #8889 unique books

with open('data/gortych_perlentaucher_books.p', 'rb') as fp:
    unique_books = pickle.load(fp) #8889 unique books

books_info = []

for book in tqdm(unique_books):
# def get_book_info(book):
    
    html_text = requests.get(book).text
    soup = BeautifulSoup(html_text, "html.parser")
    
    klappentext = soup.find_all('div', {'class': 'smaller'})[-1].text
    
    rezensionsnotiz = [e for e in soup.find_all('div') if e.find('h3', {'class': 'newspaper'}) and e.find('div', {'class': 'paragraph'}) and len(e.findChildren()) in [4,8]]
    rezensionsnotiz = [e.find('div', {'class': 'paragraph'}).text for e in rezensionsnotiz]
    
    stichworter = [e.text for e in soup.find_all('a', {'class': 'kw'})]
    
    temp_dict = {'link': book,
                 'klappentext': klappentext,
                 'rezensionsnotiz': rezensionsnotiz,
                 'stichworter': stichworter}
    books_info.append(temp_dict)
    
    
# books_info = []
# with ThreadPoolExecutor() as excecutor:
#     list(tqdm(excecutor.map(get_book_info, unique_books),total=len(unique_books)))   
    
with open('data/gortych_perlentaucher_books_info.p', 'wb') as fp:
    pickle.dump(books_info, fp, protocol=pickle.HIGHEST_PROTOCOL)
    
#%% keywords search

with open('data/gortych_perlentaucher_books_info.p', 'rb') as fp:
    books_info = pickle.load(fp)
    
dg_keywords = gsheet_to_df('1pocjWCb_hzCxN-FXw4TIeEDuzp0Lo0m60pNInjfuR6Q', 'Słowa kluczowe')

dg_keywords_list = []
for k in dg_keywords:
    dg_keywords_list.extend([e for e in dg_keywords[k].to_list() if pd.notnull(e)])


dg_keywords_lower = [e.lower() for e in dg_keywords_list]

good_books = []
for book in tqdm(books_info):
    # book = books_info[0]
        
    k = book.get('klappentext').lower()
    r = [e.lower() for e in book.get('rezensionsnotiz')]
    s = [e.lower() for e in book.get('stichworter')]
    
    if any(e in k for e in dg_keywords_lower) or any(e in t for t in r for e in dg_keywords_lower) or any(e in t for t in s for e in dg_keywords_lower):
        good_books.append(book)

df = pd.DataFrame(good_books)
df.to_excel('data/gortych_perlentaucher.xlsx', index=False)
    
with open('data/gortych_perlentaucher_good_books_info.p', 'wb') as fp:
    pickle.dump(good_books, fp, protocol=pickle.HIGHEST_PROTOCOL) #5015

#%%sprawdzenie
book = {'link': 'https://www.perlentaucher.de/buch/gerold-hofmann/mutig-gegen-marx-und-mielke.html', 'klappentext': 'Mit Fotos und Dokumenten. Vierzig Jahre lang haben sich Christen in der DDR zu ihrem Glauben bekannt, gegen Anfeindung und Diskriminierung, gegen Hohn und Spott. Manche zogen sich zurück in die innere Emigration, anderen gab der Glaube die Kraft zum Widerstand. Hofmann lässt engagierte Christen zu Wort kommen wie Heino Falcke, ehemals Propst in Erfurt, der sich freiwillig entschied, in Ostdeutschland zu bleiben und zum führenden Kopf der staatskritischen evangelischen Theologie in der DDR wurde, ebenso wie das Zwickauer Ehepaar Antje und Martin Böttger, das sich für Menschenrechte in der DDR einsetzte und deswegen von der Stasi observiert und schikaniert wurde, oder die Berliner Pfarrerin Ruth Misselwitz, auf deren Leben die Stasi einen Anschlag plante, weil sie an ihrer Kirchengemeinde in Pankow einen Friedenskreis leitete. Ausführliche Interviews dokumentieren die Gründe, die Christen zu kritischem Verhalten gegenüber dem SED-Staat motivierten und die schließlich auch dazu beitrugen, dass die Revolution im Herbst 1989 frei von aufständischer Gewalt blieb.', 'rezensionsnotiz': ['Bemerkenswert findet Rezensent Ilko-Sascha Kowalczuk diesen Interviewband, in dem er besonders die Rolle der evangelischen Kirche eindringlich herausgearbeitet findet. Denn erstens werfen die darin enthaltenen Gespräche mit DDR-Kirchenleuten und einfachen Christen ein Licht auf deren wichtigen Beitrag zur Wende. Zweitens beeindruckt den Rezensenten die "christliche Demut", mit der die Befragten ihre Rolle in jener Zeit heute reflektieren, das Unaggressive ihrer revolutionären Positionen und ihr überzeugendes Plädoyer für den berühmten aufrechten Gang.'], 'stichworter': ['Christen', 'Evangelische Kirche', 'Interview', 'Menschenrechte', 'Stasi', 'Widerstand', 'Wiedervereinigung']}

book = {'link': 'https://www.perlentaucher.de/buch/joel-dicker/die-letzten-tage-unserer-vaeter.html', 'klappentext': 'Aus dem Französischen von Amelie Thoma und Michaela Messner. 1940 verlässt der junge Paul-Emile überstürzt seine Heimatstadt Paris. Nicht einmal sein Vater weiß, wohin er geht. Denn Paul schließt sich einer geheimen Spionageeinheit an, die Winston Churchill ins Leben gerufen hat. Mit einer Handvoll französischer Freiwilliger, Stan, Gros, Flaron, Cucu und Laura, lehrt man ihn die Kunst des geheimen Krieges.Die Aufträge sind gefährlich, und die Missionen scheinen nie zu enden. So wird ihnen die Gruppe zur zweiten Familie, in der Loyalität, Sicherheit, Freundschaft und Liebe alle zusammenschweißen. In der Hoffnung, gemeinsam die letzte Mission zu überstehen.', 'rezensionsnotiz': ['Rezensent Roman Bucheli freut sich, dass dieser frühe Roman des frankophonen Schweizer Schriftstellers Joel Dicker nun auch auf Deutsch erschienen ist. Denn hier ist noch das ganze Talent Dickers zu erkennen, fährt der Kritiker fort, der den Roman im Vergleich zu Sibylle Bergs "RCE" liest. Hier wie dort geht es um Welterlösung, bei Dicker sind es keine Nerds, sondern französische Widerstandskämpfer, die 1940 in England zu Geheimagenten ausgebildet werden, um die Welt vom Faschismus zu befreien, erläutert der Kritiker. Ein paar Kitschmomente macht Bucheli auch hier aus (diesem Hang sollte Dicker später folgen, seufzt er). Vor allem aber preist er das Buch als "intellektuelles Vergnügen", das alle Melodramatik durch Tempo, Tricks und Erzählkunst ausbremst.'], 'stichworter': ['Zweiter Weltkrieg', 'Spionage', 'Spionageroman', 'Schweiz'], 'keywords': []}

k = book.get('klappentext').lower()
r = [e.lower() for e in book.get('rezensionsnotiz')]
s = [e.lower() for e in book.get('stichworter')]

for el in dg_keywords_lower:
    if el in k:
        print(f"Znaleziono '{el}' w tekście: '{k}'")

for t in r:
    for el in dg_keywords_lower:
        if el in t:
            print(f"Znaleziono '{el}' w tekście: '{t}'")
            
for t in s:
    for el in dg_keywords_lower:
        if el in t:
            print(f"Znaleziono '{el}' w tekście: '{t}'")



#%%
with open('data/gortych_perlentaucher_good_books_info.p', 'rb') as fp:
    good_books = pickle.load(fp)

dg_keywords = gsheet_to_df('1pocjWCb_hzCxN-FXw4TIeEDuzp0Lo0m60pNInjfuR6Q', 'Słowa kluczowe')
dg_keywords_list = []
for k in dg_keywords:
    dg_keywords_list.extend([e for e in dg_keywords[k].to_list() if pd.notnull(e)])
dg_keywords_lower = [e.lower() for e in dg_keywords_list]


for book in tqdm(good_books):
    keyword_hits = []
    for kw in dg_keywords_lower:
        if kw in book.get('klappentext').lower():
            keyword_hits.append(kw)
        for r in book.get('rezensionsnotiz'):
            if kw in r.lower():
                keyword_hits.append(kw)
        for s in book.get('stichworter'):
            if kw in s.lower():
                keyword_hits.append(kw)
    if 'gewalt' in keyword_hits:
        gewalt = True
    else: gewalt = False
    
    book.update({'keywords': set(keyword_hits),
                 'gewalt': gewalt})
    
for book in tqdm(good_books):
    # book = good_books[2222]
    url = book.get('link')
    
    html_text = requests.get(url).text
    soup = BeautifulSoup(html_text, "html.parser")
    
    author = soup.find('h3', {'class': 'bookauthor'}).text
    title = soup.find('h3', {'class': 'booktitle'}).text
    grey = soup.find('div', {'class': 'tiny gray'}).text.split('\n')[0]
    publisher = grey.split(',')[0].strip()
    place = grey.split(',')[1].strip()[:-4].strip()
    year = grey.split(',')[1].strip()[-4:]
    
    book.update({'author': author,
                 'title': title,
                 'publisher': publisher,
                 'place': place,
                 'year': year})
                
with open('data/gortych_perlentaucher_good_books_info.p', 'wb') as fp:
    pickle.dump(good_books, fp, protocol=pickle.HIGHEST_PROTOCOL)
    

df = pd.DataFrame(good_books)[['link', 'author', 'title', 'publisher', 'place', 'year', 'gewalt', 'keywords', 'klappentext', 'rezensionsnotiz', 'stichworter']]
df.to_excel('data/gortych_perlentaucher.xlsx', index=False)
    
    
    
    
    
    
    
    
















