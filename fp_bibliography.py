from my_functions import gsheet_to_df, df_to_gsheet, get_cosine_result
import pandas as pd
import regex as re
import glob
from unidecode import unidecode
import datetime
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import fp_credentials
import time
from selenium.common.exceptions import NoSuchElementException, ElementNotInteractableException, NoAlertPresentException, SessionNotCreatedException, TimeoutException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import numpy as np
import sys
import io
import requests
from bs4 import BeautifulSoup
import pandasql
import Levenshtein as lev
import itertools
import docx
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage
from io import StringIO
from pdfminer.pdfdocument import PDFDocument
import PyPDF2


# local files

extentions = ('*.pdf', '*.docx')
local_files = []
for extention in extentions:
    local_files.extend(glob.glob(f'F:/Cezary/Documents/FP/bibliografia starych numerów/***/**/*/{extention}'))

local_df = pd.DataFrame(local_files, columns=['total'])
local_df['numer'] = local_df['total'].apply(lambda x: re.findall('(?<= numerów\\\\)(.*?)(?=\\\\)', x)[0])
local_df['język'] = local_df['total'].apply(lambda x: re.sub('(.* numerów\\\\)(.*?\\\\)(eng|pl)(\\\\.*$)', r'\3', x))
local_df['kiedy'] = local_df['total'].apply(lambda x: re.findall('(?<=eng\\\\|pl\\\\)(.*?)(?=\\\\)', x)[0])
local_df['plik'] = local_df.apply(lambda x: x['total'][x['total'].index(x['kiedy']) + len(x['kiedy']) + 1:], axis=1)
local_df['rozszerzenie'] = local_df['plik'].apply(lambda x: re.sub('(.*?)(\.(?!.*\.))(.*$)', r'\3', x))
local_df['plik'] = local_df['plik'].apply(lambda x: re.sub('(.*?)(\.(?!.*\.))(.*$)', r'\1', x))

local_df = local_df[((local_df['kiedy'] == 'przed korektą') & (local_df['rozszerzenie'] == 'pdf')) |
                    ((local_df['kiedy'] == 'po korekcie') & (local_df['rozszerzenie'] == 'docx'))].reset_index(drop=True)
local_df['index'] = local_df.index+1

# collecting data from the website

browser = webdriver.Firefox()    
browser.get("http://fp.amu.edu.pl/admin")    
browser.implicitly_wait(5)
username_input = browser.find_element_by_id('user_login')
password_input = browser.find_element_by_id('user_pass')

username = fp_credentials.wordpress_username
password = fp_credentials.wordpress_password
time.sleep(1)
username_input.send_keys(username)
password_input.send_keys(password)

login_button = browser.find_element_by_id('wp-submit').click()

# collecting FP issues

urls = ['http://fp.amu.edu.pl/wp-admin/edit.php?post_type=page', 'http://fp.amu.edu.pl/wp-admin/edit.php?post_type=page&paged=2', 'http://fp.amu.edu.pl/wp-admin/edit.php?post_type=page&paged=3']

fp_issues = []

for url in urls:
    browser.get(url)  
    fp_issues_selector = browser.find_elements_by_css_selector('.row-title')
    for issue in fp_issues_selector:
        fp_issues.append([issue.text, issue.get_attribute('href')])
        
fp_issues = [l for l in fp_issues if any(v in l[0] for v in ('Forum Poetyki', 'Forum of Poetics'))]

for index, (issue, url) in enumerate(fp_issues):
    print(f"{index+1}/{len(fp_issues)}")
    browser.get(url)
    whole_link = browser.find_element_by_id('edit-slug-buttons').click()
    direct_link = browser.find_element_by_id('new-post-slug')
    direct_link = direct_link.get_attribute('value')
    content = browser.find_element_by_id('content').get_attribute('value')
    try:
        browser.find_element_by_css_selector('#xili_language_check_pl_pl')
        language = 'pl'
    except NoSuchElementException:
        language = 'eng'
    fp_issues[index] += [f"http://fp.amu.edu.pl/{direct_link}", content, language]
 
# collecting FP articles

url = 'http://fp.amu.edu.pl/wp-admin/edit.php'
browser.get(url)
total_no_of_pages = int(browser.find_element_by_css_selector('.total-pages').text)

fp_articles = []        
no_of_pages = 1 

while no_of_pages <= total_no_of_pages:
    print(f"{no_of_pages}/{total_no_of_pages}")
    titles = browser.find_elements_by_css_selector('.row-title')
    categories = browser.find_elements_by_css_selector('.column-categories a')
    tags = browser.find_elements_by_css_selector('#the-list .column-tags')
    languages = browser.find_elements_by_css_selector('.column-language div')
    for ti, ca, ta, la in zip(titles, categories, tags, languages):
        fp_articles.append([ti.text, ti.get_attribute('href'), ca.text, ta.text, la.text])
    no_of_pages += 1
    try:
        next_page = browser.find_element_by_css_selector('.next-page')
        next_page.click()
    except NoSuchElementException:
        pass

for i, l in enumerate(fp_articles):
    if l[-1][:2] == 'pl':
        fp_articles[i][-1] = 'pl'
    else:
        fp_articles[i][-1] = 'eng'
    
for index, article in enumerate(fp_articles):
    print(f"{index+1}/{len(fp_articles)}")
    browser.get(article[1])
    whole_link = browser.find_element_by_id('edit-slug-buttons').click()
    direct_link = browser.find_element_by_id('new-post-slug')
    direct_link = direct_link.get_attribute('value')
    content = browser.find_element_by_id('content').get_attribute('value')
    fp_articles[index] += [f"http://fp.amu.edu.pl/{direct_link}", content]
    
browser.close()

fp_issues_df = pd.DataFrame(fp_issues, columns=['tytuł numeru', 'www edycji numeru', 'www numeru', 'wstęp', 'język numeru'])
fp_articles_df = pd.DataFrame(fp_articles, columns=['tytuł artykułu', 'www edycji artykułu', 'kategoria', 'tagi', 'język artykułu', 'www numeru', 'abstrakt'])

fp_articles_df['abstrakt'] = fp_articles_df['abstrakt'].apply(lambda x: x.strip())

fp_articles_df = fp_articles_df[~fp_articles_df['tagi'].str.contains('Brak tagów')]

def get_author_and_tag(x):
    tagi = x.split(', ')
    for tag in tagi:
        if re.findall('^\p{Lu}', tag):
            tag_autora = tag
        else:
            tag_numeru = tag
    return (tag_autora, tag_numeru)

fp_articles_df['autor'], fp_articles_df['tag numeru'] = zip(*fp_articles_df['tagi'].apply(lambda x: get_author_and_tag(x)))

nowe_numery = ['lato 2020', 'summer 2020', 'zima 2020', 'winter 2020', 'lato 2019', 'summer 2019', 'wiosna 2020', 'spring 2020', 'jesień 2019', 'fall 2019']

fp_articles_df = fp_articles_df[~fp_articles_df['tag numeru'].isin(nowe_numery)]

query = "select * from fp_issues_df a join fp_articles_df b on a.'tytuł numeru' like '%'||b.'tag numeru'||'%'"
fp_merged = pandasql.sqldf(query)
fp_merged['index'] = fp_merged.index+1

# przywrócić abstrakty, na razie wyrzucone, bo tam są jeszcze pełne teksty - ogarnąć później jak je wyciągać

fp_merged = fp_merged.drop(columns='abstrakt')

# połączyć pliki z bibliografiami z metadanymi przy użyciu cosine similarity

fp_final_df = pd.DataFrame()

for i, row in fp_merged.iterrows():
    print(f"{i+1}/{len(fp_merged)}")
    author = row['autor'].split(' ')[-1]
    title = row['tytuł artykułu'].replace('<i>', '').replace('</i>', '')
    search_phraze = unidecode(f"{author} {title}")
    search_result_pdf = pd.DataFrame()
    search_result_docx = pd.DataFrame()
    for index, row2 in local_df.iterrows():
        if row2['rozszerzenie'] == 'pdf':
            result = lev.distance(search_phraze, row2['plik'])
            result_list = [row['index'], row2['index'], result]
            result_df = pd.DataFrame([result_list], columns=['fp_merged index', 'local_df index', 'lev distance'])
            search_result_pdf = search_result_pdf.append(result_df)
        else:
            result = lev.distance(search_phraze, row2['plik'])
            result_list = [row['index'], row2['index'], result]
            result_df = pd.DataFrame([result_list], columns=['fp_merged index', 'local_df index', 'lev distance'])
            search_result_docx = search_result_docx.append(result_df)
    search_result_pdf = search_result_pdf[(search_result_pdf['lev distance'] == search_result_pdf['lev distance'].min()) &
                                          (search_result_pdf['lev distance'] < 100)]
    search_result_docx = search_result_docx[(search_result_docx['lev distance'] == search_result_docx['lev distance'].min()) &
                                            (search_result_docx['lev distance'] < 100)]
    search_result = pd.concat([search_result_pdf, search_result_docx])
    fp_final_df = fp_final_df.append(search_result)
            

test = pd.merge(local_df, fp_final_df, how='left', left_on='index', right_on='local_df index').drop(columns=['local_df index']).rename(columns={'index': 'local_df index'})
test = pd.merge(test, fp_merged, how='left', left_on='fp_merged index', right_on='index').drop(columns=['index'])

wszystkie = len(test)
zrobione_automatycznie = len(test[test['fp_merged index'].notnull()])
do_zrobienia = len(test[test['fp_merged index'].isnull()])
print(wszystkie, zrobione_automatycznie, do_zrobienia)

df_to_gsheet(test, '1PRoGI6mpOIrWiKs9AF87IMcTsGeBHUx5JNsZJwMnsDI', 'połączone')
df_to_gsheet(fp_merged, '1PRoGI6mpOIrWiKs9AF87IMcTsGeBHUx5JNsZJwMnsDI', 'metadane')
df_to_gsheet(local_df, '1PRoGI6mpOIrWiKs9AF87IMcTsGeBHUx5JNsZJwMnsDI', 'bibliografia załącznikowa')

# 14.01.2021

bibliography = gsheet_to_df('1PRoGI6mpOIrWiKs9AF87IMcTsGeBHUx5JNsZJwMnsDI', 'połączone').iloc[:,0:8]
bibliography_metadata = gsheet_to_df('1PRoGI6mpOIrWiKs9AF87IMcTsGeBHUx5JNsZJwMnsDI', 'metadane')

bibliography = pd.merge(bibliography, bibliography_metadata, how='left', left_on='metadane index', right_on='index').sort_values(['metadane index', 'rozszerzenie']).reset_index(drop=True)

for i, row in bibliography.iterrows():
    if row['rozszerzenie'] == 'docx':
        bibliography.at[i, 'ścieżka do bibliografii'] = row['total']
    elif row['rozszerzenie'] == 'pdf':
        bibliography.at[i-1, 'ścieżka do pdf'] = row['total']
        
bibliography = bibliography[bibliography['ścieżka do bibliografii'].notnull()]

# tytuł numeru	język	wstęp	folder lokalny	numer	rok	spis treści	link do pdf	link do jpg	jpg	odnosnik	url_edycji

# lp	autor	ORCID	tytuł artykułu	kategoria	język	tytuł numeru	tag numeru	abstrakt	folder lokalny	afiliacja	biogram	bibliografia	słowa kluczowe	strony	tłumacz	tag autora	link do pdf	link do jpg	pdf	jpg	odnosnik	url_edycji	spis treści

bibliography_articles = bibliography[['autor', 'tytuł artykułu', 'kategoria', 'język artykułu', 'tytuł numeru', 'tag numeru', 'total', 'www edycji artykułu', 'ścieżka do pdf', 'ścieżka do bibliografii']].rename(columns={'język artykułu':'język', 'total':'folder lokalny', 'www edycji artykułu':'url_edycji'})

bibliography_articles['pdf'] = bibliography_articles['ścieżka do pdf'].apply(lambda x: re.split('(\\\\|\/)(?!.*(\\\\|\/))', x)[-1]).str.replace('\.pdf$', '', regex=True)

numbers_order = ['lato 2015',
 'summer 2015',
 'jesień 2015',
 'fall 2015',
 'zima 2016',
 'winter 2016',
 'wiosna/lato 2016',
 'spring/summer 2016',
 'jesień 2016',
 'fall 2016',
 'zima 2017',
 'winter 2017',
 'wiosna/lato 2017',
 'spring/summer 2017',
 'jesień 2017',
 'fall 2017',
 'zima/wiosna 2018',
 'winter/spring 2018',
 'lato 2018',
 'summer 2018',
 'jesień 2018',
 'fall 2018',  
 'winter/spring 2019',
 'zima/wiosna 2019']

categories_order = ['Teorie',
 'Przekłady',
 'Praktyki',
 'Słownik poetologiczny',
 'Archiwum poetologiczne',
 'Krytyki',
 'Polemiki']

combinations = list(itertools.product(numbers_order, categories_order))

bibliography_articles['sort'] = bibliography_articles[['tag numeru', 'kategoria']].apply(lambda x: (x['tag numeru'], x['kategoria']), axis=1).astype('category')
bibliography_articles['sort'] = bibliography_articles['sort'].cat.set_categories(combinations, ordered=True)
bibliography_articles = bibliography_articles.sort_values('sort').drop(columns='sort').reset_index(drop=True)

# później jeszcze sortować po stronach, gdy Gerard je doda

bibliography_articles['lp'] = None
bibliography_articles['ORCID'] = None
bibliography_articles['abstrakt'] = None
bibliography_articles['afiliacja'] = None
bibliography_articles['biogram'] = None

def getText(filename):
    doc = docx.Document(filename)
    fullText = []
    for para in doc.paragraphs:
        fullText.append(para.text)
    return '\n'.join(fullText)

for i, row in bibliography_articles.iterrows():
    print(f"{i+1}/{len(bibliography_articles)}")
    path = re.sub('\\\\', '/', row['ścieżka do bibliografii'])
    bibliography_articles.at[i, 'bibliografia'] = getText(path).strip()
    
bibliography_articles['słowa kluczowe'] = None
bibliography_articles['strony'] = None
bibliography_articles['tłumacz'] = None
#bibliography_articles['długość bibliografii'] = bibliography_articles['bibliografia'].map(len)

df_to_gsheet(bibliography_articles, '15O0yOBJ-pEWo8iOsyxwtivtgHawQNGnFnB75wx_3pao', 'artykuły')

# ORCID enrichment

fp_autorzy = gsheet_to_df('15O0yOBJ-pEWo8iOsyxwtivtgHawQNGnFnB75wx_3pao', 'artykuły')['autor'].drop_duplicates().to_list()

browser = webdriver.Firefox()  
final_output = []
for i, autor in enumerate(fp_autorzy):
    print(f"{i+1}/{len(fp_autorzy)}")
    url = f"https://orcid.org/orcid-search/search?searchQuery={autor}"
    imie = ' '.join(autor.split(' ')[:-1])
    nazwisko = autor.split(' ')[-1]
      
    browser.get(url)
    time.sleep(5)
    orcid_id = [t.text for t in browser.find_elements_by_css_selector('#main a')]
    orcid_url = [t.get_attribute('href') for t in browser.find_elements_by_css_selector('#main a')]
    first_name = [t.text for t in browser.find_elements_by_css_selector('.orcid-id-column+ td')]
    last_name = [t.text for t in browser.find_elements_by_css_selector('td:nth-child(3)')]
    affiliations = [t.text for t in browser.find_elements_by_css_selector('td:nth-child(5)')]
    total = list(zip(orcid_id, orcid_url, first_name, last_name, affiliations, itertools.repeat(autor)))[:5]
    final_output += total
    
browser.close()
        
df = pd.DataFrame(final_output, columns=['orcid id', 'orcid url', 'first name', 'last name', 'affiliations', 'nazwa autora fp'])
    
df_to_gsheet(df, '15O0yOBJ-pEWo8iOsyxwtivtgHawQNGnFnB75wx_3pao', 'orcid')    
    
# wordpress bios

browser = webdriver.Firefox()    
browser.get("http://fp.amu.edu.pl/admin")    
browser.implicitly_wait(5)
username_input = browser.find_element_by_id('user_login')
password_input = browser.find_element_by_id('user_pass')

username = fp_credentials.wordpress_username
password = fp_credentials.wordpress_password
time.sleep(1)
username_input.send_keys(username)
password_input.send_keys(password)

login_button = browser.find_element_by_id('wp-submit').click()

browser.get('http://fp.amu.edu.pl/wp-admin/edit-tags.php?taxonomy=post_tag')

total_no_of_pages = int(browser.find_element_by_css_selector('.total-pages').text)

fp_bios = []        
no_of_pages = 1 

while no_of_pages <= total_no_of_pages:
    print(f"{no_of_pages}/{total_no_of_pages}")
    people = [p.get_attribute('href') for p in browser.find_elements_by_css_selector('.row-title')]
    list_url = browser.current_url
    for person in people:
        browser.get(person)
        name = browser.find_element_by_id('name').get_attribute('value')
        tag = browser.find_element_by_id('slug').get_attribute('value')
        bio = browser.find_element_by_id('description').get_attribute('value')
        person_info = [name, tag, bio]
        fp_bios.append(person_info)
    no_of_pages += 1
    try:
        browser.get(list_url)
        next_page = browser.find_element_by_css_selector('.next-page')
        next_page.click()
    except NoSuchElementException:
        pass

fp_bios_df = pd.DataFrame(fp_bios, columns=['name', 'tag', 'bio']) 
    
df_to_gsheet(fp_bios_df, '15O0yOBJ-pEWo8iOsyxwtivtgHawQNGnFnB75wx_3pao', 'bio')


# wordpress abstracts

browser = webdriver.Firefox()    
browser.get("http://fp.amu.edu.pl/admin")    
browser.implicitly_wait(5)
username_input = browser.find_element_by_id('user_login')
password_input = browser.find_element_by_id('user_pass')

username = fp_credentials.wordpress_username
password = fp_credentials.wordpress_password
time.sleep(1)
username_input.send_keys(username)
password_input.send_keys(password)

login_button = browser.find_element_by_id('wp-submit').click()

url = 'http://fp.amu.edu.pl/wp-admin/edit.php'
browser.get(url)
total_no_of_pages = int(browser.find_element_by_css_selector('.total-pages').text)

fp_articles = []        
no_of_pages = 1 

while no_of_pages <= total_no_of_pages:
    print(f"{no_of_pages}/{total_no_of_pages}")
    titles = browser.find_elements_by_css_selector('.row-title')
    languages = browser.find_elements_by_css_selector('.column-language div')
    for ti, la in zip(titles, languages):
        fp_articles.append([ti.text, ti.get_attribute('href'), la.text])
    no_of_pages += 1
    try:
        next_page = browser.find_element_by_css_selector('.next-page')
        next_page.click()
    except NoSuchElementException:
        pass

for i, l in enumerate(fp_articles):
    if l[-1][:2] == 'pl':
        fp_articles[i][-1] = 'pl'
    else:
        fp_articles[i][-1] = 'eng'
    
for index, article in enumerate(fp_articles):
    print(f"{index+1}/{len(fp_articles)}")
    browser.get(article[1])
    content = browser.find_element_by_id('content').get_attribute('value')
    fp_articles[index].append(content)
    
browser.close()

fp_articles_df = pd.DataFrame(fp_articles, columns=['tytuł artykułu', 'www edycji artykułu', 'język artykułu', 'abstrakt'])

fp_articles_df['abstrakt'] = fp_articles_df['abstrakt'].apply(lambda x: x.strip())

test = fp_articles_df.copy()[fp_articles_df['abstrakt'].str.contains('Abstrakt|Abstract|A b s t r a k t|A b s t r a c t')]
test['abstrakt index'] = test['abstrakt'].apply(lambda x: [(m.start(0), m.end(0)) for m in re.finditer('Abstrakt|Abstract|A b s t r a k t|A b s t r a c t', x)])
test['abstrakt index'] = test['abstrakt index'].apply(lambda x: x[-1][-1])

def bar_index(x):
    try: 
        val = x.rfind('<hr />')
    except (ValueError, IndexError):
        val = None
    return val

test['bar index'] = test['abstrakt'].apply(lambda x: bar_index(x))

def cut_abstract(x):
    if x['abstrakt index'] < x['bar index']:
        val = x['abstrakt'][int(x['abstrakt index']):x['bar index']]
    else:
        val = x['abstrakt'][int(x['abstrakt index']):]
    return val

test['abstrakt 2'] = test[['abstrakt index', 'bar index', 'abstrakt']].apply(lambda x: cut_abstract(x), axis=1)

def bar_index2(x):
    try: 
        val = x.index('<hr')
    except (ValueError, IndexError):
        val = None
    return val

test['bar index'] = test['abstrakt 2'].apply(lambda x: bar_index2(x))

test['abstrakt 2'] = test[['bar index', 'abstrakt 2']].apply(lambda x: x['abstrakt 2'][:int(x['bar index'])] if pd.notnull(x['bar index']) else x['abstrakt 2'], axis=1)
test['abstrakt 2'] = test['abstrakt 2'].str.replace(' :</span></h4>', '', regex=False).str.replace(':</span></h4>', '', regex=False).str.replace(':</span></h3>', '', regex=False).str.replace('</span></h4>', '', regex=False).str.replace('</span></strong>', '', regex=False).str.replace('</div>', '', regex=False).str.replace('<div>', '', regex=False).str.replace('<div id="ftn34">', '', regex=False).str.strip()

test = test[['tytuł artykułu', 'www edycji artykułu', 'język artykułu', 'abstrakt 2']]

df_to_gsheet(test, '15O0yOBJ-pEWo8iOsyxwtivtgHawQNGnFnB75wx_3pao', 'abstrakty')

# automatyczne wydobycie słów kluczowych z pdf

df = gsheet_to_df('15O0yOBJ-pEWo8iOsyxwtivtgHawQNGnFnB75wx_3pao', 'artykuły')
df = df.replace('test', '')

def convert_pdf_to_txt(path):
    rsrcmgr = PDFResourceManager()
    retstr = StringIO()
    codec = 'utf-8'
    laparams = LAParams()
    device = TextConverter(rsrcmgr, retstr, codec=codec, laparams=laparams)
    fp = open(path, 'rb')
    interpreter = PDFPageInterpreter(rsrcmgr, device)
    password = ""
    maxpages = 0
    caching = True
    pagenos=set()

    for page in PDFPage.get_pages(fp, pagenos, maxpages=maxpages, password=password,caching=caching, check_extractable=True):
        interpreter.process_page(page)

    text = retstr.getvalue()

    fp.close()
    device.close()
    retstr.close()
    return text

for i, row in df.iterrows():
    print(f"{i+1}/{len(df)}")
    df.at[i, 'text'] = convert_pdf_to_txt(row['ścieżka do pdf'])
    
def get_keywords(x):
    try:
        if x['język'] == 'pl':
            pattern = re.escape('Słowa kluczowe | Abstrakt | Nota o autorze')
        else:
            pattern = re.escape('Keywords | Abstract | Note on the author')
        val = [(m.start(0), m.end(0)) for m in re.finditer(pattern.lower(), x['text'].lower())]
        val = val[-1][-1]
        result = x['text'][int(val):]
    except IndexError:
        result = None
    return result

df['text'] = df.apply(lambda x: get_keywords(x), axis=1)
   
df_to_gsheet(df, '15O0yOBJ-pEWo8iOsyxwtivtgHawQNGnFnB75wx_3pao', 'keywords')

# automatyczne wydobycie numerów stron

def ranges(i):
    for a, b in itertools.groupby(enumerate(i), lambda pair: pair[1] - pair[0]):
        b = list(b)
        yield f"{b[0][1]}-{b[-1][1]}"
            
            
df = gsheet_to_df('15O0yOBJ-pEWo8iOsyxwtivtgHawQNGnFnB75wx_3pao', 'artykuły')

for ind, row in df.iterrows():
    print(f"{ind+1}/{len(df)}")
    try:
        path = row['ścieżka do pdf']

        read_pdf = PyPDF2.PdfFileReader(path) 
    
        pages_range = [] 
        for i in range(read_pdf.getNumPages()): 
            page = read_pdf.getPage(i) 
            page_content = page.extractText()#.encode('UTF-8') 
            page_no = int(re.findall('^\d+', str(page_content).split('\n')[0])[0])
            pages_range.append(page_no)
        pages_range = ''.join(ranges(pages_range))
        df.at[ind, 'strony automatycznie'] = pages_range
    except IndexError:
        print(str(page_content).split('\n')[0])
        
df = df[['url_edycji', 'strony automatycznie']]

df_to_gsheet(df, '15O0yOBJ-pEWo8iOsyxwtivtgHawQNGnFnB75wx_3pao', 'strony automatycznie')
























