# przed kolejnym numerem:
#I jeszcze w bibliografii, tam gdzie są w pdfie kreski zamiast nazwiska, wstawiło przed nazwiskiem lub zamiast kropkę.
    
#%% import
import pandas as pd
import regex as re
import regex
import glob
from unidecode import unidecode
import datetime
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import fp_credentials
import time
from selenium.common.exceptions import NoSuchElementException, ElementNotInteractableException, NoAlertPresentException, SessionNotCreatedException, ElementClickInterceptedException, InvalidArgumentException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import numpy as np
import sys
import io
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import gspread as gs
from google_drive_research_folders import cr_projects
from gspread_dataframe import set_with_dataframe, get_as_dataframe

pd.options.display.max_colwidth = 10000
#%% def

def get_bool(prompt):
    while True:
        try:
           return {"true":True,"false":False}[input(prompt).lower()]
        except KeyError:
           print("Invalid input please enter True or False!")
           
def jest_autor(x):
    try:
        re.findall('(, \p{Lu})|(^-+|^—+|^–+)', x)[0]
        val = x
    except IndexError:
        val = ''
    return val

def build_author(x):
    try:
        return re.findall('(^.+?)(?=\. \„.+$)', x)[0]
    except IndexError:
        return re.sub('(^.+?)(-|–|—|\p{Ll}\.\)|\p{Ll})(\..+$)', r'\1\2', x)
    
def abstrakt_bio(x):
    x = x.split('\n')
    x = '\n'.join([f'''<span style="font-family: 'Chaparral Pro', serif; color: black;">{e}</span>''' for e in x])
    return x

def biblio_pozycja(x):
    try:
        return re.sub(f"{x['autor pozycji']}.", '', x['bibliografia']).strip()
    except:
        return ''

#%% check

get_bool('Czy są zrobione pliki jpg dla artykułów? ')
get_bool('Czy nazwiska autorów w nazwach plików są poprawne? ')
get_bool('Czy dodano pliki pdf do biblioteki wordpress (plików jpg nie dodawać)? ')
czy_tlumaczenia_po_angielsku = get_bool('Czy w numerze w angielskiej wersji znajdują się przekłady na angielski z innego języka niż polski? ')

#%% connect google drive

#autoryzacja do tworzenia i edycji plików
gc = gs.oauth()
#autoryzacja do penetrowania dysku
gauth = GoogleAuth()
gauth.LocalWebserverAuth()
drive = GoogleDrive(gauth)

#%% today

now = datetime.datetime.now()
year = now.year
month = '{:02d}'.format(now.month)
day = '{:02d}'.format(now.day)

#%% open browser

browser = webdriver.Firefox()

#%% read new issue

file_list = drive.ListFile({'q': f"'{cr_projects}' in parents and trashed=false"}).GetList() 
#[print(e['title'], e['id']) for e in file_list]
fp_folder = [file['id'] for file in file_list if file['title'] == 'Forum Poetyki – redakcja'][0]
file_list = drive.ListFile({'q': f"'{fp_folder}' in parents and trashed=false"}).GetList() 
#[print(e['title'], e['id']) for e in file_list]
fp_folder = [file['id'] for file in file_list if file['title'] == 'redakcja FP'][0]
file_list = drive.ListFile({'q': f"'{fp_folder}' in parents and trashed=false"}).GetList() 
last_number = max(file_list, key=lambda x: x['createdDate'])
print(f"{last_number['title']}  |  {last_number['id']}")

table_id = input('Podaj id arkusza bieżącego numeru: ')
aktualny_numer_sheet = gc.open_by_key(table_id)

aktualny_numer = get_as_dataframe(aktualny_numer_sheet.worksheet('artykuły'), evaluate_formulas=True).dropna(how='all').dropna(how='all', axis=1)
# aktualny_numer = get_as_dataframe(aktualny_numer_sheet.worksheet('artykuły po pętli'), evaluate_formulas=True).dropna(how='all').dropna(how='all', axis=1)
kategorie_wpisow = get_as_dataframe(gc.open_by_key('1hPVa854YZ4DIajzVoWshXFotd3A7lHHz_d-GqEgn4BI').worksheet('Arkusz1')).dropna(how='all').dropna(how='all', axis=1)

foldery_lokalne = aktualny_numer['folder lokalny'].drop_duplicates().to_list()

for folder in foldery_lokalne:
    if 'pl' in folder.lower():
        folder_pl = folder + '/'
    else:
        folder_eng = folder + '/'
        
pdf_pl= [f for f in glob.glob(folder_pl + '*.pdf', recursive=True)]
jpg_pl= [f for f in glob.glob(folder_pl + '*.jpg', recursive=True)]

pdf_eng= [f for f in glob.glob(folder_eng + '*.pdf', recursive=True)]
jpg_eng= [f for f in glob.glob(folder_eng + '*.jpg', recursive=True)]

for i, row in aktualny_numer.iterrows():
    tag_autora = row['autor'].split('❦')
    tag_autora = '❦'.join([unidecode(tag.replace(' ', '-').lower()) for tag in tag_autora])
    aktualny_numer.at[i, 'tag autora'] = tag_autora
    nazwisko = unidecode(row['autor'].split(' ')[-1]).replace('-', '')
    tytul_art = row['tytuł artykułu'].replace('<i>', '').replace('</i>', '')
    slowo_z_tytulu = unidecode(max(tytul_art.split(' '), key=len)).lower()
    if row['język'] == 'pl':
        try:
            sciezka_jpg = [e for e in jpg_pl if nazwisko in unidecode(e) and slowo_z_tytulu in e.lower()][0]
        except IndexError:
            sciezka_jpg = [e for e in jpg_pl if nazwisko in unidecode(e)][0]
        sciezka_jpg = re.sub(r'(.+)(\\(?!.*\\))(.+)', r'\3', sciezka_jpg)
        try:
            sciezka_pdf = [e for e in pdf_pl if nazwisko in unidecode(e) and slowo_z_tytulu in e.lower()][0]
        except IndexError:
            sciezka_pdf = [e for e in pdf_pl if nazwisko in unidecode(e)][0]
        sciezka_pdf = re.sub(r'(.+)(\\(?!.*\\))(.+)', r'\3', sciezka_pdf)
        aktualny_numer.at[i, 'link do pdf'] = f"http://fp.amu.edu.pl/wp-content/uploads/{year}/{month}/{sciezka_pdf}"
        aktualny_numer.at[i, 'link do jpg'] = f"http://fp.amu.edu.pl/wp-content/uploads/{year}/{month}/{sciezka_jpg}"
        aktualny_numer.at[i, 'pdf'] = sciezka_pdf
        aktualny_numer.at[i, 'jpg'] = sciezka_jpg
    elif row['język'] == 'eng':
        try:
            sciezka_jpg = [e for e in jpg_eng if nazwisko in e and slowo_z_tytulu in e.lower()][0]
        except IndexError:
            try:
                sciezka_jpg = [e for e in jpg_eng if nazwisko in e][0]
            except IndexError:
                pass
        sciezka_jpg = re.sub(r'(.+)(\\(?!.*\\))(.+)', r'\3', sciezka_jpg)
        try:
            sciezka_pdf = [e for e in pdf_eng if nazwisko in e and slowo_z_tytulu in e.lower()][0]
        except IndexError:
            try:
                sciezka_pdf = [e for e in pdf_eng if nazwisko in e][0]
            except IndexError:
                pass
        sciezka_pdf = re.sub(r'(.+)(\\(?!.*\\))(.+)', r'\3', sciezka_pdf)
        aktualny_numer.at[i, 'link do pdf'] = f"http://fp.amu.edu.pl/wp-content/uploads/{year}/{month}/{sciezka_pdf}"
        aktualny_numer.at[i, 'link do jpg'] = f"http://fp.amu.edu.pl/wp-content/uploads/{year}/{month}/{sciezka_jpg}"
        aktualny_numer.at[i, 'pdf'] = sciezka_pdf
        aktualny_numer.at[i, 'jpg'] = sciezka_jpg
        
#kodowanie HTML abstraktu i biogramu
# wstęp?
#abstrakt i bio
        
aktualny_numer['abstrakt'] = aktualny_numer['abstrakt'].apply(lambda x: abstrakt_bio(x) if pd.notnull(x) else np.nan)
aktualny_numer['biogram'] = aktualny_numer['biogram'].apply(lambda x: abstrakt_bio(x) if pd.notnull(x) else np.nan)


#wprowadzanie nowych wpisów
        
# =============================================================================
# try:
#     browser = webdriver.Chrome()
#     browser = webdriver.Firefox()
# except SessionNotCreatedException:
#     print('UWAGA!\n\nNieaktualna wersja oprogramowania ChromeDriver\n\nSprawdź wersję przeglądarki Google Chrome\n\nOdwiedź stronę: https://chromedriver.chromium.org/ i pobierz właściwy plik\n\nWypakuj go tutaj: C:\\bin')
# =============================================================================

#%% upload articles at fp.amu.edu.pl
    
browser.get("http://fp.amu.edu.pl/admin")    
browser.implicitly_wait(5)
username_input = browser.find_element('id', 'user_login')
password_input = browser.find_element('id', 'user_pass')

username = fp_credentials.wordpress_username
password = fp_credentials.wordpress_password
time.sleep(1)
username_input.send_keys(username)
password_input.send_keys(password)

login_button = browser.find_element('id', 'wp-submit').click()

for index, row in aktualny_numer.iterrows():
    if row['język'] == 'pl':
      
        browser.get('http://fp.amu.edu.pl/wp-admin/post-new.php')
        
        wybierz_obrazek = browser.find_element('id', 'set-post-thumbnail')
        wybierz_obrazek.click()
        
        wybierz_pliki = browser.find_element('xpath', "//input[starts-with(@id,'html5_')]")
        wybierz_pliki.send_keys(f"{row['folder lokalny']}\\{row['jpg']}")
        zaakceptuj_obrazek = browser.find_element('xpath', "//button[@class = 'button media-button button-primary button-large media-button-select']")
        zaakceptuj_obrazek.click()
        
        while True:
            try:
                zaakceptuj_obrazek = browser.find_element('xpath', "//button[@class = 'button media-button button-primary button-large media-button-select']")
                zaakceptuj_obrazek.click()
                wybierz_pdf = browser.find_element('xpath', "//select[@id = 'metakeyselect']/option[text()='pdf-url']").click()
            except ElementClickInterceptedException:
                time.sleep(5)
                continue
            break
        
        tekstowy = browser.find_element('id', 'content-html').click()

        wprowadz_tytul = browser.find_element('name', 'post_title')
        wprowadz_tytul.send_keys(row['tytuł artykułu'])

        jezyk_pl_button = browser.find_element('id', 'xili_language_check_pl_pl').click()

        kategoria_id = kategorie_wpisow.copy()[kategorie_wpisow['kategoria'] == row['kategoria']]['id'].to_string(index=False).strip()
        time.sleep(1)
        kategoria = browser.find_element('id', kategoria_id)
        time.sleep(1)
        if kategoria.get_attribute('id') == kategoria_id:
            kategoria.click()
        else:
            print(f"Błąd w artytule o tytule: {row['tytuł artykułu']}")

        tagi = browser.find_element('id', 'new-tag-post_tag')
        tagi_wpisu = ','.join(row['autor'].split('❦')) + ',' + row['tag numeru']
        tagi.send_keys(tagi_wpisu)
        dodaj_tagi = browser.find_element('xpath', "//input[@class = 'button tagadd']").click()
        
        # wybierz_obrazek = browser.find_element('id', 'set-post-thumbnail')
        # wybierz_obrazek.click()
        
        # wybierz_pliki = browser.find_element('xpath', "//input[starts-with(@id,'html5_')]")
        # wybierz_pliki.send_keys(f"{row['folder lokalny']}\\{row['jpg']}")
        # zaakceptuj_obrazek = browser.find_element('xpath', "//button[@class = 'button media-button button-primary button-large media-button-select']")
        # zaakceptuj_obrazek.click()
        
# =============================================================================
#         while True:
#             try:
#                 search_box = browser.find_element_by_id('mla-media-search-input').clear()
#                 search_box = browser.find_element_by_id('mla-media-search-input')
#                 search_box.send_keys(row['jpg'][:-4])
#                 search_button = browser.find_element_by_id('mla-search-submit').click()
#                 time.sleep(2)
#                 znajdz_obrazek = browser.find_element_by_css_selector('.thumbnail').click()
#                 time.sleep(2)
#                 zaakceptuj_obrazek = browser.find_element_by_xpath("//button[@class = 'button media-button button-primary button-large media-button-select']").click()
#                 czy_obrazek = browser.find_element_by_xpath("//img[@class = 'attachment-post-thumbnail size-post-thumbnail']")
#             except NoSuchElementException:
#                 time.sleep(5)
#                 continue
#             break
# =============================================================================

        # while True:
        #     try:
        #         zaakceptuj_obrazek = browser.find_element('xpath', "//button[@class = 'button media-button button-primary button-large media-button-select']")
        #         zaakceptuj_obrazek.click()
        #         wybierz_pdf = browser.find_element('xpath', "//select[@id = 'metakeyselect']/option[text()='pdf-url']").click()
        #     except ElementClickInterceptedException:
        #         time.sleep(5)
        #         continue
        #     break
        wprowadz_pdf_link = browser.find_element('id', 'metavalue').send_keys(row['link do pdf'])
        dodaj_pdf = browser.find_element('id', "newmeta-submit").click()
        
        if row['kategoria'] == 'Przekłady' and pd.isnull(row['ORCID']):
            tagi_autorow = row['tag autora'].split('❦')
            autorzy = row['autor'].split('❦')
            
            sekcja_autorstwa = """"""

            for t, a in zip(tagi_autorow, autorzy):
                tag_autor_orcid_line = f"""<h4><a href="http://fp.amu.edu.pl/tag/{t}">{a}</a></h4>\n"""
                sekcja_autorstwa += tag_autor_orcid_line
                
        else:            

            tagi_autorow = row['tag autora'].split('❦')
            autorzy = row['autor'].split('❦')
            orcidy = row['ORCID'].split('❦')
    
            sekcja_autorstwa = """"""
    
            for t, a, o in zip(tagi_autorow, autorzy, orcidy):
                tag_autor_orcid_line = f"""<h4><a href="http://fp.amu.edu.pl/tag/{t}">{a}</a></h4>
            <div><strong><strong>ORCID:</strong></strong> <a href="https://orcid.org/{o}">{o}</a></div>\n"""
                sekcja_autorstwa += tag_autor_orcid_line

        abstrakt = row['abstrakt']

        body = f"""{sekcja_autorstwa}
        <hr />
        <p style="text-align: justify;"><strong><span style="color: #808080;">A b s t r a k t</span></strong></p>
        
        {abstrakt}"""

        content = browser.find_element('id', 'content').send_keys(body)

        opublikuj = browser.find_element('id', 'publish').click()
        
        time.sleep(10)
        
        caly_link = browser.find_element('id', 'edit-slug-buttons')
        caly_link.click()
        odnosnik = browser.find_element('id', 'new-post-slug')
        odnosnik = odnosnik.get_attribute('value')
        aktualny_numer.at[index, 'odnosnik'] = odnosnik
        
        url_edycji = browser.current_url
        aktualny_numer.at[index, 'url_edycji'] = url_edycji

        create_english = browser.find_element('xpath', "//a[@title = 'For create a linked draft translation in en_US']").click()
#English
        tekstowy = browser.find_element('id', 'content-html').click()

        wprowadz_tytul = browser.find_element('name', 'post_title').clear()
        wprowadz_tytul = browser.find_element('name', 'post_title').send_keys(aktualny_numer.at[index+1, 'tytuł artykułu'])
        
        tagi = browser.find_element('id', 'new-tag-post_tag')
        tagi_wpisu = ','.join(aktualny_numer.at[index+1, 'autor'].split('❦')) + ',' + aktualny_numer.at[index+1, 'tag numeru']
        tagi.send_keys(tagi_wpisu)
        dodaj_tagi = browser.find_element('xpath', "//input[@class = 'button tagadd']").click()
        
        try:
            usun_obrazek = browser.find_element('id', 'remove-post-thumbnail').click()
        except NoSuchElementException:
            pass
        
        wybierz_obrazek = browser.find_element('id', 'set-post-thumbnail').click()
        wybierz_pliki = browser.find_element('xpath', "//input[starts-with(@id,'html5_')]")
        try:
            wybierz_pliki.send_keys(f"{aktualny_numer.at[index+1, 'folder lokalny']}\\{aktualny_numer.at[index+1, 'jpg']}")
        except InvalidArgumentException:
            wybierz_pliki.send_keys(f"{row['folder lokalny']}\\{row['jpg']}")
        zaakceptuj_obrazek = browser.find_element('xpath', "//button[@class = 'button media-button button-primary button-large media-button-select']")
        zaakceptuj_obrazek.click()

        while True:
            try:
                zaakceptuj_obrazek = browser.find_element('xpath', "//button[@class = 'button media-button button-primary button-large media-button-select']")
                zaakceptuj_obrazek.click()
                wybierz_pdf = browser.find_element('xpath', "//select[@id = 'metakeyselect']/option[text()='pdf-url']").click()
            except ElementClickInterceptedException:
                time.sleep(5)
                continue
            break
        wprowadz_pdf_link = browser.find_element('id', 'metavalue').send_keys(aktualny_numer.at[index+1, 'link do pdf'])
        dodaj_pdf = browser.find_element('id', "newmeta-submit").click()
        
        if czy_tlumaczenia_po_angielsku == False and aktualny_numer.at[index+1, 'kategoria'] == 'Przekłady' and pd.isnull(aktualny_numer.at[index+1, 'ORCID']):
            tagi_autorow = aktualny_numer.at[index+1, 'tag autora'].split('❦')
            autorzy = aktualny_numer.at[index+1, 'autor'].split('❦')
            
            sekcja_autorstwa = """"""

            for t, a in zip(tagi_autorow, autorzy):
                tag_autor_orcid_line = f"""<h4><a href="http://fp.amu.edu.pl/tag/{t}">{a}</a></h4>\n"""
                sekcja_autorstwa += tag_autor_orcid_line
            
            abstrakt = f"""
            <p style="text-align: justify;"><span style="color: #000000;">We publish</span> <a href="http://fp.amu.edu.pl/{aktualny_numer.at[index, 'odnosnik']}">Polish translation</a> <span style="color: #000000;">of the fragment of {a}'s {aktualny_numer.at[index+1, 'abstrakt']}</span></p>"""
                

            body = f"""{sekcja_autorstwa}
            <hr />
            
            {abstrakt}"""
        elif czy_tlumaczenia_po_angielsku == False and aktualny_numer.at[index+1, 'kategoria'] == 'Przekłady':
            tagi_autorow = aktualny_numer.at[index+1, 'tag autora'].split('❦')
            autorzy = aktualny_numer.at[index+1, 'autor'].split('❦')
            orcidy = aktualny_numer.at[index+1, 'ORCID'].split('❦')
            
            sekcja_autorstwa = """"""

            for t, a, o in zip(tagi_autorow, autorzy, orcidy):
                tag_autor_orcid_line = f"""<h4><a href="http://fp.amu.edu.pl/tag/{t}">{a}</a></h4>
            <div><strong><strong>ORCID:</strong></strong> <a href="https://orcid.org/{o}">{o}</a></div>\n"""
                sekcja_autorstwa += tag_autor_orcid_line
            
            abstrakt = f"""
            <p style="text-align: justify;"><span style="color: #000000;">We publish</span> <a href="http://fp.amu.edu.pl/{aktualny_numer.at[index, 'odnosnik']}">Polish translation</a> <span style="color: #000000;">of the fragment of {a}'s {aktualny_numer.at[index+1, 'abstrakt']}</span></p>"""
                
            body = f"""{sekcja_autorstwa}
            <hr />
            
            {abstrakt}"""
        else:
            tagi_autorow = aktualny_numer.at[index+1, 'tag autora'].split('❦')
            autorzy = aktualny_numer.at[index+1, 'autor'].split('❦')
            orcidy = aktualny_numer.at[index+1, 'ORCID'].split('❦')
    
            sekcja_autorstwa = """"""
    
            for t, a, o in zip(tagi_autorow, autorzy, orcidy):
                tag_autor_orcid_line = f"""<h4><a href="http://fp.amu.edu.pl/tag/{t}">{a}</a></h4>
            <div><strong><strong>ORCID:</strong></strong> <a href="https://orcid.org/{o}">{o}</a></div>\n"""
                sekcja_autorstwa += tag_autor_orcid_line
    
            abstrakt = aktualny_numer.at[index+1, 'abstrakt']
    
            body = f"""{sekcja_autorstwa}
            <hr />
            <p style="text-align: justify;"><strong><span style="color: #808080;">A b s t r a k t</span></strong></p>
            
            {abstrakt}"""
        
        content = browser.find_element('id', 'content').clear()
        content = browser.find_element('id', 'content').send_keys(body)      

        opublikuj = browser.find_element('id', 'publish').click()
        
        caly_link = browser.find_element('id', 'edit-slug-buttons').click()
        odnosnik = browser.find_element('id', 'new-post-slug')
        odnosnik = odnosnik.get_attribute('value')
        aktualny_numer.at[index+1, 'odnosnik'] = odnosnik
        
        url_edycji = browser.current_url
        aktualny_numer.at[index+1, 'url_edycji'] = url_edycji

print('Artykuły na wordpressie opublikowane')

#%% upload new issue at fp.amu.edu.pl

#dane do strony numeru
        
for i, row in aktualny_numer.iterrows():
    tytul = row['tytuł artykułu'].replace('<i>', '</em>').replace('</i>', '<em>')
    autor = row['autor'].replace('❦', ', ')
    body = f"""<p style="text-align: left; margin: 0cm 0cm 15pt; line-height: 15pt; font-size: 11pt; font-family: ChaparralPro-Regular; color: black; letter-spacing: 0.1pt; padding-left: 30px;" align="left"><a href="{row['odnosnik']}">{autor}, <em>{tytul}</em></a></p>"""
    aktualny_numer.at[i, 'spis treści'] = body

#wprowadzanie tagów
    
aktualny_numer.fillna("",inplace=True)

tagi_osob = []
for i, row in aktualny_numer.iterrows():
    if row['język'] == 'pl':
        for a, t_pl, t_eng in zip(row['autor'].split('❦'), row['biogram'].split('❦'), aktualny_numer.at[i+1, 'biogram'].split('❦')):
            tagi_osob.append((row['lp'], a, f"{t_pl}|{t_eng}"))

nowe_tagi_osob = []            
for lp, a, t in tagi_osob:
    browser.get('http://fp.amu.edu.pl/wp-admin/edit-tags.php?taxonomy=post_tag')
    szukaj_tagu = browser.find_element('id', 'tag-search-input').send_keys(a)
    szukaj_tagu_button = browser.find_element('id', 'search-submit').click()
    wybierz_osobe = browser.find_elements('css selector', '.row-title')[0].click()
    opis_tagu = browser.find_element('id', 'description').clear()
    opis_tagu = browser.find_element('id', 'description').send_keys(t)
    zaktualizuj = browser.find_element('xpath', "//input[@class = 'button button-primary' and @value = 'Aktualizuj']").click()
    opis_tagu = browser.find_element('id', 'description').get_attribute('value')
    tag_pl = re.sub('(.+)(\|)(.+)', r'\1', opis_tagu)
    tag_eng = re.sub('(.+)(\|)(.+)', r'\3', opis_tagu)
    nowe_tagi_osob.append((int(lp), a, tag_pl))
    nowe_tagi_osob.append((int(lp)+1, a, tag_eng))
    
nowe_tagi_df = pd.DataFrame(nowe_tagi_osob, columns=['lp', 'autor', 'biogram'])
nowe_tagi_df['autor'] = nowe_tagi_df.groupby('lp')['autor'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
nowe_tagi_df['biogram'] = nowe_tagi_df.groupby('lp')['biogram'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
nowe_tagi_df = nowe_tagi_df.drop_duplicates().reset_index(drop=True)

aktualny_numer['biogram'] = nowe_tagi_df['biogram']

print('Tagi na wordpressie wprowadzone')

#przygotowanie spisu treści

uzyte_kategorie_pl = []
spis_tresci_pl = """"""
uzyte_kategorie_eng = []
spis_tresci_eng = """"""
for i, row in aktualny_numer.iterrows():
    if row['język'] == 'pl':
        if row['kategoria'] not in uzyte_kategorie_pl:
            kategoria_spis_tresci = kategorie_wpisow.copy()[kategorie_wpisow['kategoria'] == row['kategoria']]['spis treści pl'].to_string(index=False).strip()
            spis_tresci_pl += f"{kategoria_spis_tresci}\n"
            uzyte_kategorie_pl.append(row['kategoria'])
        spis_tresci_pl += f"{row['spis treści']}\n"
    elif row['język'] == 'eng':
        if row['kategoria'] not in uzyte_kategorie_eng:
            kategoria_spis_tresci = kategorie_wpisow.copy()[kategorie_wpisow['kategoria'] == row['kategoria']]['spis treści eng'].to_string(index=False).strip()
            spis_tresci_eng += f"{kategoria_spis_tresci}\n"
            uzyte_kategorie_eng.append(row['kategoria'])
        spis_tresci_eng += f"{row['spis treści']}\n"        
        
#wprowadzenie strony nowego numeru

strona_numeru = get_as_dataframe(aktualny_numer_sheet.worksheet('strona'), evaluate_formulas=True).dropna(how='all').dropna(how='all', axis=1)
strona_numeru['spis treści'] = strona_numeru.apply(lambda x: spis_tresci_pl if x['język'] == 'pl' else spis_tresci_eng, axis=1)

for i, row in strona_numeru.iterrows():
    if row['język'] == 'pl':
        sciezka_pdf = min(pdf_pl, key=len)
        sciezka_pdf = re.sub(r'(.+)(\\(?!.*\\))(.+)', r'\3', sciezka_pdf)
        sciezka_jpg = min(jpg_pl, key=len)
        sciezka_jpg = re.sub(r'(.+)(\\(?!.*\\))(.+)', r'\3', sciezka_jpg)
        strona_numeru.at[i, 'link do pdf'] = f"http://fp.amu.edu.pl/wp-content/uploads/{year}/{month}/{sciezka_pdf}"
        strona_numeru.at[i, 'link do jpg'] = f"http://fp.amu.edu.pl/wp-content/uploads/{year}/{month}/{sciezka_jpg}"
        strona_numeru.at[i, 'jpg'] = sciezka_jpg
    elif row['język'] == 'eng':
        sciezka_pdf = min(pdf_eng, key=len)
        sciezka_pdf = re.sub(r'(.+)(\\(?!.*\\))(.+)', r'\3', sciezka_pdf)
        sciezka_jpg = min(jpg_eng, key=len)
        sciezka_jpg = re.sub(r'(.+)(\\(?!.*\\))(.+)', r'\3', sciezka_jpg)
        strona_numeru.at[i, 'link do pdf'] = f"http://fp.amu.edu.pl/wp-content/uploads/{year}/{month}/{sciezka_pdf}"
        strona_numeru.at[i, 'link do jpg'] = f"http://fp.amu.edu.pl/wp-content/uploads/{year}/{month}/{sciezka_jpg}"
        strona_numeru.at[i, 'jpg'] = sciezka_jpg
        
# row = strona_numeru.loc[0]
# index = 0
for index, row in strona_numeru.iterrows():
    if row['język'] == 'pl':
      
        browser.get('http://fp.amu.edu.pl/wp-admin/post-new.php?post_type=page')
        tekstowy = browser.find_element('id', 'content-html').click()

        wprowadz_tytul = browser.find_element('name', 'post_title')
        wprowadz_tytul.send_keys(row['tytuł numeru'])

        jezyk_pl_button = browser.find_element('id', 'xili_language_check_pl_pl').click()
        time.sleep(1)
        wybierz_obrazek = browser.find_element('id', 'set-post-thumbnail').click()
        wybierz_pliki = browser.find_element('xpath', "//input[starts-with(@id,'html5_')]")
        wybierz_pliki.send_keys(f"{row['folder lokalny']}\\{row['jpg']}")
        time.sleep(10)
        zaakceptuj_obrazek = browser.find_element('xpath', "//button[@class = 'button media-button button-primary button-large media-button-select']")
        zaakceptuj_obrazek.click()
        
        body = f"""<a href="{row['link do pdf']}"><img class="alignleft wp-image-3823" src="{row['link do jpg']}" alt="" width="166" height="235" /></a>{row['wstęp']}

        <h3>Spis treści:</h3>
        {row['spis treści']}"""

        content = browser.find_element('id', 'content').send_keys(body)

        opublikuj = browser.find_element('id', 'publish').click()
        
        caly_link = browser.find_element('id', 'edit-slug-buttons').click()
        odnosnik = browser.find_element('id', 'new-post-slug')
        odnosnik = odnosnik.get_attribute('value')
        strona_numeru.at[index, 'odnosnik'] = odnosnik
        
        url_edycji = browser.current_url
        strona_numeru.at[index, 'url_edycji'] = url_edycji

        create_english = browser.find_element('xpath', "//a[@title = 'For create a linked draft translation in en_US']").click()
#English
        tekstowy = browser.find_element('id', 'content-html').click()

        wprowadz_tytul = browser.find_element('name', 'post_title').clear()
        wprowadz_tytul = browser.find_element('name', 'post_title').send_keys(strona_numeru.at[index+1, 'tytuł numeru'])
        
        try:
            usun_obrazek = browser.find_element('id', 'remove-post-thumbnail').click()
        except NoSuchElementException:
            pass
        
        wybierz_obrazek = browser.find_element('id', 'set-post-thumbnail').click()
        wybierz_pliki = browser.find_element('xpath', "//input[starts-with(@id,'html5_')]")
        wybierz_pliki.send_keys(f"{strona_numeru.at[index+1, 'folder lokalny']}\\{strona_numeru.at[index+1, 'jpg']}")
        time.sleep(10)
        zaakceptuj_obrazek = browser.find_element('xpath', "//button[@class = 'button media-button button-primary button-large media-button-select']")
        zaakceptuj_obrazek.click()
        
        body = f"""<a href="{strona_numeru.at[index+1, 'link do pdf']}"><img class="alignleft wp-image-3823" src="{strona_numeru.at[index+1, 'link do jpg']}" alt="" width="166" height="235" /></a>{strona_numeru.at[index+1, 'wstęp']}

        <h3>Table of Contents:</h3>
        {strona_numeru.at[index+1, 'spis treści']}"""
        
        content = browser.find_element('id', 'content').clear()
        content = browser.find_element('id', 'content').send_keys(body)      

        opublikuj = browser.find_element('id', 'publish').click()
        
        caly_link = browser.find_element('id', 'edit-slug-buttons').click()
        odnosnik = browser.find_element('id', 'new-post-slug')
        odnosnik = odnosnik.get_attribute('value')
        strona_numeru.at[index+1, 'odnosnik'] = odnosnik
        
        url_edycji = browser.current_url
        strona_numeru.at[index+1, 'url_edycji'] = url_edycji
       
print('Strona numeru na wordpressie opublikowana')

#%% pressto – upload new issue

#pressto - dodawanie numeru

browser.get("https://pressto.amu.edu.pl/index.php/index/login")
browser.implicitly_wait(5)
username_input = browser.find_element('id', 'login-username')
password_input = browser.find_element('id', 'login-password')

username = fp_credentials.pressto_username
password = fp_credentials.pressto_password

username_input.send_keys(username)
password_input.send_keys(password)

login_button = browser.find_element('css selector', '.btn-primary').click()

browser.get("https://pressto.amu.edu.pl/index.php/fp/manageIssues#futureIssues")

utworz_numer = browser.find_element('css selector', '.pkp_linkaction_icon_add_category').click()
time.sleep(1)
odznacz_tom = browser.find_element('id', 'showVolume').click()
    
wprowadz_numer = browser.find_element('xpath', "//input[@name='number']")
try:
    wprowadz_numer.send_keys('{:.0f}'.format(strona_numeru.at[0, 'numer']))
except ValueError:
    wprowadz_numer.send_keys(strona_numeru.at[0, 'numer'])
rok = browser.find_element('xpath', "//input[@name='year']")
rok.send_keys('{:.0f}'.format(strona_numeru.at[0, 'rok']))
tytul_pl = re.sub('(.+)( \| )(.+)', r'\3', strona_numeru.at[0, 'tytuł numeru']).strip()

try:
    nazwa_numeru = f"Nr {'{:.0f}'.format(strona_numeru.at[0, 'numer'])} ({'{:.0f}'.format(strona_numeru.at[0, 'rok'])}): {tytul_pl}"
except ValueError:
    nazwa_numeru = f"Nr {strona_numeru.at[0, 'numer']} ({'{:.0f}'.format(strona_numeru.at[0, 'rok'])}): {tytul_pl}"

tytul_pl = browser.find_element('xpath', "//input[@name='title[pl_PL]']").send_keys(tytul_pl)
tytul_eng = re.sub('(.+)( \| )(.+)', r'\3', strona_numeru.at[1, 'tytuł numeru']).strip()
tytul_eng = browser.find_element('xpath', "//input[@name='title[en_US]']").send_keys(tytul_eng)

wstep_pl_source = browser.find_elements('xpath', "//i[@class='mce-ico mce-i-code']")[0].click()
wstep_pl_source = browser.find_element('xpath', "//textarea[@class='mce-textbox mce-multiline mce-abs-layout-item mce-first mce-last']").send_keys(strona_numeru.at[0, 'wstęp'])
wstep_pl_ok = browser.find_element('xpath', "//span[contains(text(),'Ok')]").click()

wstep_eng_source = browser.find_elements('xpath', "//i[@class='mce-ico mce-i-code']")[1].click()
wstep_eng_source = browser.find_element('xpath', "//textarea[@class='mce-textbox mce-multiline mce-abs-layout-item mce-first mce-last']").send_keys(strona_numeru.at[1, 'wstęp'])
wstep_eng_ok = browser.find_element('xpath', "//span[contains(text(),'Ok')]").click()

okladka_na_dysku = f"{strona_numeru.at[0, 'folder lokalny']}\{strona_numeru.at[0, 'jpg']}"
przeslij_okladke = browser.find_element('xpath', "//input[@type='file']").send_keys(okladka_na_dysku)
time.sleep(2)
# zapisz_numer = browser.find_element('xpath', "//button[@class='pkp_button submitFormButton']")
zapisz_numer = browser.find_element('xpath', "//button[@name='submitFormButton']")
zapisz_numer.click()

print('Strona numeru na pressto zapisana, ale nie opublikowana')


#%% pressto – upload new articles

#pressto dodawanie artykułów

for i, row in aktualny_numer.iterrows():
#for i, row in aktualny_numer[13:].iterrows(): #jeżeli pętla zostanie przerwana
    print(i)
    # i = 0
    # row = aktualny_numer.iloc[i,:]
    if row['język'] == 'pl':
        nowe_zgloszenie = browser.get('https://pressto.amu.edu.pl/index.php/fp/management/importexport/plugin/QuickSubmitPlugin')
        dzial_fp = row['kategoria']
        dzial = browser.find_element('xpath', f"//select[@id = 'sectionId']/option[text()='{dzial_fp}']").click()
        time.sleep(1)
        pressto_tytul_art_pl = row['tytuł artykułu'].replace('<i>', '').replace('</i>', '')
        pressto_tytul_art_pl = browser.find_element('xpath', "//input[@name='title[pl_PL]']").send_keys(pressto_tytul_art_pl)
        time.sleep(1)
        pressto_tytul_art_eng = aktualny_numer.at[i+1, 'tytuł artykułu'].replace('<i>', '').replace('</i>', '')
        pressto_tytul_art_eng = browser.find_element('xpath', "//input[@name='title[en_US]']").send_keys(pressto_tytul_art_eng)
        
        abstrakt_pl_source = browser.find_elements('xpath', "//i[@class='mce-ico mce-i-code']")[0].click()
        abstrakt_pl_source = browser.find_element('xpath', "//textarea[@class='mce-textbox mce-multiline mce-abs-layout-item mce-first mce-last']").send_keys(row['abstrakt'])
        abstrakt_pl_ok = browser.find_elements('xpath', "//span[contains(text(),'Ok')]")[-1].click()
        abstrakt_eng_source = browser.find_elements('xpath', "//i[@class='mce-ico mce-i-code']")[1].click()
        abstrakt_eng_source = browser.find_element('xpath', "//textarea[@class='mce-textbox mce-multiline mce-abs-layout-item mce-first mce-last']").send_keys(aktualny_numer.at[i+1, 'abstrakt'])
        abstrakt_eng_ok = browser.find_elements('xpath', "//span[contains(text(),'Ok')]")[-1].click()
        kliknij_poza_abstrakt = browser.find_element('xpath', "//input[@name='title[pl_PL]']").click()
        
        metadane_jezyk_pl = browser.find_elements('xpath', "//input[@class='ui-widget-content ui-autocomplete-input']")[0].send_keys('pl')
        metadane_jezyk_eng = browser.find_elements('xpath', "//input[@class='ui-widget-content ui-autocomplete-input']")[1].send_keys('en') 
        for s in row['słowa kluczowe'].split(', '):
            s = re.sub(',+', ',', s)
            slowa_kluczowe_pl = browser.find_elements('xpath', "//input[@class='ui-widget-content ui-autocomplete-input']")[2].send_keys(s, Keys.TAB)
        for s in aktualny_numer.at[i+1, 'słowa kluczowe'].split(', '):
            s = re.sub(',+', ',', s)
            slowa_kluczowe_eng = browser.find_elements('xpath', "//input[@class='ui-widget-content ui-autocomplete-input']")[3].send_keys(s, Keys.TAB)
        
        if pd.notnull(row['finansowanie']):
            try:
                finansowanie_pl = browser.find_elements('xpath', "//input[@class='ui-widget-content ui-autocomplete-input']")[4]
                finansowanie_pl.send_keys(row['finansowanie'])
                finansowanie_en = browser.find_elements('xpath', "//input[@class='ui-widget-content ui-autocomplete-input']")[5]
                finansowanie_en.send_keys(aktualny_numer.at[i+1, 'finansowanie'])
            except (KeyError, TypeError):
                pass
            
        if len(row['bibliografia']) > 0:

            bibliografia_df = pd.DataFrame(row['bibliografia'].split('\n'), columns=['bibliografia'])
            bibliografia_df = bibliografia_df[bibliografia_df['bibliografia'] != '']       
            
            bibliografia_df['autor pozycji'] = bibliografia_df['bibliografia'].apply(lambda x: build_author(x))
            bibliografia_df['autor pozycji'] = bibliografia_df['autor pozycji'].apply(lambda x: jest_autor(x))
            bibliografia_df['ile małych'] = bibliografia_df['autor pozycji'].apply(lambda x: len(re.findall(' \p{Ll}', x)))
            bibliografia_df['ile słów'] = bibliografia_df['autor pozycji'].apply(lambda x: len(x.split(' ')))
            bibliografia_df['odsetek małych'] = bibliografia_df['ile małych'] / bibliografia_df['ile słów']
            bibliografia_df['autor pozycji'] = bibliografia_df.apply(lambda x: re.sub('(^.+)(\..+$)', r'\1', x['autor pozycji']) if x['odsetek małych'] >= 0.3 else x['autor pozycji'], axis=1)
            
            bibliografia_df['autor pozycji'] = bibliografia_df['autor pozycji'].replace('---|- - - |–––|– – –|———|— — —', np.nan, regex=True).ffill()
            
            bibliografia_df['pozycja'] = bibliografia_df.apply(lambda x: biblio_pozycja(x), axis=1)  
            # bibliografia_df['pozycja'] = bibliografia_df.apply(lambda x: re.sub(f"{x['autor pozycji']}.", '', x['bibliografia']).strip(), axis=1)
            bibliografia_df['pozycja'] = bibliografia_df.apply(lambda x: x['bibliografia'] if x['pozycja'] == '' else x['pozycja'], axis=1)
            bibliografia_df['pozycja'] = bibliografia_df['pozycja'].str.replace('[- ]{2,}|[— ]{2,}|[— ]{2,}|[– ]{2,}', '', regex=True)
            bibliografia_df['id'] = bibliografia_df.index+1
            bibliografia_df['id'] = bibliografia_df['id']
            bibliografia_df = bibliografia_df.replace(r'^\s*$', np.nan, regex=True)
            bibliografia_df['bibliografia'] =  bibliografia_df[['autor pozycji', 'pozycja']].apply(lambda x: '. '.join(x.dropna().astype(str)).strip(), axis=1)
            # bibliografia_df['bibliografia'] =  bibliografia_df[['id', 'autor pozycji', 'pozycja']].apply(lambda x: '. '.join(x.dropna().astype(str)).strip(), axis=1)
            bibliografia_df = bibliografia_df['bibliografia']
            bibliografia_df = '\n'.join(bibliografia_df.str.replace('(\. )+', '. ', regex=True).to_list())
            
            bibliografia = browser.find_element('name', 'citationsRaw').send_keys(bibliografia_df, Keys.BACK_SPACE)
            # bibliografia_df = re.sub('\n+', '\n', browser.find_element('name', 'citations').get_attribute('value'))
            # browser.find_element('name', 'citations').clear()
            # bibliografia = browser.find_element('name', 'citations').send_keys(bibliografia_df, Keys.BACK_SPACE)
            time.sleep(2)
        
        for a_pl, a_en, o, af_pl, af_en, b_pl, b_en in zip(row['autor'].split('❦'), aktualny_numer.at[i+1, 'autor'].split('❦'), row['ORCID'].split('❦'), row['afiliacja'].split('❦'), aktualny_numer.at[i+1, 'afiliacja'].split('❦'), row['biogram'].split('❦'), aktualny_numer.at[i+1, 'biogram'].split('❦')):
            
            #a_pl, a_en, o, af_pl, af_en, b_pl, b_en = row['autor'].split('❦')[0], aktualny_numer.at[i+1, 'autor'].split('❦')[0], row['ORCID'].split('❦')[0], row['afiliacja'].split('❦')[0], aktualny_numer.at[i+1, 'afiliacja'].split('❦')[0], row['biogram'].split('❦')[0], aktualny_numer.at[i+1, 'biogram'].split('❦')[0]
            
            # wspolautor_dodaj = browser.find_element('xpath', "//a[@title = 'Dodaj autora']")
            wspolautor_dodaj = browser.find_element('xpath', "//a[contains(text(),'Dodaj autora')]")
            # wspolautor_dodaj = browser.find_element('xpath', "//a[contains(text(),'Dodaj współautora')]")
            wspolautor_dodaj.click()
            autor_imie_pl = re.findall('.+(?= (?!.* ))', a_pl)[0]
            time.sleep(2)
            wprowadz_imie_pl = browser.find_element('name', 'givenName[pl_PL]')
            wprowadz_imie_pl.send_keys(autor_imie_pl)
            autor_imie_en = re.findall('.+(?= (?!.* ))', a_en)[0]
            time.sleep(2)
            wprowadz_imie_en = browser.find_element('name', 'givenName[en_US]')
            wprowadz_imie_en.send_keys(autor_imie_en)
            
            autor_nazwisko_pl = re.findall('(?<= (?!.* )).+', a_pl)[0]
            wprowadz_nazwisko_pl = browser.find_element('name', 'familyName[pl_PL]')
            wprowadz_nazwisko_pl.send_keys(autor_nazwisko_pl)
            time.sleep(2)
            autor_nazwisko_en = re.findall('(?<= (?!.* )).+', a_pl)[0]
            wprowadz_nazwisko_en = browser.find_element('name', 'familyName[en_US]')
            wprowadz_nazwisko_en.send_keys(autor_nazwisko_en)
            
            kontakt = browser.find_element('name', 'email')
            kontakt.send_keys('pressto@amu.edu.pl')
            kraj = browser.find_element('xpath', "//select[@id = 'country']/option[text()='Poland']").click()
            if len(o) > 0:
                orcid = f"https://orcid.org/{o}"
                wprowadz_orcid = browser.find_element('name', 'orcid').send_keys(orcid)
            
            afiliacja_pl = browser.find_elements('xpath', "//input[@class='ui-widget-content ui-autocomplete-input']")[-1]
            # afiliacja_pl = browser.find_element('xpath', "//input[@name='affiliation[pl_PL]']")
            afiliacja_pl.send_keys(af_pl)
            time.sleep(3)
            afiliacja_pl_dropdown = browser.find_elements('xpath', "//li[@class='ui-menu-item']")
            afiliacja_pl_dropdown[0].click()

            time.sleep(2)
            # afiliacja_en = browser.find_element('xpath', "//input[@name='affiliation[en_US]']")
            # afiliacja_en.send_keys(af_en)
            
            biogram_pl = browser.find_elements('xpath', "//i[@class='mce-ico mce-i-code']")[-2].click()
            biogram_pl = browser.find_element('xpath', "//textarea[@class='mce-textbox mce-multiline mce-abs-layout-item mce-first mce-last']")
            biogram_pl.send_keys(b_pl)
            biogram_pl_ok = browser.find_elements('xpath', "//span[contains(text(),'Ok')]")[-1]
            biogram_pl_ok.click()
            time.sleep(2)
            biogram_en = browser.find_elements('xpath', "//i[@class='mce-ico mce-i-code']")[-1].click()
            biogram_en = browser.find_element('xpath', "//textarea[@class='mce-textbox mce-multiline mce-abs-layout-item mce-first mce-last']")
            biogram_en.send_keys(b_en)
            biogram_en_ok = browser.find_elements('xpath', "//span[contains(text(),'Ok')]")[-1]
            biogram_en_ok.click()

            kliknij_poza_biogram = browser.find_element('name', 'orcid').click()
            rola_autora = browser.find_element('xpath', "//input[@name='userGroupId' and @value='14']").click()
            zapisz_autora = browser.find_elements('xpath', "//button[@class = 'pkp_button submitFormButton']")[-1].click()
            time.sleep(2)
   
        # dodaj_plik_pl = browser.find_element('xpath', "//a[@title='Dodaj plik do publikacji']").click()
        dodaj_plik_pl = browser.find_element('xpath', "//a[contains(text(),'Dodaj plik do publikacji')]")
        dodaj_plik_pl.click()
        time.sleep(2)
        etykieta = browser.find_element('xpath', "//input[@class='field text required' and @name = 'label']").send_keys('PDF')
        jezyk_publikacji = browser.find_element('xpath', "//select[@id = 'galleyLocale']/option[text()='Język Polski']").click()
        
        # zapisz = browser.find_elements('xpath', "//button[@class='pkp_button submitFormButton']")
        # zapisz[-1].click()
        zapisz = browser.find_elements('xpath', "//button[@name='submitFormButton']")
        zapisz[-1].click()
        time.sleep(2)
        
        element_artykulu = browser.find_element('xpath', "//select[@id = 'genreId']/option[text()='Tekst artykułu']").click()
        przeslij_pdf = browser.find_element('xpath', "//input[@type='file']")
        przeslij_pdf.send_keys(f"{row['folder lokalny']}\\{row['pdf']}")
        time.sleep(2)
        kontunuuj_button = browser.find_element('id', 'continueButton').click()
        time.sleep(2)
        kontunuuj_button = browser.find_element('id', 'continueButton').click()
        time.sleep(2)
        potwierdz_button = browser.find_element('id', 'continueButton').click()
        time.sleep(2)
        
        # if row['kategoria'] != 'Przekłady' and czy_tlumaczenia_po_angielsku:
        if row['kategoria'] != 'Przekłady':
        
            while True:
                try:
                    # dodaj_plik_eng = browser.find_element('xpath', "//a[@title='Dodaj plik do publikacji']").click()
                    dodaj_plik_pl = browser.find_element('xpath', "//a[contains(text(),'Dodaj plik do publikacji')]")
                    dodaj_plik_pl.click()
                    time.sleep(2)
                except ElementClickInterceptedException:
                    potwierdz_button = browser.find_element('id', 'continueButton').click()
                    time.sleep(2)
                    continue
                break
                
            etykieta = browser.find_element('xpath', "//input[@class='field text required' and @name = 'label']").send_keys('PDF')
            jezyk_publikacji = browser.find_element('xpath', "//select[@id = 'galleyLocale']/option[text()='English']").click()
            
            # zapisz = browser.find_elements('xpath', "//button[@class='pkp_button submitFormButton']")
            # zapisz[-1].click()
            zapisz = browser.find_elements('xpath', "//button[@name='submitFormButton']")
            zapisz[-1].click()
            time.sleep(2)
            
            element_artykulu = browser.find_element('xpath', "//select[@id = 'genreId']/option[text()='Tekst artykułu']").click()
            przeslij_pdf = browser.find_element('xpath', "//input[@type='file']")
            przeslij_pdf.send_keys(f"{aktualny_numer.at[i+1, 'folder lokalny']}\\{aktualny_numer.at[i+1, 'pdf']}")
            time.sleep(2)
            kontunuuj_button = browser.find_element('id', 'continueButton').click()
            time.sleep(2)
            kontunuuj_button = browser.find_element('id', 'continueButton').click()
            time.sleep(2)
            potwierdz_button = browser.find_element('id', 'continueButton').click()
            time.sleep(2)
        
        while True:
                try:
                    zaplanuj_do_publikacji = browser.find_element('id', 'articlePublished').click()
                    time.sleep(2)
                except ElementClickInterceptedException:
                    potwierdz_button = browser.find_element('id', 'continueButton').click()
                    time.sleep(2)
                    continue
                break

        publikuj_w = browser.find_element('xpath', f"//select[@id = 'issueId']/option[text()='{nazwa_numeru}']").click()
        publikuj_strony = browser.find_element('name', 'pages').send_keys(row['strony'])
        
        opublikowany_data = browser.find_element('name', 'datePublished-removed').send_keys(f"{year}-{month}-{day}",Keys.ESCAPE)
        prawa_autorskie_pl = browser.find_element('name', 'copyrightHolder[pl_PL]')
        prawa_autorskie_pl.send_keys(row['autor'].replace('❦', ', '))
        time.sleep(2)
        prawa_autorskie_en = browser.find_element('name', 'copyrightHolder[en_US]')
        prawa_autorskie_en.send_keys(aktualny_numer.at[i+1, 'autor'].replace('❦', ', '))
        rok_praw = browser.find_element('name', 'copyrightYear')
        rok_praw.clear()
        rok_praw.send_keys(int(strona_numeru['rok'][0]))
        
        zapisz = browser.find_elements('xpath', "//button[@class='pkp_button submitFormButton']")
        zapisz[-1].click()
        
browser.close()
print('Artykuły na pressto zapisane, przypięte do numeru, ale nie opublikowane')        


#%%uzupełnienie tabeli artykułów na dysku google

try:
    set_with_dataframe(aktualny_numer_sheet.worksheet('artykuły po pętli'), aktualny_numer)
except gs.WorksheetNotFound:
    aktualny_numer_sheet.add_worksheet(title="artykuły po pętli", rows="100", cols="20")
    set_with_dataframe(aktualny_numer_sheet.worksheet('artykuły po pętli'), aktualny_numer)


#uzupełnienie tabeli numeru na dysku google
try:
    set_with_dataframe(aktualny_numer_sheet.worksheet('strona po pętli'), strona_numeru)
except gs.WorksheetNotFound:
    aktualny_numer_sheet.add_worksheet(title="strona po pętli", rows="100", cols="20")
    set_with_dataframe(aktualny_numer_sheet.worksheet('strona po pętli'), strona_numeru)

worksheets = ['artykuły po pętli', 'strona po pętli']
for worksheet in worksheets:
    worksheet = aktualny_numer_sheet.worksheet(worksheet)
    
    aktualny_numer_sheet.batch_update({
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

print('Metadane na dysku Google zaktualizowane!')
    
# tutaj się kończy pierwsza faza prac
#%% Po informacji od redakcji Pressto: Publikowanie na pressto + DOI
from sickle import Sickle
from tqdm import tqdm

#connect google drive
#autoryzacja do tworzenia i edycji plików
gc = gs.oauth()
#autoryzacja do penetrowania dysku
gauth = GoogleAuth()
gauth.LocalWebserverAuth()
drive = GoogleDrive(gauth)

#sprawdzić ostatnią datę w OAI!!!

sickle = Sickle('http://pressto.amu.edu.pl/index.php/fp/oai')
records = sickle.ListRecords(metadataPrefix='oai_dc')

dates = []
for record in tqdm(records):
    if record.deleted == False:
        record = record.get_metadata()
        record = {k:v for k,v in record.items() if k in ['title', 'creator', 'date', 'identifier', 'source']}
        dates.append(record.get('date')[0])

#pobranie danych z pressto

data_publikacji_na_pressto = input('data publikacji numeru na pressto w formacie YYYY-MM-DD: ')
records = sickle.ListRecords(metadataPrefix='oai_dc')

results = []
for record in tqdm(records):
    if record.deleted == False:
        record = record.get_metadata()
        record = {k:v for k,v in record.items() if k in ['title', 'creator', 'date', 'identifier', 'source']}
        if record.get('date')[0] == data_publikacji_na_pressto:
            results.append(record)

#wczytanie tabeli z aktualnym numerem

file_list = drive.ListFile({'q': f"'{cr_projects}' in parents and trashed=false"}).GetList() 
fp_folder = [file['id'] for file in file_list if file['title'] == 'Forum Poetyki – redakcja'][0]
file_list = drive.ListFile({'q': f"'{fp_folder}' in parents and trashed=false"}).GetList() 
fp_folder = [file['id'] for file in file_list if file['title'] == 'redakcja FP'][0]
file_list = drive.ListFile({'q': f"'{fp_folder}' in parents and trashed=false"}).GetList() 
last_number = max(file_list, key=lambda x: x['createdDate'])
print(f"{last_number['title']}  |  {last_number['id']}")

table_id = input('Podaj id arkusza bieżącego numeru: ')
aktualny_numer_sheet = gc.open_by_key(table_id)

aktualny_numer = get_as_dataframe(aktualny_numer_sheet.worksheet('artykuły po pętli'), evaluate_formulas=True).dropna(how='all').dropna(how='all', axis=1)

if len(results) == len(aktualny_numer)/2:
    print('OK')
else: print('Problem')

#połączenie zasobów

doi_dict = {}
for e in results:
    # e = results[0]
    for el in e.get('title'):
        # el = e.get('title')[0]
        try:
            df = aktualny_numer[aktualny_numer['tytuł artykułu'].str.replace('<i>','').str.replace('</i>','') == el]['url_edycji'].to_list()[0]
            doi_dict.update({df:e.get('identifier')[-1]})
        except IndexError: print(el)
#%% zaktualizowanie danych na stronie FP -- ale najpierw sprwdzić, że dobre metadane pobrało
#uruchomienie przeglądarki i zalogowanie na fp.amu.edu.pl

browser = webdriver.Firefox()

browser.get("http://fp.amu.edu.pl/admin")
browser.implicitly_wait(5)
username_input = browser.find_element('id', 'user_login')
password_input = browser.find_element('id', 'user_pass')

username = fp_credentials.wordpress_username
password = fp_credentials.wordpress_password
time.sleep(1)
username_input.send_keys(username)
password_input.send_keys(password)

login_button = browser.find_element('id', 'wp-submit').click()

#dodanie doi do wordpressa

for k,v in tqdm(doi_dict.items()):

    browser.get(k)
    
    content = browser.find_element('id', 'content').get_attribute('value')
    
    doi_row = f"""<div><strong><strong>DOI:</strong></strong> <a href=" https://doi.org/{v}"> https://doi.org/{v} </a></div>\n"""
    
    body = re.sub('(?=\n {0,}<hr \/>)', doi_row, content)
    content = browser.find_element('id', 'content').clear()
    content = browser.find_element('id', 'content').send_keys(body)
    time.sleep(5)
    opublikuj = browser.find_element('id', 'publish').click()


print('Numery DOI dodane na wordpress!')

browser.close()

#uzupełnienie tabeli na dysku o numery DOI

aktualny_numer['DOI'] = doi_dict.values()
set_with_dataframe(aktualny_numer_sheet.worksheet('artykuły po pętli'), aktualny_numer)

print('Tabela z metadanymi na Google Drive uzupełniona o numery DOI!')

#%% stary kod
# #Publikacja strony numeru na pressto
# time.sleep(1)
# numer_strzalka = browser.find_element('xpath', "//a[@class='show_extras']").click()
# # opublikuj_numer = browser.find_element('xpath', "//a[@title='Opublikuj numer']").click()
# opublikuj_numer = browser.find_element('xpath', "//a[contains(text(),'Opublikuj numer')]")
# opublikuj_numer.click()
# odznacz_mail = browser.find_element('id', 'sendIssueNotification').click()
# # opublikuj_numer_ok = browser.find_element('xpath', "//button[@class='pkp_button submitFormButton']").click()
# opublikuj_numer_ok = browser.find_element('xpath', "//button[@name='submitFormButton']")
# opublikuj_numer_ok.click()

# print('Strona numeru na pressto opublikowana')

# ###

# #Przypisanie tekstu do numeru i publikacja
        
#     	while True:
#                 try:
#                     breakzaplanuj_do_publikacji = browser.find_element('id', 'articlePublished').click()
#                     time.sleep(2)
#                 except ElementClickInterceptedException:
#                     potwierdz_button = browser.find_element('id', 'continueButton').click()
#                     time.sleep(2)
#                     continue
#                 break

#         publikuj_w = browser.find_element('xpath', f"//select[@id = 'issueId']/option[text()='{nazwa_numeru}']").click()
#         publikuj_strony = browser.find_element('name', 'pages').send_keys(row['strony'])
        
#         opublikowany_data = browser.find_element('name', 'datePublished-removed').send_keys(f"{year}-{month}-{day}",Keys.ESCAPE)
#         prawa_autorskie_pl = browser.find_element('name', 'copyrightHolder[pl_PL]')
#         prawa_autorskie_pl.send_keys(row['autor'].replace('❦', ', '))
#         time.sleep(2)
#         prawa_autorskie_en = browser.find_element('name', 'copyrightHolder[en_US]')
#         prawa_autorskie_en.send_keys(aktualny_numer.at[i+1, 'autor'].replace('❦', ', '))
#         rok_praw = browser.find_element('name', 'copyrightYear').send_keys(year)        
        
#         zapisz = browser.find_elements('xpath', "//button[@class='pkp_button submitFormButton']")
#         zapisz[-1].click()
        
#         wroc_do_zgloszenia = browser.find_element('xpath', "//a[contains(text(),'Idź do zgłoszenia')]").click()
#         # metadane = browser.find_element('xpath', "//a[@title = 'Wyświetl metadane zgłoszenia']").click()
#         publikacja = browser.find_element('xpath', "//button[@id = 'publication-button']")
#         time.sleep(2)
#         publikacja.click()
#         nowe_metadane = browser.find_element('xpath', "//button[contains(text(),'Stwórz nową wersję metadanych')]")
#         time.sleep(2)
#         nowe_metadane.click()
#         nowe_metadane_tak = browser.find_element('xpath', "//button[contains(text(),'Tak')]")
#         time.sleep(2)
#         nowe_metadane_tak.click()
#         # identyfikatory = browser.find_element('xpath', "//a[@name = 'catalog' and @class = 'ui-tabs-anchor']").click()
#         identyfikatory = browser.find_element('xpath', "//button[@id = 'identifiers-button']")
#         time.sleep(2)
#         identyfikatory.click()
#         # doi_artykulu = browser.find_element('xpath', "//p[contains(text(), 'fp')]").text
#         przydziel_doi = browser.find_element('xpath', "//button[contains(text(),'Przydziel')]")
#         time.sleep(2)
#         przydziel_doi.click()
#         # doi_artykulu = browser.find_element('xpath', "//input[contains(text(), 'fp')]").text
#         przydziel_doi_zapisz = browser.find_elements('css selector', ".pkpFormPage__buttons .pkpButton")
#         przydziel_doi_zapisz[3].click()
#         doi_artykulu = browser.find_element('xpath', "//input[@name = 'pub-id::doi']").get_attribute('value')
#         # zapisz = browser.find_elements('xpath', "//button[@class='pkp_button submitFormButton']")
#         time.sleep(2)
#         # zapisz[1].click()
#         # time.sleep(2)
#         bibliografia = browser.find_element('xpath', "//button[@id = 'citations-button']")
#         # bibliografia = browser.find_element('xpath', "//a[@name = 'citations' and @class = 'ui-tabs-anchor']")
#         time.sleep(2)
#         bibliografia.click()
#         bibliografia_zapisz = browser.find_elements('css selector', ".pkpFormPage__buttons .pkpButton")
#         time.sleep(2)
#         bibliografia_zapisz[2].click()
#         time.sleep(2)
#         # zapisz = browser.find_elements('xpath', "//button[@class='pkp_button parse']")
#         opublikuj = browser.find_element('xpath', "//button[contains(text(),'Opublikuj')]")
#         time.sleep(2)
#         opublikuj.click()
#         opublikuj = browser.find_element('xpath', "//button[@label = 'Opublikuj']")
#         time.sleep(2)
#         opublikuj.click()
#         time.sleep(2)
        
# #dodanie doi do wordpressa
#         browser.get(row['url_edycji'])
        
#         content = browser.find_element('id', 'content').get_attribute('value')
        
#         doi_row = f"""<div><strong><strong>DOI:</strong></strong> <a href=" https://doi.org/{doi_artykulu}"> https://doi.org/{doi_artykulu} </a></div>\n"""
        
#         body = re.sub('(?=\n {0,}<hr \/>)', doi_row, content)
#         content = browser.find_element('id', 'content').clear()
#         content = browser.find_element('id', 'content').send_keys(body)
        
#         opublikuj = browser.find_element('id', 'publish').click()
        
#         browser.get(aktualny_numer.at[i+1, 'url_edycji'])
#         content = browser.find_element('id', 'content').get_attribute('value')
#         body = re.sub('(?=\n {0,}<hr \/>)', doi_row, content)
        
#         content = browser.find_element('id', 'content').clear()
#         content = browser.find_element('id', 'content').send_keys(body)
        
#         opublikuj = browser.find_element('id', 'publish').click()


# print('Nowy numer opublikowany!')
















