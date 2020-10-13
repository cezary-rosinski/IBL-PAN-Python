from my_functions import gsheet_to_df, df_to_gsheet
import pandas as pd
import re
import regex
import glob
from unidecode import unidecode
import datetime
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import fp_credentials
import time
from selenium.common.exceptions import NoSuchElementException, ElementNotInteractableException, NoAlertPresentException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

pd.options.display.max_colwidth = 10000

now = datetime.datetime.now()
year = now.year
month = now.month


table_url = input('Podaj link do arkusza bieżącego numeru: ')
gs_table = re.findall('(?<=https:\/\/docs\.google\.com\/spreadsheets\/d\/).+(?=\/)', table_url)[0]

aktualny_numer = gsheet_to_df(gs_table, 'artykuły')
kategorie_wpisow = gsheet_to_df('1hPVa854YZ4DIajzVoWshXFotd3A7lHHz_d-GqEgn4BI', 'Arkusz1')

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
            sciezka_jpg = [e for e in jpg_pl if nazwisko in e and slowo_z_tytulu in e.lower()][0]
        except IndexError:
            sciezka_jpg = [e for e in jpg_pl if nazwisko in e][0]
        sciezka_jpg = re.sub(r'(.+)(\\(?!.*\\))(.+)', r'\3', sciezka_jpg)
        try:
            sciezka_pdf = [e for e in pdf_pl if nazwisko in e and slowo_z_tytulu in e.lower()][0]
        except IndexError:
            sciezka_pdf = [e for e in pdf_pl if nazwisko in e][0]
        sciezka_pdf = re.sub(r'(.+)(\\(?!.*\\))(.+)', r'\3', sciezka_pdf)
        aktualny_numer.at[i, 'link do pdf'] = f"http://fp.amu.edu.pl/wp-content/uploads/{year}/{month}/{sciezka_pdf}"
        aktualny_numer.at[i, 'link do jpg'] = f"http://fp.amu.edu.pl/wp-content/uploads/{year}/{month}/{sciezka_jpg}"
        aktualny_numer.at[i, 'pdf'] = sciezka_pdf
        aktualny_numer.at[i, 'jpg'] = sciezka_jpg
    elif row['język'] == 'eng':
        try:
            sciezka_jpg = [e for e in jpg_eng if nazwisko in e and slowo_z_tytulu in e.lower()][0]
        except IndexError:
            sciezka_jpg = [e for e in jpg_eng if nazwisko in e][0]
        sciezka_jpg = re.sub(r'(.+)(\\(?!.*\\))(.+)', r'\3', sciezka_jpg)
        try:
            sciezka_pdf = [e for e in pdf_eng if nazwisko in e and slowo_z_tytulu in e.lower()][0]
        except IndexError:
            sciezka_pdf = [e for e in pdf_eng if nazwisko in e][0]
        sciezka_pdf = re.sub(r'(.+)(\\(?!.*\\))(.+)', r'\3', sciezka_pdf)
        aktualny_numer.at[i, 'link do pdf'] = f"http://fp.amu.edu.pl/wp-content/uploads/{year}/{month}/{sciezka_pdf}"
        aktualny_numer.at[i, 'link do jpg'] = f"http://fp.amu.edu.pl/wp-content/uploads/{year}/{month}/{sciezka_jpg}"
        aktualny_numer.at[i, 'pdf'] = sciezka_pdf
        aktualny_numer.at[i, 'jpg'] = sciezka_jpg

#wprowadzanie nowych wpisów
        
browser = webdriver.Chrome()
browser.get("http://fp.amu.edu.pl/admin")    
browser.implicitly_wait(5)
username_input = browser.find_element_by_id('user_login')
password_input = browser.find_element_by_id('user_pass')

username = fp_credentials.wordpress_username
password = fp_credentials.wordpress_password

username_input.send_keys(username)
password_input.send_keys(password)

login_button = browser.find_element_by_id('wp-submit').click()

for index, row in aktualny_numer.iterrows():
    if row['język'] == 'pl':
      
        browser.get('http://fp.amu.edu.pl/wp-admin/post-new.php')
        tekstowy = browser.find_element_by_id('content-html').click()

        wprowadz_tytul = browser.find_element_by_name('post_title')
        wprowadz_tytul.send_keys(row['tytuł artykułu'])

        jezyk_pl_button = browser.find_element_by_id('xili_language_check_pl_pl').click()

        kategoria_id = kategorie_wpisow.copy()[kategorie_wpisow['kategoria'] == row['kategoria']]['id'].to_string(index=False).strip()
        time.sleep(1)
        kategoria = browser.find_element_by_id(kategoria_id)
        time.sleep(1)
        if kategoria.get_attribute('id') == kategoria_id:
            kategoria.click()
        else:
            print(f"Błąd w artytule o tytule: {row['tytuł artykułu']}")

        tagi = browser.find_element_by_id('new-tag-post_tag')
        tagi_wpisu = ','.join(row['autor'].split('❦')) + ',' + row['tag numeru']
        tagi.send_keys(tagi_wpisu)
        dodaj_tagi = browser.find_element_by_xpath("//input[@class = 'button tagadd']").click()
        
        wybierz_obrazek = browser.find_element_by_id('set-post-thumbnail').click()
        
        while True:
            try:
                search_box = browser.find_element_by_id('mla-media-search-input').clear()
                search_box = browser.find_element_by_id('mla-media-search-input')
                search_box.send_keys(row['jpg'])
                search_button = browser.find_element_by_id('mla-search-submit').click()
                time.sleep(2)
                znajdz_obrazek = browser.find_element_by_css_selector('.thumbnail').click()
                time.sleep(2)
                zaakceptuj_obrazek = browser.find_element_by_xpath("//button[@class = 'button media-button button-primary button-large media-button-select']").click()
                czy_obrazek = browser.find_element_by_xpath("//img[@class = 'attachment-post-thumbnail size-post-thumbnail']")
            except NoSuchElementException:
                time.sleep(5)
                continue
            break

        wybierz_pdf = browser.find_element_by_xpath("//select[@id = 'metakeyselect']/option[text()='pdf-url']").click()
        wprowadz_pdf_link = browser.find_element_by_id('metavalue').send_keys(row['link do pdf'])
        dodaj_pdf = browser.find_element_by_id("newmeta-submit").click()
        
        if row['kategoria'] == 'Przekłady' and row['ORCID'] == '':
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

        content = browser.find_element_by_id('content').send_keys(body)

        opublikuj = browser.find_element_by_id('publish').click()
        
        caly_link = browser.find_element_by_id('edit-slug-buttons').click()
        odnosnik = browser.find_element_by_id('new-post-slug')
        odnosnik = odnosnik.get_attribute('value')
        aktualny_numer.at[index, 'odnosnik'] = odnosnik
        
        url_edycji = browser.current_url
        aktualny_numer.at[index, 'url_edycji'] = url_edycji

        create_english = browser.find_element_by_xpath("//a[@title = 'For create a linked draft translation in en_US']").click()
#English
        tekstowy = browser.find_element_by_id('content-html').click()

        wprowadz_tytul = browser.find_element_by_name('post_title').clear()
        wprowadz_tytul = browser.find_element_by_name('post_title').send_keys(aktualny_numer.at[index+1, 'tytuł artykułu'])
        
        tagi = browser.find_element_by_id('new-tag-post_tag')
        tagi_wpisu = ','.join(aktualny_numer.at[index+1, 'autor'].split('❦')) + ',' + aktualny_numer.at[index+1, 'tag numeru']
        tagi.send_keys(tagi_wpisu)
        dodaj_tagi = browser.find_element_by_xpath("//input[@class = 'button tagadd']").click()
        
        try:
            usun_obrazek = browser.find_element_by_id('remove-post-thumbnail').click()
        except NoSuchElementException:
            pass
        
        wybierz_obrazek = browser.find_element_by_id('set-post-thumbnail').click()
        
        while True:
            try:
                search_box = browser.find_element_by_id('mla-media-search-input').clear()
                search_box = browser.find_element_by_id('mla-media-search-input')
                search_box.send_keys(aktualny_numer.at[index+1, 'jpg'])
                search_button = browser.find_element_by_id('mla-search-submit').click()
                time.sleep(2)
                znajdz_obrazek = browser.find_element_by_css_selector('.thumbnail').click()
                time.sleep(2)
                zaakceptuj_obrazek = browser.find_element_by_xpath("//button[@class = 'button media-button button-primary button-large media-button-select']").click()
                czy_obrazek = browser.find_element_by_xpath("//img[@class = 'attachment-post-thumbnail size-post-thumbnail']")
            except NoSuchElementException:
                time.sleep(5)
                continue
            break
        
        wybierz_pdf = browser.find_element_by_xpath("//select[@id = 'metakeyselect']/option[text()='pdf-url']").click()
        wprowadz_pdf_link = browser.find_element_by_id('metavalue').send_keys(aktualny_numer.at[index+1, 'link do pdf'])
        dodaj_pdf = browser.find_element_by_id("newmeta-submit").click()
        
        if aktualny_numer.at[index+1, 'kategoria'] == 'Przekłady' and aktualny_numer.at[index+1, 'ORCID'] == '':
            tagi_autorow = aktualny_numer.at[index+1, 'tag autora'].split('❦')
            autorzy = aktualny_numer.at[index+1, 'autor'].split('❦')
            
            sekcja_autorstwa = """"""

            for t, a in zip(tagi_autorow, autorzy):
                tag_autor_orcid_line = f"""<h4><a href="http://fp.amu.edu.pl/tag/{t}">{a}</a></h4>\n"""
                sekcja_autorstwa += tag_autor_orcid_line
                
            abstrakt = aktualny_numer.at[index+1, 'abstrakt']
                
            body = f"""{sekcja_autorstwa}
            <hr />
            
            {abstrakt}"""
        elif aktualny_numer.at[index+1, 'kategoria'] == 'Przekłady':
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
        
        content = browser.find_element_by_id('content').clear()
        content = browser.find_element_by_id('content').send_keys(body)      

        opublikuj = browser.find_element_by_id('publish').click()
        
        caly_link = browser.find_element_by_id('edit-slug-buttons').click()
        odnosnik = browser.find_element_by_id('new-post-slug')
        odnosnik = odnosnik.get_attribute('value')
        aktualny_numer.at[index+1, 'odnosnik'] = odnosnik
        
        url_edycji = browser.current_url
        aktualny_numer.at[index+1, 'url_edycji'] = url_edycji

print('Artykuły na wordpressie opublikowane')

#dane do strony numeru
        
for i, row in aktualny_numer.iterrows():
    tytul = row['tytuł artykułu'].replace('<i>', '</em>').replace('</i>', '<em>')
    autor = row['autor'].replace('❦', ', ')
    body = f"""<p style="text-align: left; margin: 0cm 0cm 15pt; line-height: 15pt; font-size: 11pt; font-family: ChaparralPro-Regular; color: black; letter-spacing: 0.1pt; padding-left: 30px;" align="left"><a href="{row['odnosnik']}">{autor}, <em>{tytul}</em></a></p>"""
    aktualny_numer.at[i, 'spis treści'] = body
        
#uzupełnienie tabeli na dysku google

df_to_gsheet(aktualny_numer, gs_table, 'artykuły po pętli')

#wprowadzanie tagów

tagi_osob = []
for i, row in aktualny_numer.iterrows():
    if row['język'] == 'pl':
        for a, t_pl, t_eng in zip(row['autor'].split('❦'), row['biografia'].split('❦'), aktualny_numer.at[i+1, 'biografia'].split('❦')):
            tagi_osob.append((a, f"{t_pl}|{t_eng}"))
            
for a, t in tagi_osob:
    browser.get('http://fp.amu.edu.pl/wp-admin/edit-tags.php?taxonomy=post_tag')
    szukaj_tagu = browser.find_element_by_id('tag-search-input').send_keys(a)
    szukaj_tagu_button = browser.find_element_by_id('search-submit').click()
    wybierz_osobe = browser.find_elements_by_css_selector('.row-title')[0].click()
    opis_tagu = browser.find_element_by_id('description').clear()
    opis_tagu = browser.find_element_by_id('description').send_keys(t)
    zaktualizuj = browser.find_element_by_xpath("//input[@class = 'button button-primary' and @value = 'Zaktualizuj']").click()

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

strona_numeru = gsheet_to_df(gs_table, 'strona')
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
        
row = strona_numeru.loc[0]
index = 0
for index, row in strona_numeru.iterrows():
    if row['język'] == 'pl':
      
        browser.get('http://fp.amu.edu.pl/wp-admin/post-new.php?post_type=page')
        tekstowy = browser.find_element_by_id('content-html').click()

        wprowadz_tytul = browser.find_element_by_name('post_title')
        wprowadz_tytul.send_keys(row['tytuł numeru'])

        jezyk_pl_button = browser.find_element_by_id('xili_language_check_pl_pl').click()
        time.sleep(1)
        wybierz_obrazek = browser.find_element_by_id('set-post-thumbnail').click()
        
        while True:
            try:
                search_box = browser.find_element_by_id('mla-media-search-input').clear()
                search_box = browser.find_element_by_id('mla-media-search-input')
                search_box.send_keys(row['jpg'])
                search_button = browser.find_element_by_id('mla-search-submit').click()
                time.sleep(2)
                znajdz_obrazek = browser.find_element_by_css_selector('.thumbnail').click()
                time.sleep(2)
                zaakceptuj_obrazek = browser.find_element_by_xpath("//button[@class = 'button media-button button-primary button-large media-button-select']").click()
                czy_obrazek = browser.find_element_by_xpath("//img[@class = 'attachment-post-thumbnail size-post-thumbnail']")
            except NoSuchElementException:
                time.sleep(5)
                continue
            break
        
        body = f"""<a href="{row['link do pdf']}"><img class="alignleft wp-image-3823" src="{row['link do jpg']}" alt="" width="166" height="235" /></a>{row['wstęp']}

        <h3>Spis treści:</h3>
        {row['spis treści']}"""

        content = browser.find_element_by_id('content').send_keys(body)

        opublikuj = browser.find_element_by_id('publish').click()
        
        caly_link = browser.find_element_by_id('edit-slug-buttons').click()
        odnosnik = browser.find_element_by_id('new-post-slug')
        odnosnik = odnosnik.get_attribute('value')
        strona_numeru.at[index, 'odnosnik'] = odnosnik
        
        url_edycji = browser.current_url
        strona_numeru.at[index, 'url_edycji'] = url_edycji

        create_english = browser.find_element_by_xpath("//a[@title = 'For create a linked draft translation in en_US']").click()
#English
        tekstowy = browser.find_element_by_id('content-html').click()

        wprowadz_tytul = browser.find_element_by_name('post_title').clear()
        wprowadz_tytul = browser.find_element_by_name('post_title').send_keys(strona_numeru.at[index+1, 'tytuł numeru'])
        
        try:
            usun_obrazek = browser.find_element_by_id('remove-post-thumbnail').click()
        except NoSuchElementException:
            pass
        
        wybierz_obrazek = browser.find_element_by_id('set-post-thumbnail').click()
        
        while True:
            try:
                search_box = browser.find_element_by_id('mla-media-search-input').clear()
                search_box = browser.find_element_by_id('mla-media-search-input')
                search_box.send_keys(strona_numeru.at[index+1, 'jpg'])
                search_button = browser.find_element_by_id('mla-search-submit').click()
                time.sleep(2)
                znajdz_obrazek = browser.find_element_by_css_selector('.thumbnail').click()
                time.sleep(2)
                zaakceptuj_obrazek = browser.find_element_by_xpath("//button[@class = 'button media-button button-primary button-large media-button-select']").click()
                czy_obrazek = browser.find_element_by_xpath("//img[@class = 'attachment-post-thumbnail size-post-thumbnail']")
            except NoSuchElementException:
                time.sleep(5)
                continue
            break
        
        body = f"""<a href="{strona_numeru.at[index+1, 'link do pdf']}"><img class="alignleft wp-image-3823" src="{strona_numeru.at[index+1, 'link do jpg']}" alt="" width="166" height="235" /></a>{strona_numeru.at[index+1, 'wstęp']}

        <h3>Table of Contents:</h3>
        {strona_numeru.at[index+1, 'spis treści']}"""
        
        content = browser.find_element_by_id('content').clear()
        content = browser.find_element_by_id('content').send_keys(body)      

        opublikuj = browser.find_element_by_id('publish').click()
        
        caly_link = browser.find_element_by_id('edit-slug-buttons').click()
        odnosnik = browser.find_element_by_id('new-post-slug')
        odnosnik = odnosnik.get_attribute('value')
        strona_numeru.at[index+1, 'odnosnik'] = odnosnik
        
        url_edycji = browser.current_url
        strona_numeru.at[index+1, 'url_edycji'] = url_edycji
       
df_to_gsheet(strona_numeru, gs_table, 'strona po pętli')

print('Strona numeru na wordpressie opublikowana')

#pressto - dodawanie numeru

browser.get("https://pressto.amu.edu.pl/index.php/index/login") 
browser.implicitly_wait(5)
username_input = browser.find_element_by_id('login-username')
password_input = browser.find_element_by_id('login-password')

username = fp_credentials.pressto_username
password = fp_credentials.pressto_password

username_input.send_keys(username)
password_input.send_keys(password)

login_button = browser.find_element_by_css_selector('.btn-primary').click()

browser.get("https://pressto.amu.edu.pl/index.php/fp/manageIssues#futureIssues")

utworz_numer = browser.find_element_by_css_selector('.pkp_linkaction_icon_add_category').click()
time.sleep(1)
odznacz_numer = browser.find_element_by_id('showNumber').click()

tom = browser.find_element_by_xpath("//input[@name='volume']").send_keys(strona_numeru.at[0, 'numer'])
rok = browser.find_element_by_xpath("//input[@name='year']").send_keys(strona_numeru.at[0, 'rok'])
tytul_pl = re.sub('(.+)( \| )(.+)', r'\3', strona_numeru.at[0, 'tytuł numeru']).strip()

nazwa_numeru = f"Tom {strona_numeru.at[0, 'numer']} ({strona_numeru.at[0, 'rok']}): {tytul_pl}"

tytul_pl = browser.find_element_by_xpath("//input[@name='title[pl_PL]']").send_keys(tytul_pl)
tytul_eng = re.sub('(.+)( \| )(.+)', r'\3', strona_numeru.at[1, 'tytuł numeru']).strip()
tytul_eng = browser.find_element_by_xpath("//input[@name='title[en_US]']").send_keys(tytul_eng)

wstep_pl_source = browser.find_elements_by_xpath("//i[@class='mce-ico mce-i-code']")[0].click()
wstep_pl_source = browser.find_element_by_xpath("//textarea[@class='mce-textbox mce-multiline mce-abs-layout-item mce-first mce-last']").send_keys(strona_numeru.at[0, 'wstęp'])
wstep_pl_ok = browser.find_element_by_xpath("//span[contains(text(),'Ok')]").click()

wstep_eng_source = browser.find_elements_by_xpath("//i[@class='mce-ico mce-i-code']")[1].click()
wstep_eng_source = browser.find_element_by_xpath("//textarea[@class='mce-textbox mce-multiline mce-abs-layout-item mce-first mce-last']").send_keys(strona_numeru.at[1, 'wstęp'])
wstep_eng_ok = browser.find_element_by_xpath("//span[contains(text(),'Ok')]").click()

okladka_na_dysku = f"{strona_numeru.at[0, 'folder lokalny']}\{strona_numeru.at[0, 'jpg']}"
przeslij_okladke = browser.find_element_by_xpath("//input[@type='file']").send_keys(okladka_na_dysku)
time.sleep(2)
zapisz_numer = browser.find_element_by_xpath("//button[@class='pkp_button submitFormButton']").click()
time.sleep(1)
numer_strzalka = browser.find_element_by_xpath("//a[@class='show_extras']").click()
opublikuj_numer = browser.find_element_by_xpath("//a[@title='Opublikuj numer']").click()
odznacz_mail = browser.find_element_by_id('sendIssueNotification').click()
opublikuj_numer_ok = browser.find_element_by_xpath("//button[@class='pkp_button submitFormButton']").click()

print('Strona numeru na pressto opublikowana')

#pressto dodawanie artykułów

for i, row in aktualny_numer.iterrows():
    if row['język'] == 'pl':
        nowe_zgloszenie = browser.get('https://pressto.amu.edu.pl/index.php/fp/submission/wizard')
        dzial_fp = row['kategoria']
        dzial = browser.find_element_by_xpath(f"//select[@id = 'sectionId']/option[text()='{dzial_fp}']").click()
        time.sleep(1)
        
        check_box_nr = 0
        while True:
            try:
                check_box_wymagania = browser.find_elements_by_xpath("//input[@type='checkbox']")[check_box_nr]
                check_box_wymagania.send_keys(Keys.SPACE, Keys.TAB, Keys.SPACE, Keys.TAB, Keys.SPACE, Keys.TAB, Keys.SPACE, Keys.TAB, Keys.SPACE, Keys.TAB, Keys.SPACE)
            except ElementNotInteractableException:
                check_box_nr += 1
                continue
            break
        check_box_oswiadczenia = browser.find_element_by_name('copyrightNoticeAgree').send_keys(Keys.SPACE, Keys.TAB, Keys.SPACE)
        
        zapisz_kontynuuj = browser.find_element_by_xpath("//button[@class = 'pkp_button submitFormButton']").click()
        
        element_artykulu = browser.find_element_by_xpath("//select[@id = 'genreId']/option[text()='Tekst artykułu']").click()
        pdf_pl_na_dysku = f"{row['folder lokalny']}\{row['pdf']}"
        przeslij_pdf = browser.find_element_by_xpath("//input[@type='file']").send_keys(pdf_pl_na_dysku)
        time.sleep(2)
        kontynuuj = browser.find_element_by_id('continueButton').click()
        time.sleep(2)
        kontynuuj = browser.find_element_by_id('continueButton').click()
        time.sleep(2)
        dodaj_kolejny_pdf = browser.find_element_by_name('newFile').click()
        time.sleep(2)
        wersja_artykulu = browser.find_element_by_xpath("//select[@id = 'revisedFileId']/option[text()='To nie jest nowa wersja istniejącego pliku']").click()
        element_artykulu = browser.find_element_by_xpath("//select[@id = 'genreId']/option[text()='Tekst artykułu']").click()
        pdf_eng_na_dysku = f"{aktualny_numer.at[i+1, 'folder lokalny']}\{aktualny_numer.at[i+1, 'pdf']}"
        przeslij_pdf = browser.find_element_by_xpath("//input[@type='file']").send_keys(pdf_eng_na_dysku)
        time.sleep(1)
        wersja_artykulu = browser.find_element_by_xpath("//select[@id = 'revisedFileId']/option[text()='To nie jest nowa wersja istniejącego pliku']").click()
        time.sleep(2)
        kontynuuj = browser.find_element_by_id('continueButton').click()
        time.sleep(2)
        kontynuuj = browser.find_element_by_id('continueButton').click()
        time.sleep(2)
        dokoncz = browser.find_element_by_id('continueButton').click()
        
        zapisz_kontynuuj = browser.find_elements_by_xpath("//button[@class='pkp_button submitFormButton']")
        zapisz_kontynuuj[1].click()
        
        pressto_tytul_art_pl = row['tytuł artykułu'].replace('<i>', '').replace('</i>', '')
        pressto_tytul_art_pl = browser.find_element_by_xpath("//input[@name='title[pl_PL]']").send_keys(pressto_tytul_art_pl)
        pressto_tytul_art_eng = aktualny_numer.at[i+1, 'tytuł artykułu'].replace('<i>', '').replace('</i>', '')
        pressto_tytul_art_eng = browser.find_element_by_xpath("//input[@name='title[en_US]']").send_keys(pressto_tytul_art_eng)
        
        abstrakt_pl_source = browser.find_elements_by_xpath("//i[@class='mce-ico mce-i-code']")[0].click()
        abstrakt_pl_source = browser.find_element_by_xpath("//textarea[@class='mce-textbox mce-multiline mce-abs-layout-item mce-first mce-last']").send_keys(row['abstrakt'])
        abstrakt_pl_ok = browser.find_element_by_xpath("//span[contains(text(),'Ok')]").click()
        
        abstrakt_eng_source = browser.find_elements_by_xpath("//i[@class='mce-ico mce-i-code']")[1].click()
        abstrakt_eng_source = browser.find_element_by_xpath("//textarea[@class='mce-textbox mce-multiline mce-abs-layout-item mce-first mce-last']").send_keys(aktualny_numer.at[i+1, 'abstrakt'])
        abstrakt_eng_ok = browser.find_element_by_xpath("//span[contains(text(),'Ok')]").click()
        
        kliknij_poza_abstrakt = browser.find_element_by_xpath("//input[@name='title[pl_PL]']").click()
        
        wspolautor_strzalka = browser.find_elements_by_xpath("//a[@class='show_extras']")[-1].click()
        wspolautor_usun = browser.find_element_by_xpath("//a[@title='Usuń autora']").click()
        time.sleep(1)
        wspolautor_usun_ok = browser.find_element_by_xpath("//a[@class='ok pkpModalConfirmButton']").click()
        for a, o in zip(row['autor'].split('❦'), row['ORCID'].split('❦')):
            wspolautor_dodaj = browser.find_element_by_xpath("//a[@title = 'Dodaj autora']").click()
            autor_imie = re.findall('.+(?= (?!.* ))', a)[0]
            wprowadz_imie = browser.find_element_by_name('givenName[pl_PL]').send_keys(autor_imie)
            autor_nazwisko = re.findall('(?<= (?!.* )).+', a)[0]
            wprowadz_nazwisko = browser.find_element_by_name('familyName[pl_PL]').send_keys(autor_nazwisko)
            kontakt = browser.find_element_by_name('email').send_keys('pressto@amu.edu.pl')
            kraj = browser.find_element_by_xpath("//select[@id = 'country']/option[text()='Polska']").click()
            orcid = f"https://orcid.org/{o}"
            wprowadz_orcid = browser.find_element_by_name('orcid').send_keys(orcid)
            rola_autora = browser.find_element_by_xpath("//input[@name='userGroupId' and @value='14']").click()
            zapisz_autora = browser.find_elements_by_xpath("//button[@class = 'pkp_button submitFormButton']")[-1].click()
# czy dodawać tłumacza?
        metadane_jezyk_pl = browser.find_elements_by_xpath("//input[@class='ui-widget-content ui-autocomplete-input']")[0].send_keys('Język Polski')
        metadane_jezyk_eng = browser.find_elements_by_xpath("//input[@class='ui-widget-content ui-autocomplete-input']")[1].send_keys('English') 
        for s in row['słowa kluczowe'].split(', '):
            slowa_kluczowe_pl = browser.find_elements_by_xpath("//input[@class='ui-widget-content ui-autocomplete-input']")[2].send_keys(s, Keys.TAB)
        for s in aktualny_numer.at[i+1, 'słowa kluczowe'].split(', '):
            slowa_kluczowe_eng = browser.find_elements_by_xpath("//input[@class='ui-widget-content ui-autocomplete-input']")[3].send_keys(s, Keys.TAB)
        bibliografia = browser.find_element_by_name('citations').send_keys(row['bibliografia'])
        zapisz_kontynuuj = browser.find_elements_by_xpath("//button[@class='pkp_button submitFormButton']")
        zapisz_kontynuuj[-1].click()
        time.sleep(1)
        zakoncz_zglaszanie = browser.find_elements_by_xpath("//button[@class='pkp_button submitFormButton']")
        zakoncz_zglaszanie[-1].click()
        zakoncz_zglaszanie_ok = browser.find_element_by_xpath("//a[@class='ok pkpModalConfirmButton']").click()
        time.sleep(5)
        url_zgloszenia = browser.current_url
        zgloszenie_id = re.findall('(?<=submissionId=)\d+', url_zgloszenia)[0]
        
        browser.get(f"https://pressto.amu.edu.pl/index.php/fp/workflow/index/{zgloszenie_id}/1")
        
        try:
            browser.switch_to_alert().accept()
        except NoAlertPresentException:
            pass
        
        realizacja = browser.find_element_by_xpath("//a[contains(text(),'Realizacja')]").click()
        dodaj_plik_pl = browser.find_element_by_xpath("//a[@title='Dodaj plik do publikacji']").click()
        time.sleep(2)
        etykieta = browser.find_element_by_xpath("//input[@class='field text required' and @name = 'label']").send_keys('PDF')
        jezyk_publikacji = browser.find_element_by_xpath("//select[@id = 'galleyLocale']/option[text()='Język Polski']").click()
        zewnetrzny_url_checkbox = browser.find_element_by_id('remotelyHostedContent').click()
        zewnetrzny_url = browser.find_element_by_name('remoteURL').send_keys(row['link do pdf'])
        zapisz = browser.find_elements_by_xpath("//button[@class='pkp_button submitFormButton']")
        zapisz[-1].click()
        time.sleep(2)
        dodaj_plik_eng = browser.find_element_by_xpath("//a[@title='Dodaj plik do publikacji']").click()
        time.sleep(2)
        etykieta = browser.find_element_by_xpath("//input[@class='field text required' and @name = 'label']").send_keys('PDF')
        jezyk_publikacji = browser.find_element_by_xpath("//select[@id = 'galleyLocale']/option[text()='English']").click()
        zewnetrzny_url_checkbox = browser.find_element_by_id('remotelyHostedContent').click()
        zewnetrzny_url = browser.find_element_by_name('remoteURL').send_keys(aktualny_numer.at[i+1, 'link do pdf'])
        zapisz = browser.find_elements_by_xpath("//button[@class='pkp_button submitFormButton']")
        zapisz[-1].click()
        
        do_publikacji = browser.find_element_by_xpath("//a[@title='##grid.action.schedulePublication##']").click()
        publikuj_w = browser.find_element_by_xpath(f"//select[@id = 'issueId']/option[text()='{nazwa_numeru}']").click()
        publikuj_strony = browser.find_element_by_name('pages').send_keys(row['strony'])
        zapisz = browser.find_elements_by_xpath("//button[@class='pkp_button submitFormButton']")
        zapisz[-1].click()
        time.sleep(1)
        przydziel_doi = browser.find_element_by_css_selector('label').text
        doi_artykulu = re.findall('(?<=Przydziel DOI ).*(?= dla artykuł)', przydziel_doi)[0]
        publikacja_ok = browser.find_elements_by_xpath("//button[@class='pkp_button submitFormButton']")
        publikacja_ok[-1].click()
        
#dodanie doi do wordpressa
        browser.get(row['url_edycji'])
        
        content = browser.find_element_by_id('content').get_attribute('value')
        
        doi_row = f"""<div><strong><strong>DOI:</strong></strong> <a href=" https://doi.org/{doi_artykulu}"> https://doi.org/{doi_artykulu} </a></div>\n"""
        
        body = re.sub('(?=\n {0,}<hr \/>)', doi_row, content)
        content = browser.find_element_by_id('content').clear()
        content = browser.find_element_by_id('content').send_keys(body)
        
        opublikuj = browser.find_element_by_id('publish').click()
        
        browser.get(aktualny_numer.at[i+1, 'url_edycji'])
        content = browser.find_element_by_id('content').get_attribute('value')
        body = re.sub('(?=\n {0,}<hr \/>)', doi_row, content)
        
        content = browser.find_element_by_id('content').clear()
        content = browser.find_element_by_id('content').send_keys(body)
        
        opublikuj = browser.find_element_by_id('publish').click()
        
print('Artykuły na pressto opublikowane i dodane DOI na wordpressie')

browser.close()

print('Done')

# ręcznie dodać redaktora - dlaczego?
# czemu doi są nieaktywne?
        


























