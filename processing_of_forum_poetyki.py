from my_functions import gsheet_to_df, df_to_gsheet
import pandas as pd
import re
import regex
import glob
from unidecode import unidecode
import datetime
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import wordpress_credentials
import time
from selenium.common.exceptions import NoSuchElementException

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
        aktualny_numer.at[i, 'jpg'] = sciezka_jpg

#wprowadzanie nowych wpisów
        
browser = webdriver.Chrome()
browser.get("http://fp.amu.edu.pl/admin")    
browser.implicitly_wait(5)
username_input = browser.find_element_by_id('user_login')
password_input = browser.find_element_by_id('user_pass')

username = wordpress_credentials.wordpress_username
password = wordpress_credentials.wordpress_password

username_input.send_keys(username)
password_input.send_keys(password)

login_button = browser.find_element_by_id('wp-submit').click()

for index, row in aktualny_numer.iterrows():
    if index%2 == 0:
      
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
        
        odnosnik = browser.find_element_by_id('sample-permalink').text
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
        
        odnosnik = browser.find_element_by_id('sample-permalink').text
        aktualny_numer.at[index+1, 'odnosnik'] = odnosnik
        
        url_edycji = browser.current_url
        aktualny_numer.at[index+1, 'url_edycji'] = url_edycji

#dane do strony numeru
        
for i, row in aktualny_numer.iterrows():
    tytul = row['tytuł artykułu'].replace('<i>', '</em>').replace('</i>', '<em>')   
    body = f"""<p style="text-align: left; margin: 0cm 0cm 15pt; line-height: 15pt; font-size: 11pt; font-family: ChaparralPro-Regular; color: black; letter-spacing: 0.1pt; padding-left: 30px;" align="left"><a href="{row['odnosnik']}">{row['autor']}, <em>{tytul}</em></a></p>"""
    aktualny_numer.at[i, 'spis treści'] = body
        
#uzupełnienie tabeli na dysku google

df_to_gsheet(aktualny_numer, gs_table, 'artykuły po pętli')

#ile dla kategorii, potem pętla co dwa, a dla kategorii licznik i while, jeśli licznik jest niższy od tego, ile dla kategorii

ile_dla_kategorii = aktualny_numer[['kategoria', 'język']].value_counts()

#wprowadzenie strony nowego numeru

strona_numeru = gsheet_to_df(gs_table, 'strona')

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
        


# link do pdf
# link do jpg, jpg, spis treści

#spis treści generowany automatycznie


browser.close()





