import requests
from requests import get
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import re


### ENCYKLOPEDIA TEATRU ###
url = "https://www.encyklopediateatru.pl/sztuki"
results = requests.get(url)
soup = BeautifulSoup(results.text, "html.parser")

schemat = re.compile('\/sztuki\/.')

indeks_tytulow =  soup.findAll('a', attrs={'href': schemat})

for e in indeks_tytulow:
    print(e.text, e['href'])

indeks = []    
for e in indeks_tytulow:
    if len(e.text) == 1:
        indeks.append((e.text, e['href']))
indeks.append(('a', '/sztuki'))

        
for ind, link in indeks:
    url = f"https://www.encyklopediateatru.pl{link}"
    results = requests.get(url)
    soup = BeautifulSoup(results.text, "html.parser")
    
    tytuly = soup.find_all(attrs={'class': 'title'})
    for e in tytuly:
        print(e.text)
        
    autorzy = soup.find_all(attrs={'class': 'author'})
    
#inne liczby, szukamy dalej
    
    container = soup.select('#results .col-xl-6 li')
    for e in container:
        url_sztuki = e.find('a')['href']
        tytul = e.find('span', {'class': 'title'}).text
        autor = e.find_all('span', {'class': 'author'})
        autor = '❦'.join([aut.text for aut in autor])
        print(f"{url_sztuki}|{tytul}|{autor}")
        
#gotowe
lista_sztuk = []
for ind, link in indeks:
    print(ind)
    url = f"https://www.encyklopediateatru.pl{link}"
    results = requests.get(url)
    soup = BeautifulSoup(results.text, "html.parser")
    container = soup.select('#results .col-xl-6 li')
    for element in container:
        try:
            url_sztuki = element.find('a')['href']
        except AttributeError:
            url_sztuki = None
        try:
            tytul = element.find('span', {'class': 'title'}).text
        except AttributeError:
            tytul = None
        try:
            autor = element.find('span', {'class': 'author'}).text
        except AttributeError:
            autor = None
        lista_sztuk.append((tytul, autor, f"https://www.encyklopediateatru.pl{url_sztuki}"))
        
df = pd.DataFrame(lista_sztuk, columns=['tytul', 'autor', 'link']).drop_duplicates().reset_index(drop=True)
df['autor'] = df['autor'].str.strip()

df.to_excel('enc_tea.xlsx', index=False)

        
    

#selenium

from selenium import webdriver
from selenium.webdriver.common.keys import Keys

browser = webdriver.Chrome()
browser.get("https://www.encyklopediateatru.pl/sztuki")

#nie działa
indeksy = browser.find_elements_by_css_selector('.index-alphabet a')
for elem in indeksy:
    elem.click()
    browser.back()
    
#nie działa
    
for elem in range(len(browser.find_elements_by_css_selector('.index-alphabet a'))):
    print(f"{elem}/{len(browser.find_elements_by_css_selector('.index-alphabet a'))}")
    indeksy = browser.find_elements_by_css_selector('.index-alphabet a')
    indeksy[elem].click()
    browser.back()

#nie działa do końca
lista_sztuk2 = []
for elem in range(0, len(browser.find_elements_by_css_selector('.index-alphabet a'))):
    print(f"{elem}/{len(browser.find_elements_by_css_selector('.index-alphabet a'))}")
    indeksy = browser.find_elements_by_css_selector('.index-alphabet a')
    indeksy[elem].click()
    container = browser.find_elements_by_css_selector('#results .col-xl-6 li')
    for element in container:
        print(element.text)
    browser.get("https://www.encyklopediateatru.pl/sztuki")
        try:
            url_sztuki = element.find('a')['href']
        except AttributeError:
            url_sztuki = None
        try:
            tytul = element.find('span', {'class': 'title'}).text
        except AttributeError:
            tytul = None
        try:
            autor = element.find('span', {'class': 'author'}).text
        except AttributeError:
            autor = None
        lista_sztuk2.append((tytul, autor, f"https://www.encyklopediateatru.pl{url_sztuki}"))
    browser.get("https://www.encyklopediateatru.pl/sztuki")
    
### BIBLIOGRAFIA MAŁOPOLSKI ###
import requests
from requests import get
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import re
from selenium import webdriver
from selenium.webdriver.common.keys import Keys

url = "https://bibliografia.malopolska.pl/sowacgi.php?KatID=0&typ=repl&plnk=q__*&sort=byscore&view=3"
results = requests.get(url)
soup = BeautifulSoup(results.text, "html.parser")

rekordy_na_stronie = soup.select('.record-description')


lista_rekordow = []
for rekord in rekordy_na_stronie:
    lista_rekordow.append(rekord.text.split('\n'))
    
lista_rekordow = [[elem for elem in podlista if elem.startswith('LDR')] for podlista in lista_rekordow]
lista_rekordow = [item for sublist in lista_rekordow for item in sublist]

lista_rekordow = [podlista for podlista in lista_rekordow if max(podlista)]

browser = webdriver.Chrome()
browser.get("https://bibliografia.malopolska.pl/sowacgi.php?KatID=0&typ=repl&plnk=q__*&sort=byscore&view=3")

next_page = browser.find_element_by_xpath('//body/div[1]/div[2]/div[2]/div[1]/div[2]/a[3]/*[1]')
url = browser.current_url
next_page.click()



schemat = re.compile('\/sztuki\/.')

indeks_tytulow =  soup.findAll('a', attrs={'href': schemat})
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
