import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
from my_functions import gsheet_to_df

bazhum_pbl_mapping = gsheet_to_df('16OkWDxvJML-SG_7WBF98XE9OtkPSvOUTjaNahVM4a3I', 'Arkusz2')
bazhum_pbl_mapping.fillna(value=pd.np.nan, inplace=True)
bazhum_pbl_mapping = bazhum_pbl_mapping[bazhum_pbl_mapping['PBL_czasopismo'].notnull()]['BazHum_czasopismo'].tolist()

bazhum_pbl_mapping = ['Acta Universitatis Nicolai Copernici. Nauki Humanistyczno-Społeczne. Filozofia', 'Acta Universitatis Nicolai Copernici. Nauki Humanistyczno-Społeczne. Logika', 'Acta Universitatis Nicolai Copernici. Nauki Humanistyczno-Społeczne. Nauki Polityczne', 'Acta Universitatis Nicolai Copernici. Nauki Humanistyczno-Społeczne. Socjologia Wychowania', 'Muzealnictwo', 'Pamięć i Sprawiedliwość', 'Res Historica', 'Rocznik Towarzystwa Literackiego imienia Adama Mickiewicza', 'Studia Sandomierskie : teologia, filozofia, historia', 'Studia Źródłoznawcze / Commentationes', 'Sztuka i Filozofia']

url = 'http://bazhum.muzhp.pl/czasopismo/lista/'
response = requests.get(url)
response.encoding = 'UTF-8'
soup = BeautifulSoup(response.text, 'html.parser')
magazine_links = soup.findAll('a', attrs={'href': re.compile("czasopismo/\d+")})
bazhum_magazines = []
for magazine in magazine_links:
    link = f"http://bazhum.muzhp.pl{magazine['href']}"
    title = magazine.text
    bazhum_magazines.append((title, link))
    
bazhum_magazines = [t for t in bazhum_magazines if t[0] in bazhum_pbl_mapping]

final_data = []    
for i, magazine in enumerate(bazhum_magazines): 
    print(str(i) + '/' + str(len(bazhum_magazines)))
    url = magazine[1]
    response = requests.get(url)
    response.encoding = 'UTF-8'
    soup = BeautifulSoup(response.text, 'html.parser')
    
    bazhum_specification = soup.findAll('div', attrs={'class': 'box_metryczka'})
    bazhum_specification = bazhum_specification[0].text.lstrip('\n')
    bazhum_specification = re.sub('\n(?=\n)', '', bazhum_specification)
    bazhum_specification = re.sub(' +\n', '', bazhum_specification).split('\n')
    bazhum_specification = list(filter(None, bazhum_specification))
    test = []
    for row in bazhum_specification:
        if row.startswith('  '):
            test[-1] += ' ❦ ' + row.strip()
        else:
            test.append(row)
    for i, row in enumerate(test):
        test[i] = test[i].replace('❦', '=', 1)
        test[i] = test[i].replace(' ❦ ', ', ')

    bazhum_specification = [e for e in test if e not in ['Udostępnij', 'Inne tytuły']]
    
    numbers = soup.findAll('ul', attrs={'class': 'lista'})[0].findAll('a')
    magazine_numbers = []
    for number in numbers:
        magazine_number = number.text.strip()
        number_link = f"http://bazhum.muzhp.pl{number['href']}"
        magazine_numbers.append((magazine_number, number_link))
    
    for index, number in enumerate(magazine_numbers):
        print('    ' + str(index) + '/' + str(len(magazine_numbers)))
        url = number[1]
        response = requests.get(url)
        response.encoding = 'UTF-8'
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # full_text = soup.findAll('a', attrs={'href': re.compile('http://bazhum.muzhp.pl/media/')})
        full_text = soup.findAll('a', attrs={'href': re.compile('http://.*?pl/media//files')})
        full_text = [e['href'] for e in full_text if 'twitter' not in e['href']]
        box = soup.findAll(attrs={'class': 'copybox'})
        volume = []
        for pdf, biblio in zip(full_text, box):
            # pdf_tekstu = pdf['href']
            pdf_tekstu = pdf
            box_tekstu = biblio.text
            volume.append([box_tekstu, pdf_tekstu])
            
        volume = [t for t in volume if t[0].startswith('@Article')]
        
        for i, lista in enumerate(volume):
            volume[i][0] = re.split('\n   +', volume[i][0])
            volume[i][0] = [re.sub('\n', '', x) for x in volume[i][0]]
            volume[i][0] = [x.strip(' ') for x in volume[i][0]]
            volume[i][0] = list(filter(None, volume[i][0]))
            volume[i][0] = '❦'.join(volume[i][0][1:-1])
            volume[i] = '❦'.join(volume[i])
            volume[i] = volume[i].split('❦')
            volume[i] = bazhum_specification + volume[i]
        final_data.append(volume)

final_data = list(filter(None, final_data))        
df = pd.concat([pd.DataFrame(d, columns=['tytul_czasopisma', 'ISSN', 'wydawca', 'author', 'title', 'year', 'volume', 'number', 'journal', 'pages', 'full_text']) for d in final_data])
for column in df:
    if column != 'full_text':
        df[column] = df[column].str.replace(r'(.+?)( = )(.+?$)', r'\3', regex=True)
        
df.to_excel('bazhum_web_scraping1.xlsx', index=False)

errors = []
for lista in final_data:
    for lista2 in lista:
        if len(lista2) == 10:
           errors.append(lista2) 
df = pd.DataFrame(errors, columns=['tytul_czasopisma', 'ISSN', 'author', 'title', 'year', 'volume', 'number', 'journal', 'pages', 'full_text'])
for column in df:
    if column != 'full_text':
        df[column] = df[column].str.replace(r'(.+?)( = )(.+?$)', r'\3', regex=True)
df.to_excel('bazhum_web_scraping3.xlsx', index=False)           

proper = []
for lista in final_data:
    for lista2 in lista:
        if len(lista2) == 11:
           proper.append(lista2)           
df = pd.DataFrame(proper, columns=['tytul_czasopisma', 'ISSN', 'wydawca', 'author', 'title', 'year', 'volume', 'number', 'journal', 'pages', 'full_text'])
for column in df:
    if column != 'full_text':
        df[column] = df[column].str.replace(r'(.+?)( = )(.+?$)', r'\3', regex=True)
df.to_excel('bazhum_web_scraping2.xlsx', index=False)

other = []
for lista in final_data:
    for lista2 in lista:
        if len(lista2) not in [10, 11]:
           other.append(lista2)





