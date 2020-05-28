import requests
from bs4 import BeautifulSoup
import re
import pandas as pd

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

final_data = []    
for i, magazine in enumerate(bazhum_magazines): 
    print(str(i) + '/' + str(len(bazhum_magazines)))
    
    url = magazine[1]
    response = requests.get(url)
    response.encoding = 'UTF-8'
    soup = BeautifulSoup(response.text, 'html.parser')
    
    bazhum_specification = soup.findAll('div', attrs={'class': 'box_metryczka'})   
    bazhum_specification = bazhum_specification[0].text.split('\n')
    for i, elem in enumerate(bazhum_specification):
        bazhum_specification[i] = bazhum_specification[i].strip()
    bazhum_specification = list(filter(None, bazhum_specification))
    bazhum_specification = [e for e in bazhum_specification if e != 'Udostępnij']
    bazhum_specification = [i+' = '+j for i,j in zip(bazhum_specification[::2], bazhum_specification[1::2])]
    
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
        
        full_text = soup.findAll('a', attrs={'href': re.compile('http://bazhum.muzhp.pl/media/')})
        box = soup.findAll(attrs={'class': 'copybox'})
        volume = []
        for pdf, biblio in zip(full_text, box):
            pdf_tekstu = pdf['href']
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
df = pd.concat([pd.DataFrame(d, columns=['tytul_czasopisma', 'ISSN', 'Wydawca', 'author', 'title', 'year', 'volume', 'number', 'journal', 'pages', 'full_text']) for d in final_data])
for column in df:
    if column != 'full_text':
        df[column] = df[column].str.replace(r'(.+?)( = )(.+?$)', r'\3', regex=True)
        
df.to_excel('bazhum_web_scraping.xlsx', index=False)




