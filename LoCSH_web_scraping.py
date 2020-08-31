import requests
from bs4 import BeautifulSoup
import re
import pandas as pd

url = "https://id.loc.gov/search/?q=--literatures&q=cs:http://id.loc.gov/authorities/subjects&start=1"
response = requests.get(url)
response.encoding = 'UTF-8'
soup = BeautifulSoup(response.text, 'html.parser')

links = soup.findAll('tbody', attrs={'class': 'tbody-group'})
links = [re.split('\n', elem.text) for elem in links]
links = [[elem[i] for i in [3,7,11]] for elem in links]

next_page = soup.findAll('a', attrs={'class': 'next'})
next_page = list(set([elem['href'] for elem in next_page]))[0]

iteration = 1
while len(next_page) > 1:
    print(iteration)
    response = requests.get('https://id.loc.gov/search/' + next_page)
    response.encoding = 'UTF-8'
    soup = BeautifulSoup(response.text, 'html.parser')
    
    next_links = soup.findAll('tbody', attrs={'class': 'tbody-group'})
    next_links = [re.split('\n', elem.text) for elem in next_links]
    next_links = [[elem[i] for i in [3,7,11]] for elem in next_links]
    
    links += next_links
    
    next_page = soup.findAll('a', attrs={'class': 'next'})
    next_page = list(set([elem['href'] for elem in next_page]))[0]
    
    iteration += 1
    
df = pd.DataFrame(links, columns=['name', 'id', 'name_variants']).drop_duplicates().reset_index(drop=True)

df.to_excel('LoC_national_literatures.xlsx', index=False)

print('Done')    








