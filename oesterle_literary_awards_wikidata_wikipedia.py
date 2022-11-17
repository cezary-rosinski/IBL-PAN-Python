import requests
from bs4 import BeautifulSoup

url = 'https://pl.wikipedia.org/wiki/Kategoria:Polskie_nagrody_literackie'

result = requests.get(url).content
soup = BeautifulSoup(result, 'lxml')
titles = [e.text for e in soup.select('#mw-pages a')]
links = [e['href'] for e in soup.select('#mw-pages a')]
soup.sel