import requests
from bs4 import BeautifulSoup
import re
import pandas as pd

years = range(2005,2013)

for year in years:
    
    
    
year = 2005
url = f"http://filmpolski.pl/fp/index.php?filmy_z_roku={year}&typ=2"
response = requests.get(url)
response.encoding = 'UTF-8'
soup = BeautifulSoup(response.text, 'html.parser')
movies_list = soup.select('.tytul a')
movies = [(movie.text, movie['href']) for movie in movies_list]
movies = list(set(movies))
movies = [(a, re.findall('\d+', b)[0]) for (a, b) in movies]
movies = [(a, f"http://filmpolski.pl/fp/index.php?film={b}") for (a, b) in movies]
    
for movie in movies:
    link = movie[1]
    link = movies[0][1]
    response = requests.get(link)
    response.encoding = 'UTF-8'
    soup = BeautifulSoup(response.text, 'html.parser')
    movie_title = soup.select_one('#film h1').text.capitalize()
    movie_type = soup.select_one('b').text

# tytuł filmu, typ filmu, kraj produkcji, scenariusz, bohater, produkcja, reżyser, opis, pierwowzór


movies_list = soup.findAll('a', attrs={'href': re.compile("index\.php\/\d+")})
movies = [(movie.text, movie['href']) for movie in movies_list]


movies = []
for movie in movies_list:
    movies.append()
print(soup)
    





