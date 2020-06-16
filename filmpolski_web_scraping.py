import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import regex
import numpy as np

# def
def get_text(x):
    return x.text

def wszystko(x):
    everything = []
    for header, cell in zip(headers, x):
        pair = f"{header}: {cell}"
        everything.append(pair)
    everything = '\n'.join(everything)
    return everything

# main

years = range(2005,2013)
movies_description = []   

for index, year in enumerate(years):
    print(str(index) + '/' + str(len(years)))
    url = f"http://filmpolski.pl/fp/index.php?filmy_z_roku={year}&typ=2"
    response = requests.get(url)
    response.encoding = 'UTF-8'
    soup = BeautifulSoup(response.text, 'html.parser')
    movies_list = soup.select('.tytul a')
    movies = [(movie.text, movie['href']) for movie in movies_list]
    movies = list(set(movies))
    movies = [(a, re.findall('\d+', b)[0]) for (a, b) in movies]
    movies = [(a, f"http://filmpolski.pl/fp/index.php?film={b}") for (a, b) in movies]
    for ind, movie in enumerate(movies):
        print('    ' + str(ind) + '/' + str(len(movies)))
        link = movie[1]
        response = requests.get(link)
        response.encoding = 'UTF-8'
        soup = BeautifulSoup(response.text, 'html.parser')
        movie_title = soup.select_one('#film h1').text.capitalize()
        movie_type = soup.select_one('b').text
        metryczka = soup.select_one('.tech').text
        try:
            country_of_production = re.findall('(?<=Produkcja:)(.+)', metryczka)[0].strip()
        except IndexError:
            country_of_production = np.nan
        try:
            year_of_production = re.findall('(?<=Rok produkcji:)(.+)', metryczka)[0].strip()
        except IndexError:
            year_of_production = np.nan
        try:
            premiere = re.findall('(?<=Premiera:)(.+)', metryczka)[0].strip()
        except IndexError:
            premiere = np.nan
        try:
            genre = re.findall('(?<=Gatunek:)(.+)', metryczka)[0].strip()
        except IndexError:
            genre = np.nan
        duration = metryczka.split('\n')
        duration = list(filter(None, duration))
        try:
            duration = [elem for elem in duration if re.findall("\d min|''|\"", elem)][0]
        except IndexError:
            duration = np.nan
        body = list(set(map(get_text, soup.select('.ekipa li'))))
        for i, elem in enumerate(body):
            body[i] = regex.sub('(\p{Ll})(\p{Lu})', r'\1: \2', body[i], 1)
            body[i] = regex.sub('(\p{Ll})(\p{Lu})', r'\1, \2', body[i])
        body = '\n'.join(body)
        try:
            director = re.findall('(?<=Reżyseria:)(.+)', body)[0].strip()
        except IndexError:
            director = np.nan
        try:
            scenario = re.findall('(?<=Scenariusz:)(.+)', body)[0].strip()
        except IndexError:
            scenario = np.nan
        try:
            production_company = re.findall('(?<=Produkcja:)(.+)', body)[0].strip()
        except IndexError:
            production_company = np.nan
        try:
            hero = re.findall('(?<=Bohater:)(.+)', body)[0].strip()
        except IndexError:
            hero = np.nan
        try:
            description = soup.select_one('.opis').text
        except AttributeError:
            description = np.nan
        varia = soup.findAll('table', attrs={'class': "varia_table"})
        varia = [elem.text.strip() for elem in varia if 'Pierwowzór' in elem.text]
        varia = [regex.sub('(\s+\n\s+)', '\n', elem) for elem in varia]
        for i, elem in enumerate(varia):
            varia[i] = regex.sub('(\p{Ll})(\p{Lu}{2})', r'\1: \2', varia[i], 1)
            varia[i] = regex.sub('(\p{Ll})(\p{Lu})', r'\1: \2', varia[i])
        varia = '\n'.join(varia)    
        if len(varia) == 0:
            varia = np.nan
        movies_description.append((year, movie_title, movie_type, country_of_production, year_of_production, premiere, genre, duration, director, scenario, production_company, hero, description, varia))

filmpolski_movies = pd.DataFrame(movies_description, columns=['rok', 'tytuł filmu', 'typ filmu', 'kraj produkcji', 'rok produkcji', 'premiera', 'gatunek', 'czas trwania', 'reżyser', 'scenariusz', 'produkcja', 'bohater', 'opis', 'pierwowzor'])

headers = filmpolski_movies.columns.tolist()

filmpolski_movies['wszystko'] = filmpolski_movies.apply(lambda x: wszystko(x), axis=1)

filmpolski_movies.to_excel('filmpolski_2005-2012.xlsx', index=False)






