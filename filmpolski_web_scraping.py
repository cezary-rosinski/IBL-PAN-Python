import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import regex
import numpy as np
import itertools

# def

def get_text(x):
    return x.text

def wszystko(x):
    everything = []
    for header, cell in zip(headers, x):
        if pd.notnull(cell):
            pair = f"{header}: {cell}"
            everything.append(pair)
    everything = '\n'.join(everything)
    return everything

def ranges(i):
    for a, b in itertools.groupby(enumerate(i), lambda pair: pair[1] - pair[0]):
        b = list(b)
        yield f"{b[0][1]}-{b[-1][1]}"
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
        
        link = 'http://filmpolski.pl/fp/index.php?film=1216115'
        response = requests.get(link)
        response.encoding = 'UTF-8'
        soup = BeautifulSoup(response.text, 'html.parser')
        try:
            movie_title = soup.select_one('#film h1').text.capitalize()
        except AttributeError:
            movie_title = np.nan
        try:
            movie_type = soup.select_one('b').text
        except AttributeError:
            movie_type = np.nan
        metryczka = soup.select_one('.tech').text
        try:
            country_of_production = re.findall('(?<=Produkcja:)(.+)', metryczka)[0].strip()
        except IndexError:
            country_of_production = np.nan
        try:
            year_of_production = re.findall('(?<=Rok produkcji:)(.+)', metryczka)[0].strip()
        except IndexError:
            year_of_production = np.nan
        duration = metryczka.split('\n')
        duration = list(filter(None, duration))
        try:
            duration = [elem for elem in duration if re.findall("\d min|''|\"", elem)][0]
        except IndexError:
            duration = np.nan
        body = list(set(map(get_text, [elem for elem in soup.select('.ekipa li')])))
        
        body = []
        for elem in soup.select('.ekipa li'):
            for children in elem:
                try:
                    body.append((children.text.strip(), children.attrs['class'][0].strip()))
                except KeyError:
                    pass       
        try:
            df = pd.DataFrame(body, columns=['content', 'type'])
            df['help'] = df.apply(lambda x: x.name if x['type'] == 'ekipa_funkcja' else np.nan, axis=1).ffill()
            df['function'] = df.apply(lambda x: x['content'] if x['type'] == 'ekipa_funkcja' else np.nan, axis=1).ffill()
            df = df[(df['function'].isin(['Reżyseria', 'Scenariusz', 'Produkcja', 'Bohater'])) &
                    (df['type'] != 'ekipa_funkcja') &
                    (df['content'] != '')]
            df['help'] = df.apply(lambda x: x.name if x['type'] == 'ekipa_osoba' else np.nan, axis=1).ffill()
            df['content'] = df.apply(lambda x: f"({x['content']})" if x['type'] == 'ekipa_opis' else x['content'], axis=1)
            df['grouped'] = df.groupby('help')['content'].apply(lambda x: ' '.join(x))
            df = df[df['grouped'].notnull()][['function', 'grouped']].drop_duplicates()
            df['grouped'] = df.groupby('function')['grouped'].transform(lambda x: ', '.join(x))
            df = df.drop_duplicates()
            try:
                director = '❦'.join(df[df['function'] == 'Reżyseria']['grouped'].to_list())
            except:
                director = np.nan
            try:
                scenario = '❦'.join(df[df['function'] == 'Scenariusz']['grouped'].to_list())
            except:
                scenario = np.nan
            try:
                production_company = '❦'.join(df[df['function'] == 'Produkcja']['grouped'].to_list())
            except:
                production_company = np.nan
            try:
                hero = '❦'.join(df[df['function'] == 'Bohater']['grouped'].to_list())
            except:
                hero = np.nan
        except ValueError:
            director = np.nan
            scenario = np.nan
            production_company = np.nan
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
        try:
            ostatnia = soup.select_one('.ostatnia')['href'] 
            episodes_list = []       
            episodes = [re.findall('\d+', elem.text)[-1] for elem in soup.select('.podzbior h1')]
            episodes_years = [elem.text for elem in soup.select('.podzbior .film_tech2') if re.findall('\d{4}', elem.text) and len(elem.text) == 4]
            episodes = list(zip(episodes, episodes_years))
            for elem in episodes:
                episodes_list.append(elem)
            link_start = 'http://filmpolski.pl/fp/' + re.sub('(.+?\_)(.+?$)', r'\1', ostatnia)
            zakres = range(2, int(re.sub('(.+?\_)(.+?$)', r'\2', ostatnia)) + 1)
            zakres = [link_start + str(elem) for elem in zakres]
            for link in zakres:
                response = requests.get(link)
                response.encoding = 'UTF-8'
                soup = BeautifulSoup(response.text, 'html.parser')
                episodes = [re.findall('\d+', elem.text)[-1] for elem in soup.select('.podzbior h1')]
                episodes_years = [elem.text for elem in soup.select('.podzbior .film_tech2')]
                episodes = list(zip(episodes, episodes_years))  
                for elem in episodes:
                    episodes_list.append(elem)
            
            df_episodes = pd.DataFrame(episodes_list, columns=['episode', 'year'])
            df_episodes['episode'] = df_episodes.groupby('year').transform(lambda x: ', '.join(x))
            df_episodes = df_episodes.drop_duplicates()
            for i, row in df_episodes.iterrows():
                df_episodes.at[i, 'episode'] = '❦'.join(list(ranges([int(i) for i in row['episode'].split(', ')])))
            df_episodes['total'] = df_episodes.apply(lambda x: f"{x['year']}: odcinki {x['episode']}", axis=1)
            episodes = '; '.join(df_episodes['total'].to_list())
        except (TypeError, IndexError, ValueError):
            episodes = np.nan
        movies_description.append([year, movie_title, movie_type, country_of_production, year_of_production, duration, episodes, director, scenario, production_company, hero, description, varia])

movies_description = [[np.nan if e == '' else e for e in elem] for elem in movies_description]
filmpolski_movies = pd.DataFrame(movies_description, columns=['rok', 'tytuł filmu', 'typ filmu', 'kraj produkcji', 'rok produkcji', 'czas trwania', 'odcinki', 'reżyser', 'scenariusz', 'produkcja', 'bohater', 'opis', 'pierwowzor'])

headers = filmpolski_movies.columns.tolist()

filmpolski_movies['wszystko'] = filmpolski_movies.apply(lambda x: wszystko(x), axis=1)

filmpolski_movies.to_excel('filmpolski_2005-2012.xlsx', index=False)






