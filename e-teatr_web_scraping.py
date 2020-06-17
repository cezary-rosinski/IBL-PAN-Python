import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import numpy as np

# def
def description_of_contributors(x):
    if pd.notnull(x['tłumacz']) and pd.notnull(x['adaptator']) and pd.notnull(x['reżyser']):
        val = f"Tł. {x['tłumacz']}. Adapt. {x['adaptator']}. Reż. {x['reżyser']}."
    elif pd.notnull(x['tłumacz']) and pd.notnull(x['adaptator']):
        val = f"Tł. {x['tłumacz']}. Adapt. {x['adaptator']}."
    elif pd.notnull(x['tłumacz']) and pd.notnull(x['reżyser']):
        val = f"Tł. {x['tłumacz']}. Reż. {x['reżyser']}."
    elif pd.notnull(x['adaptator']) and pd.notnull(x['reżyser']):
        val = f"Adapt. {x['adaptator']}. Reż. {x['reżyser']}."
    elif pd.notnull(x['tłumacz']):
        val = f"Tł. {x['tłumacz']}."
    elif pd.notnull(x['adaptator']):
        val = f"Adapt. {x['adaptator']}."
    elif pd.notnull(x['reżyser']):
        val = f"Reż. {x['reżyser']}."
    else:
        val = np.nan
    return val

# main

years = range(2005,2013)
plays_description = []  

for index, year in enumerate(years):
    print(str(index) + '/' + str(len(years)))    
    url = f"http://www.e-teatr.pl/pl/realizacje/lista.html?nazwisko=&tytul=&rok={year}&Submit=szukaj"
    response = requests.get(url)
    response.encoding = 'UTF-8'
    soup = BeautifulSoup(response.text, 'html.parser')
    plays = soup.select('#recenzjeLista a')
    plays = [(play.text, play['href']) for play in plays]
    plays = [(a, f"http://www.e-teatr.pl/pl/realizacje/{b}") for (a, b) in plays]
    for ind, play in enumerate(plays):
        print('    ' + str(ind) + '/' + str(len(plays)))
        link = play[1]
        response = requests.get(link)
        response.encoding = 'UTF-8'
        soup = BeautifulSoup(response.text, 'html.parser')
        performances = soup.findAll('a', attrs={'href': re.compile("^\d+\,szczegoly.html")})
        performances = [(p.text.strip(), f"http://www.e-teatr.pl/pl/realizacje/{p['href']}") for p in performances]
        for i, performance in enumerate(performances):
            link = performance[1]
            response = requests.get(link)
            response.encoding = 'UTF-8'
            soup = BeautifulSoup(response.text, 'html.parser')
            body = soup.select_one('td').text.strip()
            body = body.split('\n')
            body = list(filter(None, body))
            try:
                obsada_index = body.index("Obsada:")
                body = body[:obsada_index]
            except ValueError:
                pass
            title_of_play = [re.findall('(?<=tytuł realizacji:)(.+)', t)[0].strip() for t in body if 'tytuł realizacji:' in t][0]
            try:
                title_of_text = [re.findall('(?<=utwór:)(.+)', t)[0].strip() for t in body if 'utwór:' in t][0]
                author_index = [i+1 for i, word in enumerate(body) if word.startswith('utwór:')][0]
                author_of_text = re.sub('\(|\)', '', body[author_index]).strip()
            except IndexError:
                title_of_text = np.nan
                author_of_text = np.nan
            try:
                place_of_premiere = [re.findall('(?<=miejsce premiery:)(.+)', t)[0].strip() for t in body if 'miejsce premiery:' in t][0]
            except IndexError:
                place_of_premiere = np.nan
            try:
                date_of_premiere = [re.findall('(?<=data premiery:)(.+)', t)[0].strip() for t in body if 'data premiery:' in t][0]
            except IndexError:
                date_of_premiere = np.nan
            try:
                translator = ['❦'.join(re.findall('(?<=przekład:)(.+)', t)) for t in body if 'przekład:' in t][0]
            except IndexError:
                translator = np.nan
            try:
                adaptator = ['❦'.join(re.findall('(?<=adaptacja:)(.+)', t)) for t in body if 'adaptacja:' in t][0]
            except IndexError:
                adaptator = np.nan
            try:
                director = ['❦'.join(re.findall('(?<=reżyseria:)(.+)', t)) for t in body if 'reżyseria:' in t][0]
            except IndexError:
                director = np.nan
            plays_description.append((year, title_of_play, title_of_text, author_of_text, place_of_premiere, date_of_premiere, translator, adaptator, director))
        
eteatr_plays = pd.DataFrame(plays_description, columns=['rok', 'tytuł sztuki', 'utwór', 'autor utworu', 'miejce premiery', 'data premiery', 'tłumacz', 'adaptator', 'reżyser'])
       
eteatr_plays['opis współtwórców'] = eteatr_plays.apply(lambda x: description_of_contributors(x), axis=1)

eteatr_plays.to_excel('e-teatr_2005-2012.xlsx', index=False)
        







