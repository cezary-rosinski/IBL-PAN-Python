from my_functions import gsheet_to_df
import pandas as pd
from ast import literal_eval
import json

podkorpus_1000 = gsheet_to_df('17OHdiMqtfzPSzkeyPHcCzdGY8gOhek3bhv1zKjs9CCc', 'Podkorpus 1000')
korpus_roboczy = gsheet_to_df('17OHdiMqtfzPSzkeyPHcCzdGY8gOhek3bhv1zKjs9CCc', 'korpus roboczy')
ostateczna_deduplikacja = gsheet_to_df('17OHdiMqtfzPSzkeyPHcCzdGY8gOhek3bhv1zKjs9CCc', 'ostateczna deduplikacja')


podkorpus_1000 = podkorpus_1000.head(1000).drop(columns=podkorpus_1000.columns.values[-2:]).drop(columns='CZY ISTNIEJE W JEDNYM Z NICH')
ostateczna_deduplikacja = ostateczna_deduplikacja[['lp', 'creator_wikidata', 'geonames', 'num_tokens', 'birthplace', 'birthplace_id', 'birthplace_latitude', 'birthplace_longitude']]

df = pd.merge(podkorpus_1000, ostateczna_deduplikacja, on='lp', how='left')

# korpus_roboczy = korpus_roboczy[['id', 'bn_genre']]

# df = pd.merge(df, korpus_roboczy, left_on='pierwodruki_id', right_on='id', how='left')

#dopytać Agnieszkę --> czy CZY ISTNIEJE W JEDNYM Z NICH jest potrzebne, czy brać coś z korpusu roboczego?



books = df[['lp', 'creator', 'title', 'zabór', 'epoka', 'recepcja', 'ELTeC', 'year', 'num_tokens']]

length = books.shape[0]
for l in range(1, length+1):
    books.at[l-1, 'id'] = f'metapnc_b_{l}'

books.index = books['id']

books_json = books.to_dict(orient='index')

people = df[['creator', 'gender', 'creator_wikidata']].drop_duplicates().reset_index(drop=True)


length = people.shape[0] + length
for i, row in people.iterrows():
    people.at[i, 'id'] =  f'metapnc_p_{range(1001, length+1)[i]}'

people.index = people['id']

people_json = people.to_dict(orient='index')

test = [dict(ele) for ele in set([tuple(el.items()) for sub in [literal_eval(e) for e in df['geonames'].to_list()] for el in sub])]
test = pd.DataFrame(test)

test2 = df[['birthplace', 'birthplace_id',
'birthplace_latitude', 'birthplace_longitude']].drop_duplicates().dropna().rename(columns={'birthplace': 'name', 'birthplace_id': 'wikidataId', 'birthplace_latitude': 'lat', 'birthplace_longitude': 'lng'})

places = pd.concat([test, test2]).reset_index(drop=True)

length = places.shape[0] + length
for i, row in places.iterrows():
    places.at[i, 'id'] =  f'metapnc_g_{range(1391, length+1)[i]}'

places.index = places['id']

places_json = places.to_dict(orient='index')

jsons = {'dh2023_books': books_json,
         'dh2023_people': people_json,
         'dh2023_places': places_json}

for k, v in jsons.items():
    with open(f'{k}.json', 'w') as f:
        json.dump(v, f)

#mapowanie na ontologię --> z PH
ontology_columns = {'lp', 
                    'creator', 
                    'title', 
                    'gender', 
                    'zabór', 
                    'epoka', 
                    'recepcja',
                    'ELTeC', 
                    'pierwodruki_id', 
                    'year', 
                    'corpus_id', 
                    'creator_wikidata',
                    'geonames', 
                    'num_tokens', 
                    'birthplace', 
                    'birthplace_id',
                    'birthplace_latitude', 
                    'birthplace_longitude'}