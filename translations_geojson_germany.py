import json
from shapely.geometry import shape, Point
import geopandas as gpd
import geoplot
import geoplot.crs as gcrs
import pandas as pd
from tqdm import tqdm
from ast import literal_eval
import numpy as np
from datetime import datetime

with open(r"C:\Users\Cezary\Downloads\germany_1_sehr_hoch.geo.json", 'r', encoding='utf-8') as f:
  germany_json = json.load(f)
  
with open(r"C:\Users\Cezary\Downloads\bundeslands_1_sehr_hoch.geo.json", 'r', encoding='utf-8') as f:
  germany_states_json = json.load(f)
  
#%%
now = datetime.now().date()
year = now.year
month = '{:02}'.format(now.month)
day = '{:02}'.format(now.day)

#%%rysowanie


# geojson_data = germany_json.get('features')[0].get('geometry').get('coordinates')[0]

# data = gpd.read_file("https://raw.githubusercontent.com/holtzy/The-Python-Graph-Gallery/master/static/data/france.geojson")

# data = gpd.read_file(r"C:\Users\Cezary\Downloads\germany_1_sehr_hoch.geo.json")
data = gpd.read_file("https://raw.githubusercontent.com/isellsoap/deutschlandGeoJSON/main/1_deutschland/1_sehr_hoch.geo.json")
data.crs

geoplot.polyplot(data, projection=gcrs.AlbersEqualArea(), edgecolor='darkgrey', facecolor='lightgrey', linewidth=.3, figsize=(12, 8))

#%% sprawdzanie

berlin_point = Point(13.383333, 52.516667)
warsaw_point = Point(21.011111, 52.23)
gorlitz_point = Point(14.987222, 51.152778)
zgorzelec_point = Point(15, 51.152778)
strasburg_point = Point(7.7483211608260225, 48.574048246493405)
kehl_point = Point(7.81596513628016, 48.57949364093979)

polygon = shape(germany_json.get('features')[0].get('geometry'))

polygon.contains(berlin_point)
polygon.contains(warsaw_point)
polygon.contains(gorlitz_point)
polygon.contains(zgorzelec_point)
polygon.contains(strasburg_point)
polygon.contains(kehl_point)


#%%
# wgranie danych translations

translations_df = pd.read_excel('translations_after_first_manual_2022-08-11.xlsx')

geo_translations_df = translations_df[['001', 'geonames_id', 'geonames_name', 'geonames_country', 'geonames_lat', 'geonames_lng']]
geo_translations_df = geo_translations_df[geo_translations_df['geonames_id'].notnull()]
for column in geo_translations_df.columns.values[1:]:
    geo_translations_df[column] = geo_translations_df[column].apply(lambda x: literal_eval(x))

geo_translations_dict = {}
for i, row in tqdm(geo_translations_df.iterrows(), total=geo_translations_df.shape[0]):
    # i = 13
    # row = geo_translations_df.iloc[i,:]
    geo_translations_dict[row['001']] = [{row[1:].index.values[i]:el for i, el in enumerate(e)} for e in zip(*row[1:])]
    
unique_geo_names = [dict(el) for el in set([tuple(e.items()) for sub in list(geo_translations_dict.values()) for e in sub])]

lands_dict = {'Baden-Württemberg': 'West Germany',
              'Bayern': 'West Germany',
              'Berlin': 'Berlin',
              'Brandenburg': 'East Germany',
              'Bremen': 'West Germany',
              'Hamburg': 'West Germany',
              'Hessen': 'West Germany',
              'Mecklenburg-Vorpommern': 'East Germany',
              'Niedersachsen': 'West Germany',
              'Nordrhein-Westfalen': 'West Germany',
              'Rheinland-Pfalz': 'West Germany',
              'Saarland': 'West Germany',
              'Sachsen-Anhalt': 'East Germany',
              'Sachsen': 'East Germany',
              'Schleswig-Holstein': 'West Germany',
              'Thüringen': 'East Germany'}

polygon_germany = shape(germany_json.get('features')[0].get('geometry'))
polygons_lands_dict = dict(zip([e.get('properties').get('name') for e in germany_states_json.get('features')], [e.get('geometry') for e in germany_states_json.get('features')]))
polygons_lands_dict = {k:shape(v) for k,v in polygons_lands_dict.items()}

german_places = {}
for geo_dict in tqdm(unique_geo_names):    
    # geo_dict = unique_geo_names[52]
    point = Point(float(geo_dict.get('geonames_lng')), float(geo_dict.get('geonames_lat')))
    if polygon_germany.contains(point):
        germany_state = lands_dict.get({k for k,v in polygons_lands_dict.items() if v.contains(point)}.pop())
        temp_dict = geo_dict.copy()
        temp_dict.update({'State': germany_state})
        german_places[geo_dict.get('geonames_id')] = temp_dict
    

translations_df['division_of_germany'] = ''
for i, row in tqdm(translations_df.iterrows(), total=translations_df.shape[0]):
    # i = 0
    # row = test.iloc[i,:]
    if 1945 <= row['year'] <= 1990 and not isinstance(row['geonames_id'], float):
        geonames_ids = literal_eval(row['geonames_id'])
        if any(e in geonames_ids for e in german_places):
            state_of_germany = [german_places.get(e).get('State') if e in german_places else None for e in geonames_ids]
            translations_df.at[i, 'division_of_germany'] = state_of_germany
 
translations_df.to_excel(f'translations_after_first_manual_with_germany_{now}.xlsx', index=False)







#%% notatki

data = {'001': 924780891,
 'geonames_id': [6951076, 2643743],
 'geonames_name': ['Harmondsworth', 'London'],
 'geonames_country': ['United Kingdom', 'United Kingdom'],
 'geonames_lat': ['51.48836', '51.50853'],
 'geonames_lng': ['-0.47757', '-0.12574']}

data_series = pd.Series(data)


{924780891: [{'geonames_id': 6951076,
              'geonames_name': 'Harmondsworth',
              'geonames_country': 'United Kingdom',
              'geonames_lat': 51.48836,
              'geonames_lng': -4.7757},
             {'geonames_id': 6951076,
              'geonames_name': 'Harmondsworth',
              'geonames_country': 'United Kingdom',
              'geonames_lat': 51.48836,
              'geonames_lng': -4.7757}]}









# depending on your version, use: from shapely.geometry import shape, Point

# load GeoJSON file containing sectors
with open('sectors.json') as f:
    js = json.load(f)

# construct point based on lon/lat returned by geocoder
point = Point(-122.7924463, 45.4519896)

# check each polygon to see if it contains the point
for feature in js['features']:
    polygon = shape(feature['geometry'])
    if polygon.contains(point):
        print 'Found containing polygon:', feature