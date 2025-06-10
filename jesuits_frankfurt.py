import sys
sys.path.insert(1, 'D:\IBL\Documents\IBL-PAN-Python')
import requests
from tqdm import tqdm
import numpy as np
from my_functions import gsheet_to_df, simplify_string, cluster_strings, marc_parser_to_dict
from concurrent.futures import ThreadPoolExecutor
import json
import glob
import random
import time
from datetime import datetime
import Levenshtein as lev
import io
import pandas as pd
from bs4 import BeautifulSoup
import pickle
from SPARQLWrapper import SPARQLWrapper, JSON
import sys
import time
from urllib.error import HTTPError, URLError
import geopandas as gpd
import geoplot
import geoplot.crs as gcrs
from shapely.geometry import shape, Point
from geonames_accounts import geonames_users
from ast import literal_eval
import math

#%% genrals
query = """SELECT DISTINCT ?item ?itemLabel WHERE {
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],mul,en". }
  {
    SELECT DISTINCT ?item WHERE {
      ?item p:P39 ?statement0.
      ?statement0 (ps:P39/(wdt:P279*)) wd:Q1515704.
    }
  }
}
    """
user_agent = "WDQS-example Python/%s.%s" % (sys.version_info[0], sys.version_info[1])
sparql = SPARQLWrapper("https://query.wikidata.org/sparql", agent=user_agent)
sparql.setQuery(query)
sparql.setReturnFormat(JSON)
while True:
    try:
        data = sparql.query().convert()
        break
    except HTTPError:
        time.sleep(2)
    except URLError:
        time.sleep(5)

generals = [{e.get('item').get('value'): e.get('itemLabel').get('value')} for e in data.get('results').get('bindings')]
generals = [e for e in generals if e != {'http://www.wikidata.org/entity/Q64782473': 'Ignatius of Loyola (fictional character)'}]

def get_coordinates(qid):
    
    qid = 
    query = f"""
    SELECT ?birthCoord ?deathCoord WHERE {{
      wd:{qid} wdt:P19 ?birthPlace;
                wdt:P20 ?deathPlace.
      ?birthPlace wdt:P625 ?birthCoord.
      ?deathPlace wdt:P625 ?deathCoord.
    }}
    """
    user_agent = "WDQS-example Python/%s.%s" % (sys.version_info[0], sys.version_info[1])
    sparql = SPARQLWrapper("https://query.wikidata.org/sparql", agent=user_agent)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    while True:
        try:
            data = sparql.query().convert()
            break
        except HTTPError:
            time.sleep(2)
        except URLError:
            time.sleep(5)
    binding = data["results"]["bindings"][0]

    # Parse "Point(lon lat)" into (lat, lon)
    def parse_point(p):
        point = p["value"].replace("Point(", "").replace(")", "").split()
        lon, lat = map(float, point)
        return lat, lon

    birth_coords = parse_point(binding["birthCoord"])
    death_coords = parse_point(binding["deathCoord"])
    return birth_coords, death_coords

def haversine(coord1, coord2):
    R = 6371.0  # Earth radius in kilometers
    lat1, lon1 = map(math.radians, coord1)
    lat2, lon2 = map(math.radians, coord2)
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1)*math.cos(lat2)*math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

for e in generals:
    e = generals[0]
    qid = list(e.keys())[0].split('/')[-1]
    birth, death = get_coordinates(qid)
    dist = haversine(birth, death)
    print(f"Distance for {qid}, {list(e.values())[0]}: {dist:.2f} km")


#%% participants

query = """SELECT DISTINCT ?item ?itemLabel WHERE {
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],mul,en". }
  {
    SELECT DISTINCT ?item WHERE {
      ?item p:P611 ?statement0.
      ?statement0 (ps:P611/(wdt:P279*)) wd:Q36380.
    }
  }
}
    """
user_agent = "WDQS-example Python/%s.%s" % (sys.version_info[0], sys.version_info[1])
sparql = SPARQLWrapper("https://query.wikidata.org/sparql", agent=user_agent)
sparql.setQuery(query)
sparql.setReturnFormat(JSON)
while True:
    try:
        data = sparql.query().convert()
        break
    except HTTPError:
        time.sleep(2)
    except URLError:
        time.sleep(5)










def get_wikidata_qid_from_viaf(viaf_id):
    #viaf_id = '9891285'
    # endpoint_url = "https://query.wikidata.org/sparql"
    query = """SELECT DISTINCT ?item ?itemLabel WHERE {
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],mul,en". }
  {
    SELECT DISTINCT ?item WHERE {
      ?item p:P611 ?statement0.
      ?statement0 (ps:P611/(wdt:P279*)) wd:Q36380.
    }
  }
}
    """
    user_agent = "WDQS-example Python/%s.%s" % (sys.version_info[0], sys.version_info[1])
    sparql = SPARQLWrapper("https://query.wikidata.org/sparql", agent=user_agent)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    while True:
        try:
            data = sparql.query().convert()
            break
        except HTTPError:
            time.sleep(2)
        except URLError:
            time.sleep(5)
    try:
        qid_url = data["results"]["bindings"][0]["item"]["value"]
        qid = qid_url.split("/")[-1]
        return qid
    except (IndexError, KeyError):
        return None