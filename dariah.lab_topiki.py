from glob import glob
import requests
import xml.etree.ElementTree as et
import lxml.etree
from bs4 import BeautifulSoup
from tqdm import tqdm
import regex as re
import zipfile
import os
from concurrent.futures import ThreadPoolExecutor
import json
import pandas as pd
import pickle
from my_functions import gsheet_to_df
import pandas as pd

#%%

path = r"D:\IBL\CLARIN\Topiki\pierwsze\results_fextor/"
files = [f for f in glob(f"{path}*", recursive=True)]

results = {}
for file in tqdm(files):
    # file = files[0]
    
    with open(file, encoding='utf-8') as f:
        file_content = json.load(f)
    
    for k,v in file_content['base'].items():
        results.setdefault(k, 0)
        results[k] += v
    
df = pd.DataFrame().from_dict(results, orient='index').reset_index().rename(columns={'index': 'word', 0: 'frequency'}).sort_values('frequency', ascending=False)

short = [e for e in df['word'].to_list() if len(e) <= 4]

stoplista = ['ich', 'es', 'bit', 'akt', 'ram', 'ya', 'dit', 'sem', 'ara', 'www', 'jeu', 'fu', 'air', 'chat', 'lay', 'lid', 'niti', 'kons', 'iter', 'uta', 'quiz', 'lai', 'nó', 'sms', 'gnom', 'perl', 'sao', 'cv', 'rete', 'gag', 'trik', 'mote', 'kita', 'lais', 'waka', 'zoil', 'bryk', 'peon', 'aube', 'lti', 'zaju', 'clou', 'bejt', 'buli', 'bajt', 'klip', 'naru', 'feng', 'yarn', 'dima', 'lef', 'arai', 'aton', 'ikos', 'kabi', 'scop', 'loa', 'entw', 'rpg', 'leis', 'kwiz', 'saga', 'łam', 'jow', 'klio', 'dtp', 'ha']

df = df.loc[~df['word'].isin(stoplista)]
