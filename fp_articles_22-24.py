from __future__ import unicode_literals
import requests
from bs4 import BeautifulSoup
import pandas as pd
import regex as re
import time
from time import mktime
from tqdm import tqdm  #licznik
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

#%%

urls = {'28-29': 'https://pressto.amu.edu.pl/index.php/fp/issue/view/2602',
        '30': 'https://pressto.amu.edu.pl/index.php/fp/issue/view/2659',
        '31': 'https://pressto.amu.edu.pl/index.php/fp/issue/view/2686',
        '32': 'https://pressto.amu.edu.pl/index.php/fp/issue/view/2727',
        '33-34': 'https://pressto.amu.edu.pl/index.php/fp/issue/view/2807',
        '35': 'https://pressto.amu.edu.pl/index.php/fp/issue/view/2847'}

results = []

for k,v in tqdm(urls.items()):
    # v = 'https://pressto.amu.edu.pl/index.php/fp/issue/view/2602'
    html_text = requests.get(v).text
    soup = BeautifulSoup(html_text, 'html.parser')
    authors = [e.text.strip() for e in soup.select('.article-summary-authors')]
    titles = [e.text.strip() for e in soup.select('.article-summary-title a')]
    dois = [e.text.strip() for e in soup.select('.article-summary-doi a')]
    
    iteration = [', '.join(e) for e in zip(authors, titles, dois)]
    results.extend(iteration)
    
with open('data/FP_articles.txt', 'w') as f:
    for line in results:
        f.write(f"{line}.\n")
    

    
    