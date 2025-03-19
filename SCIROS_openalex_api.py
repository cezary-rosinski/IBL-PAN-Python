import requests
from concurrent.futures import ThreadPoolExecutor
import json

#%%

file_path = "data/SCIROS_openalex_TOS.json"

url_base = 'https://api.openalex.org/works?filter=abstract.search:open%20science&&per-page=100'
cursor = '*'
url = f"{url_base}&cursor={cursor}"

r = requests.get(url).json()
results = r.get('results')

with open(file_path, "w", encoding="utf-8") as f:
    f.write("[\n")  # Początek listy JSON

for i, entry in enumerate(results):
    with open(file_path, "a", encoding="utf-8") as f:
        json.dump(entry, f, ensure_ascii=False)
        if i < len(results) - 1:
            f.write(",\n") 

cursor = r.get('meta').get('next_cursor')
iteration = 0
while cursor:
    url = f"{url_base}&cursor={cursor}"
    r = requests.get(url).json()
    results.extend(r.get('results'))
    
    for i, entry in enumerate(results):
        with open(file_path, "a", encoding="utf-8") as f:
            json.dump(entry, f, ensure_ascii=False)
            if i < len(results) - 1:
                f.write(",\n") 
    
    cursor = r.get('meta').get('next_cursor')
    iteration += 1
    print(iteration)
    
# Zamykamy listę JSON
with open(file_path, "a", encoding="utf-8") as f:
    f.write("\n]")
    
    
#%% Magda

'https://api.openalex.org/works?search=("Open Science" AND (Theory OR Data OR Access OR Method OR Discourse OR Research OR Humanities OR "Scholarly communication" OR Infrastructure))&sort=publication_year:desc'

'https://api.openalex.org/works?search=("Open Science" AND (Theory OR "Scholarly communication"))&sort=publication_year:desc'
    
    
x = "open scence jest super theory"

if 'open science' in x and ('theory' in x or 'data' in x or 'access' in x or 'method' in x or 'discourse' in x or 'research' in x or 'humanities' in x or 'scholarly communication' in x or 'infrastructure' in x):
    print(True)
    
    
    
    
    
    
    
    