import requests
from concurrent.futures import ThreadPoolExecutor
import json
from collections import Counter

#%% def

def create_temp_dict(record):
    temp_dict = {
        'title': record.get('title'),
        'publication_year': record.get('publicationo_year'),
        'publication_date': record.get('publication_date'),
        'openalex_id': record.get('ids').get('openalex') if 'openalex' in record.get('ids') else None,
        'doi_id': record.get('ids').get('doi') if 'doi' in record.get('ids') else None,
        'mag_id': record.get('ids').get('mag') if 'mag' in record.get('ids') else None,
        'pmid_id': record.get('ids').get('pmid') if 'pmid' in record.get('ids') else None,
        'pmcid_id': record.get('ids').get('pmcid') if 'pmcid' in record.get('ids') else None,
        'language': record.get('language'),
        # 'license': record.get('locations').get('license'),
        'type': record.get('type'),
        'open_access': record.get('open_access').get('is_oa'),
        'open_access_status': record.get('open_access').get('oa_status'),
        'authorships': record.get('authorships'),
        'cited_by_count': record.get('cited_by_count'),
        'topic_name': record.get('primary_topic').get('display_name') if record.get('primary_topic') else None,
        'topic_field': record.get('primary_topic').get('field').get('display_name') if record.get('primary_topic') else None,
        'referenced_works': record.get('referenced_works')
        }
    return temp_dict
    
#%%

file_path = "data/SCIROS_openalex_TOS.json"

# url_base = 'https://api.openalex.org/works?filter=abstract.search:open%20science&&per-page=100'
url_base = 'https://api.openalex.org/works?filter=type:book|article|book-chapter|editorial|report,title_and_abstract.search:%28%28%22Open%20Access%22%20OR%20%22Citizen%20Science%22%20OR%20%22Open%20Science%22%20OR%20%22Open%20Methods%22%20OR%20%22Open%20Research%20Methods%22%20OR%20%22Open%20Humanities%22%20OR%20%22Open%20Infrastructure%22%20OR%20%22Open%20Research%20Infrastructure%22%20OR%20%22Open%20Scholarship%22%29%20AND%20%28Theories%20OR%20Understandings%20OR%20Concepts%20OR%20Philosophies%20OR%20Critiques%20OR%20Values%20OR%20Epistemologies%20OR%20Manifestos%20OR%20Meanings%20OR%20Ideas%20OR%20Premises%20OR%20Discourses%29%29&per-page=100'
cursor = '*'
url = f"{url_base}&cursor={cursor}"

r = requests.get(url).json()
results = r.get('results')

list_of_records = []
for record in results:
    # record = results[0]
    if record.get('language') == 'en':
        list_of_records.append(create_temp_dict(record))

# with open(file_path, "w", encoding="utf-8") as f:
#     f.write("[\n")  # Początek listy JSON

# for i, entry in enumerate(list_of_records):
#     with open(file_path, "a", encoding="utf-8") as f:
#         json.dump(entry, f, ensure_ascii=False)
#         if i < len(list_of_records) - 1:
#             f.write(",\n") 

cursor = r.get('meta').get('next_cursor')
total_no_of_records = r.get('meta').get('count')
iteration = r.get('meta').get('per_page')
while cursor:
    url = f"{url_base}&cursor={cursor}"
    r = requests.get(url).json()
    results = r.get('results')
    
    # list_of_records = []
    for record in results:
        if record.get('language') == 'en':
            list_of_records.append(create_temp_dict(record))
            
    # for i, entry in enumerate(list_of_records):
    #     with open(file_path, "a", encoding="utf-8") as f:
    #         json.dump(entry, f, ensure_ascii=False)
    #         if i < len(list_of_records) - 1:
    #             f.write(",\n") 
    
    cursor = r.get('meta').get('next_cursor')
    iteration += r.get('meta').get('per_page')
    print("{:.0%}".format(iteration/total_no_of_records))
    
# Zamykamy listę JSON
# with open(file_path, "a", encoding="utf-8") as f:
#     f.write("\n]")

with open(file_path, 'w', encoding='utf-8') as f:
    json.dump(list_of_records, f)
    
list_of_records[0].get('authorships')
max([len(e.get('authorships')) for e in list_of_records])
test = [e for e in list_of_records if len(e.get('authorships')) == 100]
#%% calculating the authors
path = r"C:\Users\Cezary\Downloads\SCIROS_openalex_TOS.json"

with open(path) as f:
    d = json.load(f)

test = [[el.get('author').get('id') for el in e.get('authorships')] for e in d]
test = [e for sub in test for e in sub]

counter_authors = Counter(test)
counter_authors.most_common(10)

x = 'https://openalex.org/A5018769425'
y = []
for e in d:
    for el in e.get('authorships'):
        if x == el.get('author').get('id'):
            y.append(e)
            
[e.get('title') for e in y]
y = [e for e in d if [x in el.get('author').get('id') for el in e.get('authorships')]]    
#%% Magda

'https://api.openalex.org/works?search=("Open Science" AND (Theory OR Data OR Access OR Method OR Discourse OR Research OR Humanities OR "Scholarly communication" OR Infrastructure))&sort=publication_year:desc'

'https://api.openalex.org/works?search=("Open Science" AND (Theory OR "Scholarly communication"))&sort=publication_year:desc'
    
    
x = "open scence jest super theory"

if 'open science' in x and ('theory' in x or 'data' in x or 'access' in x or 'method' in x or 'discourse' in x or 'research' in x or 'humanities' in x or 'scholarly communication' in x or 'infrastructure' in x):
    print(True)
    
    
    
    
    
    
    
    