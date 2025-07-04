import requests
from concurrent.futures import ThreadPoolExecutor
import json
from collections import Counter
import pandas as pd
import regex as re
from tqdm import tqdm
from datetime import date

#%% def

def create_temp_dict(record):
    temp_dict = {
        'id': record.get('id'),
        'abstract': ' '.join([k for k,v in record.get('abstract_inverted_index').items()]) if pd.notnull(record.get('abstract_inverted_index')) else None,
        'title': record.get('title'),
        'publication_year': record.get('publication_year'),
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
        'referenced_works': record.get('referenced_works'),
        'biblio_first_page': record.get('biblio').get('first_page'),
        'biblio_issue': record.get('biblio').get('issue'),
        'biblio_last_page': record.get('biblio').get('last_page'),
        'biblio_volume': record.get('biblio').get('volume'),
        'primary_location_id': record.get('primary_location').get('source').get('id') if record.get('primary_location') and record.get('primary_location').get('source') else None,
        'primary_location_name': record.get('primary_location').get('source').get('display_name') if record.get('primary_location') and record.get('primary_location').get('source') else None,
        'primary_location_issn_l': record.get('primary_location').get('source').get('issn_l') if record.get('primary_location') and record.get('primary_location').get('source') and record.get('primary_location').get('source').get('issn_l') else None,
        'primary_location_issn': ','.join(record.get('primary_location').get('source').get('issn')) if record.get('primary_location') and record.get('primary_location').get('source') and record.get('primary_location').get('source').get('issn') else None,
        'keywords': record.get('keywords'),
        'concepts_names': '|'.join([e.get('display_name') for e in record.get('concepts')]),
        'concepts_wikidata': '|'.join([e.get('wikidata') for e in record.get('concepts')]),
        'topics_names': '|'.join([e.get('display_name') for e in record.get('topics')]),
        'topics_ids': '|'.join([e.get('id') for e in record.get('topics')])
        }
    return temp_dict
    
#%% harvesting openalex

#record = results[0]
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
    json.dump(list_of_records, f) #32,311 recors as of 18.06.2025
    
# list_of_records[0].get('authorships')
# max([len(e.get('authorships')) for e in list_of_records])
# test = [e for e in list_of_records if len(e.get('authorships')) == 100]

#%% proximity search

# Zbiory słów z wildcardami
set_A = [r"Open\w*", r"Citizen"]
set_B = [r"Scien\w*", r"Data", r"Access", r"Method\w*", r"Research", r"Humanities", r"Scholar\w*", r"Infrastructure\w*"]
set_C = [r"Theor\w*", r"Understanding\w*", r"Concept\w*", r"Philosoph\w*", r"Criti\w*", r"Value\w*", 
         r"Ethic\w*", r"epistem\w*", r"Manifest\w*", r"Meaning\w*", r"Idea\w*", r"Premise\w*", r"Discourse\w*"]

def matches_any(word, pattern_list):
    return any(re.fullmatch(p, word, flags=re.IGNORECASE) for p in pattern_list)

def proximity_query_bool(text, window_C=2):
    tokens = re.findall(r'\w+', text)

    for i in range(len(tokens) - 1):
        # Szukaj par A-B lub B-A (W/0)
        if matches_any(tokens[i], set_A) and matches_any(tokens[i+1], set_B):
            ab_indices = [i, i+1]
        elif matches_any(tokens[i], set_B) and matches_any(tokens[i+1], set_A):
            ab_indices = [i, i+1]
        else:
            continue

        # Szukaj C w promieniu ±2 wokół A i B
        min_index = max(0, min(ab_indices) - window_C)
        max_index = min(len(tokens), max(ab_indices) + window_C + 1 + 1)

        for j in range(min_index, max_index):
            if j in ab_indices:
                continue  # pomijamy słowa A i B
            if matches_any(tokens[j], set_C):
                return True  # znalazło dopasowanie

    return False

# # 🔍 Przykład:
# text = "The philosophical idea of open science and data access is growing."
# text = "Open Science Politics and Ethics"
# text = "Open Science Ethics and Politics"
# text = "Ethics and Politics of Open Science"
# print(proximity_query_bool(text))  # ➜ True albo False

# def proximity_query(text, window_AB=0, window_C=2):
#     tokens = re.findall(r'\w+', text)  # proste tokenizowanie, tylko słowa
#     results = []

#     for i in range(len(tokens)):
#         # Sprawdź czy token i to A
#         if matches_any(tokens[i], set_A):
#             # Szukaj B w oknie W/0
#             if i + 1 < len(tokens) and matches_any(tokens[i + 1], set_B):
#                 ab_index = i
#             elif i - 1 >= 0 and matches_any(tokens[i - 1], set_B):
#                 ab_index = i - 1
#             else:
#                 continue

#             # Teraz sprawdź C w odległości 2 słów od A-B
#             start = max(0, ab_index - window_C)
#             end = min(len(tokens), ab_index + window_C + 2)

#             for j in range(start, end):
#                 if matches_any(tokens[j], set_C):
#                     results.append({
#                         "C_word": tokens[j],
#                         "A_word": tokens[i],
#                         "B_word": tokens[i + 1] if ab_index == i else tokens[i - 1],
#                         "position_C": j,
#                         "position_A": i,
#                         "position_B": ab_index if ab_index != i else (i + 1 if ab_index == i else i - 1)
#                     })
#     return results
# sample_text = "The philosophical idea of open science and data access is growing. Understanding citizen research is crucial."
# sample_text = "Open Science Politics and Ethics"
# sample_text = "Open Science Ethics and Politics"
# proximity_query(sample_text)

correct_records = []

for r in tqdm(list_of_records):
    # r = list_of_records[0]
    if any(pd.notnull(e) and proximity_query_bool(e) for e in [r.get('abstract'), r.get('title')]):
        correct_records.append(r)

file_path = "data/SCIROS_openalex_TOS_proximity.json"
with open(file_path, 'w', encoding='utf-8') as f:
    json.dump(correct_records, f)

#%% handling the authors fields and keywords fields
# path = r"data/SCIROS_openalex_TOS.json"
path = "data/SCIROS_openalex_TOS_proximity.json"

with open(path) as f:
    d = json.load(f)

# test = [[el.get('author').get('id') for el in e.get('authorships')] for e in d]
# test = [e for sub in test for e in sub]

# counter_authors = Counter(test)
# counter_authors.most_common(10)

# x = 'https://openalex.org/A5018769425'
# y = []
# for e in d:
#     for el in e.get('authorships'):
#         if x == el.get('author').get('id'):
#             y.append(e)
            
#[e.get('title') for e in y]
# d = list_of_records
for e in d:
    # e = d[0]
    authors_list = []
    for a in e.get('authorships'):
        # a = e.get('authorships')[0]
        a_name = a.get('author').get('display_name')
        a_orcid = a.get('author').get('orcid') if a.get('author').get('orcid') else 'no_orcid'
        a_id = a.get('author').get('id') if a.get('author').get('id') else 'no_author_id'
        a_string = f'{a_name}|{a_orcid}|{a_id}'
        affiliations = a.get('affiliations')
        affiliation_list = []
        for i, af in enumerate(affiliations, 1):
            af_name = af.get('raw_affiliation_string')
            af_ids = ','.join(af.get('institution_ids'))
            af_string = f'{i};{af_name};{af_ids}'
            affiliation_list.append(af_string)
        affiliation_string = '|'.join(affiliation_list)
        a_string = f'{a_string}${affiliation_string}'
        authors_list.append(a_string)
    authors_list = '❦'.join(authors_list)
    e.update({'authors': authors_list})
    e.pop('authorships')
    
for e in d:
    #e = d[0]
    keywords_list = []
    for i, k in enumerate(e.get('keywords'), 1):
        k_id = k.get('id')
        k_name = k.get('display_name')
        k_string = f'{i};{k_id};{k_name}'
        keywords_list.append(k_string)
    keywords_string = '|'.join(keywords_list)
    e.update({'keywords': keywords_string})
    
df_sample = pd.DataFrame(d)
df_sample.to_excel(f'data/SCIROS_TOS_openalex_proximity_{date.today()}.xlsx', index=False) #1361 records as of 18.06.2025








    
    
    
    
    
    
    
    