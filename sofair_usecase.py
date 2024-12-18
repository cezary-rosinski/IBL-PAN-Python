import pandas as pd
from tqdm import tqdm
import requests
from concurrent.futures import ThreadPoolExecutor

#%%

doaj_journals = pd.read_csv(r"C:\Users\Cezary\Downloads\journalcsv__doaj_20241218_0821_utf8.csv", encoding='utf8')


doaj_journals.columns.values

doaj_journals['Keywords']
doaj_journals['Subjects']

doaj_journals['Languages in which the journal accepts manuscripts']

'Journal ISSN (print version)'
'Journal EISSN (online version)'


subjects = doaj_journals['Subjects'].to_list()
languages = set(doaj_journals['Languages in which the journal accepts manuscripts'].to_list())


# subjects_top_level = [[el.strip() for el in e.split('|')] for e in subjects]
# subjects_top_level = [[el.strip() for el in '.'.join([el.split(':')[0] for el in e]).split('.')] for e in subjects_top_level]
# subjects_top_level = set([e for sub in subjects_top_level for e in sub])


humanities_categories = ['Music and books on Music',
                         'Political science',
                         'Psychology',
                         'Language and Literature',
                         'Fine Arts',
                         'History America',
                         'Library science',
                         'History (General) and history of Europe',
                         'Philosophy',
                         'Anthropology',
                         'Education',
                         'Social Sciences',
                         'Religion',
                         'Auxiliary sciences of history']

def get_top_subject(x):
    # x = subjects[29]
    x = [e.strip() for e in x.split('|')]
    x = '. '.join([e.strip() for e in '.'.join([e.split(':')[0] for e in x]).split('.')])
    return x

doaj_journals['top_subjects'] = doaj_journals['Subjects'].apply(lambda x: get_top_subject(x))
doaj_journals['TrueFalse'] = doaj_journals['top_subjects'].apply(lambda x: True if x in humanities_categories else False)

doaj_selection = doaj_journals.loc[(doaj_journals['TrueFalse'] == True) &
                                   (doaj_journals['Languages in which the journal accepts manuscripts'].str.contains('English'))]

issns = []
for i, row in doaj_selection[['Journal ISSN (print version)', 'Journal EISSN (online version)']].iterrows():
    if row['Journal ISSN (print version)']:
        issns.append(row['Journal ISSN (print version)'])
    else: issns.append(row['Journal EISSN (online version)'])
issns = set(issns)

#%% crossref harvesting for journals

journals_metadata = []
for issn in tqdm(list(issns)): #1.5 minutes for 50 journals -- 63 minutes for 2130 journals
    # doi = list(sofair_dois)[0]
    url = 'https://api.crossref.org/journals/' + issn
    metadata = requests.get(url)
    if metadata.text != 'Resource not found.':
        metadata = metadata.json()
        journals_metadata.append({issn: metadata})
    else: journals_metadata.append({issn: None})
        
# #def
# # for issn in tqdm(issns):
# def get_journal_metadata(issn):
#     # doi = list(sofair_dois)[0]
#     url = 'https://api.crossref.org/journals/' + issn
#     metadata = requests.get(url)
#     if metadata.text != 'Resource not found.':
#         metadata = metadata.json()
#         journals_metadata.append({issn: metadata})
#     else: journals_metadata.append({issn: None})

# journals_metadata = []
# with ThreadPoolExecutor() as excecutor:
#     list(tqdm(excecutor.map(get_journal_metadata, issns),total=len(issns)))  

journals_metadata_detailed = []
for e in tqdm(journals_metadata): 
    # e = journals_metadata[2]
    if list(e.values())[0]:
        for k,v in e.items():
            # k,v = list(e.keys())[0], list(e.values())[0]
            years = [e[0] for e in v.get('message').get('breakdowns').get('dois-by-issued-year')]
            no_of_articles = sum([e[1] for e in v.get('message').get('breakdowns').get('dois-by-issued-year')])
            
            iter_dict = {
                'ISSN': k,
                'years': sorted([e[0] for e in v.get('message').get('breakdowns').get('dois-by-issued-year')]),
                'no_of_articles': sum([e[1] for e in v.get('message').get('breakdowns').get('dois-by-issued-year')])}
            journals_metadata_detailed.append(iter_dict)
            
sum([e.get('no_of_articles') for e in journals_metadata_detailed])

df_journals_metadata = pd.DataFrame(journals_metadata_detailed)

#%% crossref harvesting for journals
#ograć iterację, jeśli źródło daje więcej niż 1000 tekstóW

url = 'https://api.crossref.org/journals/1889-979X/works?rows=1000'

articles_metadata = []

for e in tqdm(journals_metadata_detailed): #5 minutes for 43 articles -- 86% of journals in crossref | 4h for 86% of journals from 2130 total
    issn = e.get('ISSN')
    url = 'https://api.crossref.org/journals/' + issn + '/works?rows=1000'
    metadata = requests.get(url).json()
    iteration = [[e.get('ISSN'), e.get('DOI'), e.get('title'), e.get('abstract')] for e in metadata.get('message').get('items')]
    articles_metadata.extend(iteration)
















