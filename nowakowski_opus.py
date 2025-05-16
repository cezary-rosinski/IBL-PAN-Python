from my_functions import gsheet_to_df
import pandas as pd

#%%

df_texts = gsheet_to_df('1ubUo9sdYQgH3xBpmi4FRDMRdTeuoEBbtrvmgHyH0wYg', 'Arkusz1')
df_people = gsheet_to_df('1ubUo9sdYQgH3xBpmi4FRDMRdTeuoEBbtrvmgHyH0wYg', 'Arkusz2')

people_dict = dict(zip(df_people['Person:'].to_list(), df_people['Person Wikidata:'].to_list()))
people_dict = {k:v for k,v in people_dict.items() if isinstance(v, str) and 'wikidata' in v}

#%%
key_authors = [[el.strip() for el in e.split(';')] if isinstance(e, str) else None for e in df_texts['Key authors cited:'].to_list()]

key_authors_wikidata = []
for i, authors in enumerate(key_authors):
    if authors:
        key_authors_wikidata.append('; '.join([people_dict.get(e, 'no Wikidata ID') for e in authors]))
    else: key_authors_wikidata.append(None)

key_authors_df = pd.DataFrame(key_authors_wikidata)

test = pd.concat([df_texts[['Key authors cited:']], key_authors_df], axis=1)
test['count_authors'] = test['Key authors cited:'].apply(lambda x: x.count(';') if isinstance(x, str) else 0)
test['count_wiki'] = test[0].apply(lambda x: x.count(';') if isinstance(x, str) else 0)


response = [[el.strip() for el in e.split(';')] if isinstance(e, str) else None for e in df_texts['Response to author:'].to_list()]
response_wikidata = []
for i, authors in enumerate(response):
    if authors:
        response_wikidata.append('; '.join([people_dict.get(e, 'no Wikidata ID') for e in authors]))
    else: response_wikidata.append(None)

response_df = pd.DataFrame(response_wikidata)

test = pd.concat([df_texts[['Response to author:']], response_df], axis=1)
test['count_response'] = test['Response to author:'].apply(lambda x: x.count(';') if isinstance(x, str) else 0)
test['count_wiki'] = test[0].apply(lambda x: x.count(';') if isinstance(x, str) else 0)


dedicated_to = [[el.strip() for el in e.split(';')] if isinstance(e, str) else None for e in df_texts['Dedicated to'].to_list()]
dedicated_to_wikidata = []
for i, authors in enumerate(dedicated_to):
    if authors:
        dedicated_to_wikidata.append('; '.join([people_dict.get(e, 'no Wikidata ID') for e in authors]))
    else: dedicated_to_wikidata.append(None)

dedicated_to_df = pd.DataFrame(dedicated_to_wikidata)

test = pd.concat([df_texts[['Dedicated to']], dedicated_to_df], axis=1)
test['count_response'] = test['Dedicated to'].apply(lambda x: x.count(';') if isinstance(x, str) else 0)
test['count_wiki'] = test[0].apply(lambda x: x.count(';') if isinstance(x, str) else 0)









