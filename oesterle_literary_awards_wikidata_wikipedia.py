import requests
from bs4 import BeautifulSoup
from my_functions import gsheet_to_df
import regex as re
from tqdm import tqdm
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import pywikibot
from bs4 import BeautifulSoup
from pywikibot import textlib
from pywikibot.exceptions import NoPageError
from collections import Counter

#%% def

def get_wikidata_data(x):
    try:
        if x.get('mainsnak').get('datavalue').get('value').get('id'):
            return f"www.wikidata.org/wiki/{x.get('mainsnak').get('datavalue').get('value').get('id')}"
        elif x.get('mainsnak').get('datavalue').get('value').get('time'):
            return x.get('mainsnak').get('datavalue').get('value').get('time')
        elif x.get('mainsnak').get('datavalue').get('value').get('text'):
            return x.get('mainsnak').get('datavalue').get('value').get('text')
        elif x.get('mainsnak').get('datavalue').get('value').get('amount'):
            return list(x.get('mainsnak').get('datavalue').get('value').values())
    except AttributeError:
        return x.get('mainsnak').get('datavalue').get('value')

def get_wikidata_label(wikidata_url):   
# for wikidata_url in wikidata_urls:
    # wikidata_url = wikidata_urls[0]
    wikidata_id = re.findall('Q.+$', wikidata_url)[0]
    url = f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json'
    result = requests.get(url).json()
    try:
        label = result.get('entities').get(wikidata_id).get('labels').get('en').get('value')
    except AttributeError:
        try:
            label = result.get('entities').get(wikidata_id).get('labels').get('de').get('value')
        except AttributeError:
            label = None
    wikidata_urls_dict.update({wikidata_url: label})
#%% reading literary awards spreadsheet

literary_awards = gsheet_to_df('1satFAR9-5DFGwk-PFrgcWVOBMahNs7RcQ3dY1BvLVvM', 'Awards')
literary_awards_wikidata = literary_awards.loc[literary_awards['Wikidata ID'].str.contains('wikidata', na=False)]
literary_awards_wikidata_list = literary_awards_wikidata['Wikidata ID'].to_list()
literary_awards_wikidata_list_ids = set([re.findall('Q.+$', e)[0] for e in literary_awards_wikidata_list])

#%% reading wikidata mapping

wikidata_mapping = gsheet_to_df('1satFAR9-5DFGwk-PFrgcWVOBMahNs7RcQ3dY1BvLVvM', 'wikidata mapping')
wikidata_properties = set([e for e in wikidata_mapping['Wikidata property'].to_list() if not isinstance(e, float)])
davids_model = wikidata_mapping["David's model"].to_list()
davids_model_with_properties = {k:v for k,v in dict(zip(wikidata_mapping['Wikidata property'], wikidata_mapping["David's model"])).items() if not isinstance(k, float)}

wikidata_response_dict = {}
wikipedia_urls = {}
for wikidata_id in tqdm(literary_awards_wikidata_list_ids):
    # wikidata_id = list(literary_awards_wikidata_list_ids)[0]
    # wikidata_id = 'Q37922'
    # wikidata_id = 'Q159909'
    url = f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json'
    result = requests.get(url).json()
    wikipedia_links = result.get('entities').get(wikidata_id).get('sitelinks')
    wikipedia_urls.update({wikidata_id: wikipedia_links})
    try:
        english_title = [e.get('value') for e in result.get('entities').get(wikidata_id).get('aliases').get('en')][0]
    except TypeError:
        english_title = None
    claims = {k:v for k,v in result.get('entities').get(wikidata_id).get('claims').items() if k in wikidata_properties}
    doe_model = {davids_model_with_properties.get(k):[get_wikidata_data(e) for e in v] for k,v in claims.items()}
    doe_model.update({'Official title in English': english_title})
    wikidata_response_dict.update({wikidata_id: doe_model})
    
df = pd.DataFrame.from_dict(wikidata_response_dict, orient='index')
df['Endowment'] = df['Endowment'].apply(lambda x: [e for sub in x for e in sub] if isinstance(x, list) else x)
df['Frequency'] = df['Frequency'].apply(lambda x: [e for sub in x for e in sub] if isinstance(x, list) else x)

wikidata_urls = [[[ele for ele in el if not isinstance(ele, float) and 'wiki' in ele] if not isinstance(el, float) and el is not None else None for el in df[e].to_list()] for e in df.columns.values]
wikidata_urls = [e for sub in wikidata_urls for e in sub if e]
wikidata_urls = [e for sub in wikidata_urls for e in sub]
       
wikidata_urls_dict = {}
with ThreadPoolExecutor() as executor:
    list(tqdm(executor.map(get_wikidata_label,wikidata_urls), total=len(wikidata_urls)))
    
df_labels = df.copy()
for column in df_labels:
    df_labels[column] = df_labels[column].apply(lambda x: [wikidata_urls_dict.get(e, e) for e in x] if isinstance(x, list) else x)

#%% winners from wikipedia
# german awards
wikipedia_urls_from_table = literary_awards.loc[literary_awards['Wikidata ID'].str.contains('wikipedia', na=False)]['Wikidata ID'].to_list()
wikipedia_urls_de = {k:v.get('dewiki').get('url') for k,v in wikipedia_urls.items()}
wikipedia_urls_de_list = list(wikipedia_urls_de.values())
wikipedia_urls_de_list.extend(wikipedia_urls_from_table)

wikipedia_labels_de = {k:v.get('dewiki').get('title') for k,v in wikipedia_urls.items()}
wikipedia_labels_de_list = list(wikipedia_labels_de.values())
[wikipedia_labels_de_list.append(e.split('/')[-1]) for e in wikipedia_urls_from_table]


#%% Polish and Czech awards
def return_wikidata_id(x):
    try:
        return x.data_item().title()
    except NoPageError:
        return None
    
#pl
site = pywikibot.Site('pl', 'wikipedia')
cat = pywikibot.Category(site, "Polskie_nagrody_literackie")
polish_awards_wikipedia = [(e.title(), e.full_url(), return_wikidata_id(e)) for e in list(cat.articles())]
df_pl = pd.DataFrame(polish_awards_wikipedia, columns=['title', 'wikipedia', 'wikidata'])
df_pl['county origin'] = 'PL'

#cz
site = pywikibot.Site('cs', 'wikipedia')
cat = pywikibot.Category(site, "%C4%8Cesk%C3%A9_liter%C3%A1rn%C3%AD_ceny")
czech_awards_wikipedia = [(e.title(), e.full_url(), return_wikidata_id(e)) for e in list(cat.articles())]
df_cz = pd.DataFrame(czech_awards_wikipedia, columns=['title', 'wikipedia', 'wikidata'])
df_cz['county origin'] = 'CZ'

#de-david
site = pywikibot.Site('de', 'wikipedia')
german_do_awards_wikipedia = [(pywikibot.Page(site, e).title(), pywikibot.Page(site, e).full_url(), return_wikidata_id(pywikibot.Page(site, e))) for e in wikipedia_labels_de_list]
df_de_david = pd.DataFrame(german_do_awards_wikipedia, columns=['title', 'wikipedia', 'wikidata'])
df_de_david['county origin'] = 'DE-DO'

#de
site = pywikibot.Site('de', 'wikipedia')
cat = pywikibot.Category(site, "Literaturpreis (Deutschland)")
german_awards_wikipedia = [(e.title(), e.full_url(), return_wikidata_id(e)) for e in list(cat.articles())]
german_awards_wikipedia = [e for e in german_awards_wikipedia if e[1] not in [el[1] for el in german_do_awards_wikipedia]]
df_de = pd.DataFrame(german_awards_wikipedia, columns=['title', 'wikipedia', 'wikidata'])
df_de['county origin'] = 'DE'

#total

df_total = pd.concat([df_pl, df_cz, df_de_david, df_de])

#%%

literary_awards = gsheet_to_df('1satFAR9-5DFGwk-PFrgcWVOBMahNs7RcQ3dY1BvLVvM', 'Awards_pl_cs_de')
literary_awards_wikidata_list_ids = set([e for e in literary_awards['wikidata'].to_list() if not isinstance(e,float)])

wikidata_mapping = gsheet_to_df('1satFAR9-5DFGwk-PFrgcWVOBMahNs7RcQ3dY1BvLVvM', 'wikidata mapping')
wikidata_properties = set([e for e in wikidata_mapping['Wikidata property'].to_list() if not isinstance(e, float)])

davids_model = wikidata_mapping["David's model"].to_list()
davids_model_with_properties = {k:v for k,v in dict(zip(wikidata_mapping['Wikidata property'], wikidata_mapping["David's model"])).items() if not isinstance(k, float)}

wikidata_response_dict = {}
wikipedia_urls = {}
for wikidata_id in tqdm(literary_awards_wikidata_list_ids):
    # wikidata_id = list(literary_awards_wikidata_list_ids)[0]
    # wikidata_id = 'Q37922'
    # wikidata_id = 'Q11789161'
    url = f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json'
    result = requests.get(url).json()
    wikipedia_links = result.get('entities').get(wikidata_id).get('sitelinks')
    wikipedia_urls.update({wikidata_id: wikipedia_links})
    try:
        english_title = result.get('entities').get(wikidata_id).get('labels').get('en').get('value')
    except AttributeError:
        english_title = None
    claims = {k:v for k,v in result.get('entities').get(wikidata_id).get('claims').items() if k in wikidata_properties}
    doe_model = {davids_model_with_properties.get(k):[get_wikidata_data(e) for e in v] for k,v in claims.items()}
    doe_model.update({'Official title in English': english_title})
    wikidata_response_dict.update({wikidata_id: doe_model})
    
df = pd.DataFrame.from_dict(wikidata_response_dict, orient='index')
df['Endowment'] = df['Endowment'].apply(lambda x: [e for sub in x for e in sub] if isinstance(x, list) else x)
df['Frequency'] = df['Frequency'].apply(lambda x: [e for sub in x for e in sub] if isinstance(x, list) else x)

wikidata_urls = [[[ele for ele in el if not isinstance(ele, float) and 'wiki' in ele] if not isinstance(el, float) and el is not None else None for el in df[e].to_list()] for e in df.columns.values]
wikidata_urls = [e for sub in wikidata_urls for e in sub if e]
wikidata_urls = [e for sub in wikidata_urls for e in sub]
       
wikidata_urls_dict = {}
with ThreadPoolExecutor() as executor:
    list(tqdm(executor.map(get_wikidata_label,wikidata_urls), total=len(wikidata_urls)))

wikidata_urls_dict['www.wikidata.org/wiki/Q21296312'] = 'Central Council of Trade Unions'
    
df_labels = df.copy()
for column in df_labels:
    df_labels[column] = df_labels[column].apply(lambda x: [wikidata_urls_dict.get(e, e) for e in x] if isinstance(x, list) else x)
    
df_id_labels = df.copy()
for column in df_id_labels:
    df_id_labels[column] = df_id_labels[column].apply(lambda x: [{e: wikidata_urls_dict.get(e, e)} for e in x] if isinstance(x, list) else x)

df_id_labels = df_id_labels[["Country of the donor-organisation", 'Year of foundation', 'Official title in English', 'Coding: Personality reference', 'Organization and coordination', 'Endowment', "Prize's homepage", 'Laureates', 'Alternative titles', 'Frequency']]

df_id_labels.to_excel('awards.xlsx')

all_columns = ["Official title in English", "Prize's homepage", "Alternative titles", "Year of foundation ", "Frequency", "Endowment", "Donor-Organisation", "Former Donor-Organisations", "Country of the donor-organisation", "Organization and coordination", "Jurors", "Reach", "Awardee profile", "Award subject", "Work awarded", "Nominees", "Laureates", "Genre", "Additional information on awarding practice", "European program", "Self-description of the Literary Prize", "Text of self-description", "Laureat speech (link)", "Laudation (link)", "Communication mode of the European program", "Coding: Value reference", "Coding: Spatial reference", "Coding: Personality reference", "Coding: Literary-aesthetic reference", "Coding: Structural-political reference", "Conditions of participation", "Formal requirements / restrictions", "Award Framework", "Remarks"]
columns_wikidata = ["Country of the donor-organisation", "Year of foundation", "Official title in English", "Coding: Personality reference", "Organization and coordination", "Endowment", "Prize's homepage", "Laureates", "Alternative titles", "Frequency"]

[e for e in all_columns if e.strip() not in columns_wikidata]

#%% 
site = pywikibot.Site('de', 'wikipedia')
page = pywikibot.Page(site, "Goethepreis der Stadt Frankfurt am Main")

page_parsed = page.get_parsed_page()


soup = BeautifulSoup(page_parsed, 'lxml')
a = soup.find_all("li", {'li': re.compile('\d{4}')})
a = soup.find_all("li")
x = [(e.text, e.a['href'], e.a['title']) for e in a if re.match('\d{4}', e.text)]


links = [e.text for e in soup.find_all('loc')]



#%% wikipedia



url = 'https://pl.wikipedia.org/wiki/Kategoria:Polskie_nagrody_literackie'
'https://cs.wikipedia.org/wiki/Kategorie:%C4%8Cesk%C3%A9_liter%C3%A1rn%C3%AD_ceny'

result = requests.get(url).content
soup = BeautifulSoup(result, 'lxml')
titles = [e.text for e in soup.select('#mw-pages a')]
links = [e['href'] for e in soup.select('#mw-pages a')]
soup.sel


























