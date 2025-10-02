import pandas as pd
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import regex as re

#%% reading data

df_artemis = pd.read_csv('data/vienna_workshop_2025/artemis_dataset_release_v0.csv')
df_ola = pd.read_csv('data/vienna_workshop_2025/ola_dataset_release_v0.csv')

#%% data reshaping

painting_list = df_artemis['painting'].drop_duplicates().to_list()
painters_set = set([e.split('_')[0].replace('.','-').replace('--','-') for e in painting_list])
painters_urls = [f"https://www.wikiart.org/en/{e}" for e in painters_set]

#%% painters wikiart url check
def check_painter_wikiart_url(url):
    html_text = requests.get(url)
    if html_text.status_code != 200:
        errors.append(url)
    
errors = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(check_painter_wikiart_url, painters_urls),total=len(painters_urls)))
    
painters_urls = [e for e in painters_urls if e not in errors]
painters_set = [e for e in painters_set if e not in [el.split('/')[-1] for el in errors]]
#%%wikiart webscraping
def scrape_artist_info(url):

    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        artist_data = {}
        
        # Find all list items in the artist info section
        info_items = soup.find_all('li', class_='dictionary-values')
        
        for li in info_items:
            # Find the label (in <s> tag)
            label_tag = li.find('s')
            if not label_tag:
                continue
            
            label = label_tag.get_text(strip=True).rstrip(':')
            
            # Find all links within this list item (after the label)
            links = li.find_all('a')
            
            if links:
                # Store all links as a list
                artist_data[label] = [link.get_text(strip=True) for link in links]
            else:
                # No links - get plain text
                # Remove the label from the text
                full_text = li.get_text(strip=True)
                text = full_text.replace(label + ':', '').strip()
                if text:
                    # Store as single-element list
                    artist_data[label] = [text]
        artist_data['wikiart_url'] = url
        wikiart_response.append(artist_data)
        # return artist_data
    
    except:
        errors.append(url)

wikiart_response = []
errors = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(scrape_artist_info, painters_urls),total=len(painters_urls)))
    
#wikiart label
def get_wikiart_label(url):
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        label = soup.find('h3').text.strip()
        wikiart_labels.update({url:label})
    except (AttributeError, TypeError):
        errors.append(url)

wikiart_labels = {}
errors = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_wikiart_label, painters_urls),total=len(painters_urls)))
wikiart_labels.update({'https://www.wikiart.org/en/remedios-varo': 'Remedios Varo'})

for e in wikiart_response:
    e.update({'label': wikiart_labels.get(e.get('wikiart_url'))})
#%% wikipedia connection
def get_wikipedia_page(url):
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        wikipedia = soup.find('a', {'class': 'truncate external'})['href']
        wikiart_wikipedia.update({url:wikipedia})
    except TypeError:
        errors.append(url)

wikiart_wikipedia = {}
errors = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_wikipedia_page, painters_urls),total=len(painters_urls)))
wikiart_wikipedia.update({'https://www.wikiart.org/en/istvan-ilosvai-varga': 'https://hu.wikipedia.org/wiki/Ilosvai_Varga_Istv%C3%A1n'})

painters_urls = [e for e in painters_urls if e not in errors]
wikiart_response = [e for e in wikiart_response if e.get('wikiart_url') in painters_urls]

for e in wikiart_response:
    e.update({'wikipedia_url': wikiart_wikipedia.get(e.get('wikiart_url'))})

# url = 'https://www.wikiart.org/en/jean-dubuffet'
#%% wikidata connection
wikipedia_urls = set(wikiart_wikipedia.values())
def get_wikidata_id(wikipedia_url):
    try:
        # wikipedia_url = list(wikipedia_urls)[0]
        lang = re.findall('(.{2})(?=\.wikipedia)', wikipedia_url)[0]
        title = wikipedia_url.split('/')[-1]
        url = f'https://{lang}.wikipedia.org/w/api.php?action=query&prop=pageprops&titles={title}&format=json'
        headers = {'User-Agent': 'CoolBot/0.0 (https://example.org/coolbot/; coolbot@example.org)'}
        response = requests.get(url, headers=headers).json()
        page_id = list(response.get('query').get('pages').keys())[0]
        wikidata_id = response.get('query').get('pages').get(page_id).get('pageprops').get('wikibase_item')
        wikipedia_wikidata.update({wikipedia_url: wikidata_id})
    except:
        errors.append(wikipedia_url)

wikipedia_wikidata = {}
errors = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_wikidata_id, wikipedia_urls),total=len(wikipedia_urls)))

#%% wikidata enrichment

url = f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json'
headers = {'User-Agent': 'CoolBot/0.0 (https://example.org/coolbot/; coolbot@example.org)'}

response = requests.get(url, headers=headers)

#%% RDF generation