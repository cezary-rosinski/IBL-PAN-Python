import pandas as pd
from tqdm import tqdm
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
import regex as re
from SPARQLWrapper import SPARQLWrapper, JSON
from urllib.parse import unquote
import warnings
warnings.filterwarnings("ignore")

#%%

df_artemis = pd.read_csv('data/vienna_workshop_2025/artemis_dataset_release_v0.csv')
df_ola = pd.read_csv('data/vienna_workshop_2025/ola_dataset_release_v0.csv')


#%%wikiart webscraping

painting_list = set(df_artemis['painting'].to_list())
painting_dict = {k:f"https://www.wikiart.org/en/{k.replace('_','/').replace('.','-').replace('--','-')}" for k in painting_list}

painters = set([e.split('_')[0].replace('.','-').replace('--','-') for e in painting_list])
painters_urls = [f"https://www.wikiart.org/en/{e}" for e in painters]

#painters check

# def painters_check(e):
# # for e in tqdm(painters_urls):
#     if requests.get(e).status_code != 200:
#         errors.append(e)
# #def       
# errors = []       
# with ThreadPoolExecutor() as excecutor:
#     list(tqdm(excecutor.map(painters_check, painters_urls),total=len(painters_urls)))   
    
dict_refine = {'https://www.wikiart.org/en/eugã¨ne-grasset': 'https://www.wikiart.org/en/eug-ne-grasset',
               'https://www.wikiart.org/en/mestre-ataã­de': 'https://www.wikiart.org/en/mestre-ata-de',
               "https://www.wikiart.org/en/georgia-o'keeffe": 'https://www.wikiart.org/en/georgia-o-keeffe',
               "https://www.wikiart.org/en/andrea-del-verrocchio": 'https://www.wikiart.org/en/andrea-del-verrochio',
               "https://www.wikiart.org/en/pierre-paul-prud'hon": 'https://www.wikiart.org/en/pierre-paul-prud-hon',
               "https://www.wikiart.org/en/david-burliuk": 'https://www.wikiart.org/en/david-burliuk',
               "https://www.wikiart.org/en/roger-bissiã¨re": 'https://www.wikiart.org/en/roger-bissi-re',
               "https://www.wikiart.org/en/andrei-cadere": 'https://www.wikiart.org/en/andre-cadere',
               "https://www.wikiart.org/en/georges-lacombeâ\xa0": 'https://www.wikiart.org/en/georges-lacombe',
               "https://www.wikiart.org/en/allan-d'arcangelo": 'https://www.wikiart.org/en/allan-d-arcangelo',
               "https://www.wikiart.org/en/arnold-bã¶cklin": 'https://www.wikiart.org/en/arnold-bocklin',
               "https://www.wikiart.org/en/grã©goire-michonze": 'https://www.wikiart.org/en/gr-goire-michonze',
               "https://www.wikiart.org/en/antã³nio-de-carvalho-da-silva-porto": 'https://www.wikiart.org/en/ant-nio-de-carvalho-da-silva-porto',
               "https://www.wikiart.org/en/andrã©-lhote": 'https://www.wikiart.org/en/andr-lhote',
               "https://www.wikiart.org/en/fã©lix-del-marle": 'https://www.wikiart.org/en/f-lix-del-marle',
               "https://www.wikiart.org/en/joaquã­n-sorolla": 'https://www.wikiart.org/en/joaqu-n-sorolla',
               "https://www.wikiart.org/en/petro-kholodny-(elder)": 'https://www.wikiart.org/en/petro-kholodny-elder',
               "https://www.wikiart.org/en/marevna-(marie-vorobieff)": 'https://www.wikiart.org/en/marevna-marie-vorobieff',
               "https://www.wikiart.org/en/ferdinand-georg-waldmã¼ller": 'https://www.wikiart.org/en/ferdinand-georg-waldm-ller'}

painters_urls = [dict_refine.get(e, e) for e in painters_urls]
#%%
def scrape_artist_info(url):
    """
    Scrapes artist information from WikiArt artist page
    All values are returned as lists
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
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
            artist_data['url'] = [url]
        artist_wikiart_data.append(artist_data)
        # return artist_data
    
    except Exception:
        artist_data = {}
        artist_data['url'] = [url]
        artist_wikiart_data.append(artist_data)


artist_wikiart_data = []       
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(scrape_artist_info, painters_urls),total=len(painters_urls)))   

#%%

def get_painter_wikipedia(e):
# for e in painters_urls:
    # e = painters_urls[0]
    try:
        html_text = requests.get(e).text
        soup = BeautifulSoup(html_text, 'lxml')
        wikipedia = soup.find('a', {'class': 'truncate external'})['href']
        painters_wikipedia.update({e: wikipedia})
    except TypeError:
        errors.append(e)

painters_wikipedia = {}
errors = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_painter_wikipedia, painters_urls),total=len(painters_urls)))  

painters_wikipedia_from_errors = {'https://www.wikiart.org/en/adam-baltatu': 'https://ro.wikipedia.org/wiki/Adam_B%C4%83l%C8%9Batu',
                                  'https://www.wikiart.org/en/alekos-kontopoulos': 'https://fr.wikipedia.org/wiki/Al%C3%A9kos_Kond%C3%B3poulos',
                                  'https://www.wikiart.org/en/alex-hay': '',
                                  'https://www.wikiart.org/en/arturo-souto': 'https://es.wikipedia.org/wiki/Arturo_Souto',
                                  'https://www.wikiart.org/en/aurel-cojan': 'https://fr.wikipedia.org/wiki/Aurel_Cojan',
                                  'https://www.wikiart.org/en/carlos-saenz-de-tejada': 'https://es.wikipedia.org/wiki/Carlos_S%C3%A1enz_de_Tejada',
                                   'https://www.wikiart.org/en/charles-reiffel': 'https://en.wikipedia.org/wiki/Charles_Reiffel',
                                   'https://www.wikiart.org/en/constantin-blendea': '',
                                   'https://www.wikiart.org/en/cornelis-vreedenburgh': 'https://en.wikipedia.org/wiki/Cornelis_Vreedenburgh',
                                   'https://www.wikiart.org/en/costas-niarchos': '',
                                   'https://www.wikiart.org/en/david-burliuk': 'https://en.wikipedia.org/wiki/David_Burliuk',
                                   'https://www.wikiart.org/en/denise-green': 'https://en.wikipedia.org/wiki/Denise_Green',
                                   'https://www.wikiart.org/en/ding-yanyong': '',
                                   'https://www.wikiart.org/en/doug-wheeler': 'https://en.wikipedia.org/wiki/Doug_Wheeler',
                                   'https://www.wikiart.org/en/emmanuel-zairis': 'https://en.wikipedia.org/wiki/Emmanuel_Zairis',
                                   'https://www.wikiart.org/en/endre-bartos': 'https://hu.wikipedia.org/wiki/Bartos_Endre',
                                   'https://www.wikiart.org/en/franz-richard-unterberger': 'https://de.wikipedia.org/wiki/Franz_Richard_Unterberger',
                                   'https://www.wikiart.org/en/george-mavroides': '',
                                   'https://www.wikiart.org/en/guntis-strupulis': '',
                                   'https://www.wikiart.org/en/ivan-vladimirov': 'https://en.wikipedia.org/wiki/Ivan_Vladimirov',
                                   'https://www.wikiart.org/en/jean-david': 'https://de.wikipedia.org/wiki/Jean_David_(Maler)',
                                   'https://www.wikiart.org/en/jean-degottex': 'https://en.wikipedia.org/wiki/Jean_Degottex',
                                   'https://www.wikiart.org/en/joan-hernandez-pijuan': 'https://es.wikipedia.org/wiki/Joan_Hern%C3%A1ndez_Pijuan',
                                   'https://www.wikiart.org/en/jules-perahim': 'https://ro.wikipedia.org/wiki/Jules_Perahim',
                                   'https://www.wikiart.org/en/karl-schrag': 'https://en.wikipedia.org/wiki/Karl_Schrag',
                                   'https://www.wikiart.org/en/leon-berkowitz': 'https://en.wikipedia.org/wiki/Leon_Berkowitz',
                                   'https://www.wikiart.org/en/ligia-macovei': 'https://ro.wikipedia.org/wiki/Ligia_Macovei',
                                   'https://www.wikiart.org/en/lucia-demetriade-balacescu': '',
                                   'https://www.wikiart.org/en/luciano-bartolini': '',
                                   'https://www.wikiart.org/en/margareta-sterian': 'https://ro.wikipedia.org/wiki/Margareta_Sterian',
                                   'https://www.wikiart.org/en/mostafa-dashti': '',
                                   'https://www.wikiart.org/en/nikola-tanev': 'https://bg.wikipedia.org/wiki/%D0%9D%D0%B8%D0%BA%D0%BE%D0%BB%D0%B0_%D0%A2%D0%B0%D0%BD%D0%B5%D0%B2',
                                   'https://www.wikiart.org/en/octav-angheluta': 'https://ro.wikipedia.org/wiki/Octavian_Anghelu%C8%9B%C4%83',
                                   'https://www.wikiart.org/en/paul-mathiopoulos': 'https://el.wikipedia.org/wiki/%CE%A0%CE%B1%CF%8D%CE%BB%CE%BF%CF%82_%CE%9C%CE%B1%CE%B8%CE%B9%CF%8C%CF%80%CE%BF%CF%85%CE%BB%CE%BF%CF%82',
                                   'https://www.wikiart.org/en/periklis-vyzantios': 'https://el.wikipedia.org/wiki/%CE%A0%CE%B5%CF%81%CE%B9%CE%BA%CE%BB%CE%AE%CF%82_%CE%92%CF%85%CE%B6%CE%AC%CE%BD%CF%84%CE%B9%CE%BF%CF%82',
                                   'https://www.wikiart.org/en/peter-busa': '',
                                   'https://www.wikiart.org/en/petro-kholodny-elder': 'https://en.wikipedia.org/wiki/Petro_Kholodnyi',
                                   'https://www.wikiart.org/en/pino-pinelli': 'https://en.wikipedia.org/wiki/Pino_Pinelli_(painter)',
                                   'https://www.wikiart.org/en/rafael-zabaleta': 'https://es.wikipedia.org/wiki/Rafael_Zabaleta',
                                   'https://www.wikiart.org/en/ralph-rosenborg': 'https://en.wikipedia.org/wiki/Ralph_Rosenborg',
                                   'https://www.wikiart.org/en/ramon-oviedo': 'https://es.wikipedia.org/wiki/Ram%C3%B3n_Oviedo',
                                   'https://www.wikiart.org/en/robert-silvers': '',
                                   'https://www.wikiart.org/en/spyros-papaloukas': 'https://en.wikipedia.org/wiki/Spyros_Papaloukas',
                                   'https://www.wikiart.org/en/sven-lukin': '',
                                   'https://www.wikiart.org/en/theophrastos-triantafyllidis': '',
                                   'https://www.wikiart.org/en/tsuruko-yamazaki': 'https://fr.wikipedia.org/wiki/Tsuruko_Yamazaki',
                                   'https://www.wikiart.org/en/utagawa-sadatora': 'https://ja.wikipedia.org/wiki/%E6%AD%8C%E5%B7%9D%E8%B2%9E%E8%99%8E',
                                   'https://www.wikiart.org/en/vangel-naumovski': 'https://fr.wikipedia.org/wiki/Vangel_Naumovski',
                                   'https://www.wikiart.org/en/vasile-dobrian': 'https://ro.wikipedia.org/wiki/Vasile_Dobrian',
                                   'https://www.wikiart.org/en/vasile-kazar': 'https://eo.wikipedia.org/wiki/L%C3%A1szl%C3%B3_Kaz%C3%A1r',
                                   'https://www.wikiart.org/en/yov-kondzelevych': 'https://uk.wikipedia.org/wiki/%D0%99%D0%BE%D0%B2_%D0%9A%D0%BE%D0%BD%D0%B4%D0%B7%D0%B5%D0%BB%D0%B5%D0%B2%D0%B8%D1%87'}

painters_wikipedia = painters_wikipedia | painters_wikipedia_from_errors
painters_wikipedia = {k:v for k,v in painters_wikipedia.items() if v}
artist_wikiart_data = [e for e in artist_wikiart_data if e]

for i, e in enumerate(artist_wikiart_data):
    # e = artist_wikiart_data[0]
    painter_wikipedia = painters_wikipedia.get(e.get('url')[0])
    artist_wikiart_data[i]['wikipedia'] = [painter_wikipedia]
#%%

def get_wikidata_id_from_wikipedia_url(wikipedia_url):
    try:
        # wikipedia_url = 'https://de.wikipedia.org/wiki/Franz_Richard_Unterberger'
        lang = re.findall('(.{2})(?=\.wikipedia)', wikipedia_url)[0]
        url = f'https://{lang}.wikipedia.org/w/api.php?action=query&prop=pageprops&ppprop=wikibase_item&redirects=1&format=json&titles={wikipedia_url.split("/")[-1]}'
        headers = {'User-Agent': 'CoolBot/0.0 (https://example.org/coolbot/; coolbot@example.org)'}
        r = requests.get(url, headers = headers).json()
        j_key = list(r.get('query').get('pages').keys())[0]
        wikidata_id = r.get('query').get('pages').get(j_key).get('pageprops').get('wikibase_item')
        wikidata_ids.update({wikipedia_url:wikidata_id})
    except:
        print(wikipedia_url)
        
wikipedia_urls = [el for el in [e.get('wikipedia')[0] for e in artist_wikiart_data] if el]

wikidata_ids = {}
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_wikidata_id_from_wikipedia_url, wikipedia_urls),total=len(wikipedia_urls)))
    
wikidata_extra = {'https://en.wikipedia.org/wiki/Gustave_Doré': 'Q6682',
                  'https://en.wikipedia.org/wiki/Artemisia_Gentileschi': 'Q212657',
                  'https://hu.wikipedia.org/wiki/Ilosvai_Varga_Istv%C3%A1n': 'Q47090984' }

wikidata_ids = wikidata_ids | wikidata_extra
#błąd: https://www.wikiart.org/en/istvan-ilosvai-varga
#%% odpytanie wikidaty i stworzenie grafu
def get_wikidata_label(wikidata_id):
    languages = ['pl', 'en', 'fr', 'de', 'es', 'cs']
    url = f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json'
    try:
        result = requests.get(url).json()
        for lang in languages:
            label = result['entities'][wikidata_id]['labels'][lang]['value']
            break
    except ValueError:
        label = None
    return label   

wikidata_ids_set = set(wikidata_ids.values())

wikidata_id = w
#%%

for k,v in tqdm(painting_dict.items()):
    v = 'https://www.wikiart.org/en/a-y-jackson/algoma-in-november-1935'
    
    html_text = requests.get(v).text
    soup = BeautifulSoup(html_text, 'lxml')
    painter = soup.find('h5', {'itemprop': 'creator'})
    
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    
    date_of_publication = re.sub(r'(http:\/\/www\.afiszteatralny\.pl\/)(\d{4})(\/)(\d{2})(\/[\w\.\-]*)', r'\2-\4', article_link)
    text_of_article = soup.find('div', class_='post-entry')
    title_of_article = soup.find('div', class_='post-header').h1.text.strip()
    tags = " ".join([x.text.replace("\n", " ").strip() for x in soup.find_all('div', class_='entry-tags gray-2-secondary')]).replace('Tags:', '').replace(',', ' |')
    article = text_of_article.text.strip().replace('\n', '')

#https://www.wikiart.org/


#%%wikidata enrichment