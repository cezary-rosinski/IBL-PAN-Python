import pandas as pd
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import regex as re
from rdflib import Graph, Namespace, URIRef, Literal, BNode
from rdflib.namespace import RDF, RDFS, XSD, FOAF, OWL
from itertools import combinations

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

errors2 = {k for k,v in wikipedia_wikidata.items() if v is None}
errors2 = {k for k,v in wikiart_wikipedia.items() if v in errors2}
errors = {k for k,v in wikiart_wikipedia.items() if v in errors}
errors = errors | errors2
painters_urls = [e for e in painters_urls if e not in errors]
wikiart_response = [e for e in wikiart_response if e.get('wikiart_url') in painters_urls]

for e in wikiart_response:
    e.update({'wikidata_id': wikipedia_wikidata.get(e.get('wikipedia_url'))})

#%% wikidata enrichment
wikidata_ids = set(wikipedia_wikidata.values())
def harvest_wikidata(wikidata_id):
    try:
        # wikidata_id = wikiart_response[0].get('wikidata_id')
        url = f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json'
        headers = {'User-Agent': 'CoolBot/0.0 (https://example.org/coolbot/; coolbot@example.org)'}
        result = requests.get(url, headers=headers).json()
        claims = ['P21', 'P27', 'P106', 'P40', 'P1038', 'P135', 'P463', 'P136', 'P737']
        temp_dict = result.get('entities').get(wikidata_id).get('claims')
        temp_dict = {k:[e.get('mainsnak').get('datavalue').get('value').get('id') for e in v] for k,v in temp_dict.items() if k in claims}
        wikidata_response.update({wikidata_id:temp_dict})
    except:
        errors.append(wikidata_id)
    
wikidata_response = {}
errors = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(harvest_wikidata, wikidata_ids),total=len(wikidata_ids)))

errors = [e for e in errors if e]
errors = {k for k,v in wikipedia_wikidata.items() if v in errors}
errors = {k for k,v in wikiart_wikipedia.items() if v in errors}
painters_urls = [e for e in painters_urls if e not in errors]
wikiart_response = [e for e in wikiart_response if e.get('wikiart_url') in painters_urls]

#%% wikidata labels
wikidata_ids_for_labeling = set([el for sub in [{k for k,v in e.items()} for e in  wikidata_response.values()] for el in sub])
wikidata_ids_for_labeling2 = set([ele for suba in [[el for sub in [v for k,v in e.items()] for el in sub] for e in  wikidata_response.values()] for ele in suba])
wikidata_ids_for_labeling = wikidata_ids_for_labeling | wikidata_ids_for_labeling2

def get_wikidata_label(wikidata_id, pref_langs = ['en', 'es', 'fr', 'de', 'pl', 'ro', 'mul']):
    # wikidata_id = 'Q20666586'
    url = f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json'
    headers = {'User-Agent': 'CoolBot/0.0 (https://example.org/coolbot/; coolbot@example.org)'}
    try:
        result = requests.get(url, headers=headers).json()
        try:
            langs = [e for e in list(result.get('entities').get(wikidata_id).get('labels').keys()) if e in pref_langs]
        except AttributeError:
            langs = [e for e in list(result.get('entities').get(wikidata_id).get('labels').keys()) if e in pref_langs]
        if langs:
            order = {lang: idx for idx, lang in enumerate(pref_langs)}
            sorted_langs = sorted(langs, key=lambda x: order.get(x, float('inf')))
            for lang in sorted_langs:
                label = result['entities'][wikidata_id]['labels'][lang]['value']
                break
        else: label = None
    except ValueError:
        label = None
    wikidata_labels.update({wikidata_id: label})

wikidata_labels = {}
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_wikidata_label, wikidata_ids_for_labeling),total=len(wikidata_ids_for_labeling)))
    
wikidata_response = {k:{wikidata_labels.get(ka):[wikidata_labels.get(e) for e in va] for ka,va in v.items()} for k,v in wikidata_response.items()}

#%% df preparations for RDF

df_paintings = df_artemis.copy()
df_paintings['painter_url'] = df_paintings['painting'].apply(lambda x: f"https://www.wikiart.org/en/{x.split('_')[0].replace('.','-').replace('--','-')}")
df_paintings = df_paintings.loc[df_paintings['painter_url'].isin(painters_urls)]
df_paintings = df_paintings.groupby('painting').agg({
    'art_style': 'first',  # lub 'last', wszystkie wartości są takie same
    'emotion': list,  # łączenie wszystkich emocji
    'utterance': list,  # łączenie wszystkich wypowiedzi
    'painter_url': 'first'
}).reset_index()

for e in wikiart_response:
    # e = wikiart_response[0]
    wikidata_r = wikidata_response.get(e.get('wikidata_id'))
    e.update(wikidata_r)
    
df_people = pd.DataFrame(wikiart_response)

#%% RDF sampling

df_people_sample = df_people[:200]
df_paintings_sample = df_paintings.loc[df_paintings['painter_url'].isin(df_people_sample['wikiart_url'].to_list())]

#%% RDF generation

ViWo = Namespace('https://example.org/vienna_workshop/')
dcterms = Namespace("http://purl.org/dc/terms/")
rdf = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
FABIO = Namespace("http://purl.org/spar/fabio/")
BIRO = Namespace("http://purl.org/spar/biro/")
VIAF = Namespace("http://viaf.org/viaf/")
geo = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")
bibo = Namespace("http://purl.org/ontology/bibo/")
schema = Namespace("http://schema.org/")
WDT = Namespace("http://www.wikidata.org/entity/")
OUTPUT_TTL = "data/Vienna_workshop_2025/vienna_workshop.ttl"

g = Graph()

g.bind("vienna_workshop", ViWo)
g.bind("dcterms", dcterms)
g.bind("fabio", FABIO)
g.bind("geo", geo)
g.bind("bibo", bibo)
g.bind("sch", schema)
g.bind("biro", BIRO)
g.bind("foaf", FOAF)
g.bind("wdt", WDT)
g.bind("owl", OWL)

# 1) Person
def add_person(row):
    pid = str(row["wikiart_url"])
    person = ViWo[f"Person/{pid}"]
    g.add((person, RDF.type, schema.Person))
    g.add((person, schema.name, Literal(row["label"])))
    if isinstance(row["wikidata_id"], str):
        g.add((person, OWL.sameAs, WDT[row["wikidata_id"]]))
    if isinstance(row["wikipedia_url"], str):
        g.add((person, OWL.sameAs, URIRef(row["wikipedia_url"])))
    if isinstance(row["wikiart_url"], str):
        g.add((person, OWL.sameAs, URIRef(row["wikiart_url"])))
    if isinstance(row['Nationality'], list):
        for e in row['Nationality']:
            g.add((person, ViWo.nationality, Literal(e)))
    if isinstance(row['Art Movement'], list):
        for e in row['Art Movement']:
            g.add((person, ViWo.movement, Literal(e)))
    if isinstance(row['Painting School'], list):
        for e in row['Painting School']:
            g.add((person, ViWo.paintingSchool, Literal(e)))
    if isinstance(row['Genre'], list):
        for e in row['Genre']:
            g.add((person, ViWo.genre, Literal(e)))
    if isinstance(row['Field'], list):
        for e in row['Field']:
            g.add((person, ViWo.field, Literal(e)))
    if isinstance(row['sex or gender'], list):
        for e in row['sex or gender']:
            g.add((person, schema.gender, Literal(e)))
    if isinstance(row['sex or gender'], list):
        for e in row['sex or gender']:
            g.add((person, schema.gender, Literal(e)))
    if isinstance(row['country of citizenship'], list):
        for e in row['country of citizenship']:
            g.add((person, schema.nationality, Literal(e)))
    if isinstance(row['occupation'], list):
        for e in row['occupation']:
            g.add((person, schema.hasOccupation, Literal(e)))
    if isinstance(row['movement'], list):
        for e in row['movement']:
            g.add((person, ViWo.movement, Literal(e)))
    if isinstance(row['genre'], list):
        for e in row['genre']:
            g.add((person, ViWo.genre, Literal(e)))
    if isinstance(row['Art institution'], list):
        for e in row['Art institution']:
            g.add((person, ViWo.artInstitution, Literal(e)))
    if isinstance(row['child'], list):
        g.add((person, ViWo.hasChildren, Literal(True)))
    else: g.add((person, ViWo.hasChildren, Literal(False)))
    if isinstance(row['Influenced on'], list):
        for e in row['Influenced on']:
            g.add((person, ViWo.influencedOn, Literal(e)))
    if isinstance(row['influenced by'], list):
        for e in row['influenced by']:
            g.add((person, ViWo.influencedBy, Literal(e)))
    if isinstance(row['Influenced by'], list):
        for e in row['Influenced by']:
            g.add((person, ViWo.influencedBy, Literal(e)))
    if isinstance(row['Teachers'], list):
        for e in row['Teachers']:
            g.add((person, ViWo.teachers, Literal(e)))
    if isinstance(row['Friends and Co-workers'], list):
        for e in row['Friends and Co-workers']:
            g.add((person, FOAF.knows, Literal(e)))
    if isinstance(row['relative'], list):
        for e in row['relative']:
            g.add((person, ViWo.hasRelative, Literal(e)))
    if isinstance(row['Family and Relatives'], list):
        for e in row['Family and Relatives']:
            g.add((person, ViWo.hasRelative, Literal(e)))
    if isinstance(row['Pupils'], list):
        for e in row['Pupils']:
            g.add((person, ViWo.pupils, Literal(e)))
    if isinstance(row['member of'], list):
        for e in row['member of']:
            g.add((person, schema.memberOf, Literal(e)))

for _, r in df_people.iterrows():
# for _, r in df_people_sample.iterrows():
    add_person(r)
    
# 2) Painting 
def add_painting(row):
# for _, row in df_novels.iterrows():
    tid = str(row["painting"])
    painting = ViWo[f"Painting/{tid}"]
    g.add((painting, RDF.type, schema.Painting))
    g.add((painting, schema.author, ViWo[f"Person/{row['painter_url']}"]))
    g.add((painting, schema.title, Literal(row['painting'].split('_')[-1])))
    g.add((painting, ViWo.artStyle, Literal(row['art_style'])))
    for e, u in zip(row['emotion'], row['utterance']):
        g.add((painting, ViWo.emotion, Literal(e)))
        g.add((painting, ViWo.utterance, Literal(u)))
    if isinstance(row["painting"], str):
        g.add((painting, OWL.sameAs, URIRef(f"https://www.wikiart.org/en/{row['painting'].replace('.','-').replace('--','-').replace('_','/')}")))
           
for _, r in tqdm(df_paintings.iterrows(), total=len(df_paintings)):
# for _, r in tqdm(df_paintings_sample.iterrows(), total=len(df_paintings_sample)):
    add_painting(r)
    
# --- EXPORT ---
g.serialize(destination=OUTPUT_TTL, format="turtle")
print(f"RDF triples written to {OUTPUT_TTL}") 

#zmienić url autora, bo ma podwójne https 
    
#%% Network generation
def create_relation_table(data_list):
    """
    Tworzy tabelę relacji między artystami na podstawie wspólnych wartości.

    Args:
        data_list: Lista słowników z danymi artystów (jak w przykładzie w pytaniu)

    Returns:
        DataFrame z relacjami (wikipedia1, wikipedia2, attribute, shared_value)
    """

    # Pola, których nie porównujemy
    IGNORE_KEYS = {'wikiart_url', 'wikidata_id', 'label', 'wikipedia_url'}

    # Mapowanie różnych wariantów kluczy na nazwę kanoniczną
    CANON = {
        'Nationality': 'nationality',
        'Art Movement': 'art movement',
        'Painting School': 'painting school',
        'Genre': 'genre',
        'genre': 'genre',
        'Field': 'field',
        # 'sex or gender': 'sex or gender',
        'country of citizenship': 'country of citizenship',
        'occupation': 'occupation',
        'movement': 'movement',
        'Art institution': 'art institution',
        # 'child': 'child',
        'Influenced on': 'influenced on',
        'influenced by': 'influenced by',
        'Influenced by': 'influenced by',
        'Teachers': 'teachers',
        'Friends and Co-workers': 'friends and co-workers',
        # 'relative': 'relative',
        'Pupils': 'pupils',
        'member of': 'member of',
        # 'Family and Relatives': 'family and relatives',
    }

    def canon_key(k):
        # zostawiamy oryginał jeśli nie mamy mapowania i nie jest do ignorowania
        return CANON.get(k, k)

    def to_list(v):
        return v if isinstance(v, list) else [v]

    def norm(x):
        # normalizacja tylko dla stringów
        if isinstance(x, str):
            return x.strip().casefold()
        return x

    relations = []

    for idx1, idx2 in combinations(range(len(data_list)), 2):
        d1 = data_list[idx1]
        d2 = data_list[idx2]

        wiki1 = d1.get('label')
        wiki2 = d2.get('label')
        if not wiki1 or not wiki2:
            continue

        # Porównujemy po wspólnych kluczach (po kanonizacji) z pominięciem ignorowanych
        # Zbudujmy mapy: klucz_kanoniczny -> (oryginalny_klucz, wartość)
        def build_keymap(d):
            km = {}
            for k, v in d.items():
                if k in IGNORE_KEYS:
                    continue
                ck = canon_key(k)
                # jeśli ten klucz już był pod innym wariantem, preferuj pierwszy napotkany
                if ck not in km:
                    km[ck] = (k, v)
            return km

        km1 = build_keymap(d1)
        km2 = build_keymap(d2)

        # wspólne atrybuty (po kanonizacji)
        common_attrs = set(km1.keys()) & set(km2.keys())
        for cattr in common_attrs:
            orig_k1, v1 = km1[cattr]
            orig_k2, v2 = km2[cattr]

            if v1 is None or v2 is None:
                continue

            # zamień na listy i znormalizuj do porównania
            l1 = to_list(v1)
            l2 = to_list(v2)

            # mapy: znormalizowana_wartość -> oryginalne_wartości (lista, bo mogą być duplikaty)
            norm_map1 = {}
            for x in l1:
                norm_map1.setdefault(norm(x), []).append(x)
            norm_map2 = {}
            for x in l2:
                norm_map2.setdefault(norm(x), []).append(x)

            # część wspólna po normalizacji
            common_norm_vals = set(norm_map1.keys()) & set(norm_map2.keys())
            for nv in common_norm_vals:
                # wybierz jedną reprezentację oryginalną (preferuj z pierwszego słownika)
                shared_originals = norm_map1[nv]
                for shared in shared_originals:
                    relations.append({
                        'label1': wiki1,
                        'label2': wiki2,
                        'attribute': cattr,       # nazwa kanoniczna
                        'shared_value': shared    # oryginalna wartość z d1
                    })

    df = pd.DataFrame(relations, columns=['label1', 'label2', 'attribute', 'shared_value'])
    return df

# Użycie funkcji
wikiart_relations = [{k:v for k,v in e.items() if k not in ['sex or gender', 'child', 'relative', 'Family and Relatives']} for e in wikiart_response]
relations_df = create_relation_table(wikiart_relations)

relations_df.to_csv('data/Vienna_workshop_2025/art_network_full.csv', index=False)

relations_df["Name_1"], relations_df["Name_2"] = zip(*relations_df.apply(lambda row: sorted([row["label1"], row["label2"]]), axis=1))

relations_df_to_co_occurence = relations_df[['Name_1', 'Name_2']]

co_occurrence = relations_df_to_co_occurence.groupby(['Name_1','Name_2']).size().reset_index(name='weight')
co_occurrence.to_excel('data/Vienna_workshop_2025/art_network_nodes_and_weights.xlsx', index=False)

# test = [e for e in wikiart_relations if e.get('label') in ['A.Y. Jackson', 'Alexander Calder']]
# test = [e for e in wikiart_response if 'Nationality' in e and e.get('Nationality')[0] == 'Austrian']















