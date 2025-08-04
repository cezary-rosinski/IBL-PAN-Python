import glob
import os
import itertools
import pandas as pd
import spacy
from tqdm import tqdm
import regex as re
import networkx as nx
from ipysigma import Sigma

#%% podejście z indeksem

import re
from collections import defaultdict

def clean_page_number(raw):
    # Usuwa znaki niebędące cyframi i dzieli liczby zlepione (np. 39120 → [391, 20] jeśli >4 cyfry)
    cleaned = re.sub(r'[^\d]', '', raw)
    if len(cleaned) > 4:
        return [int(cleaned[:-2]), int(cleaned[-2:])]
    return [int(cleaned)] if cleaned else []

def extract_index_entries(text):
    results = defaultdict(list)

    # Normalizacja
    text = text.replace('\n', ' ').replace('- ', '')
    
    # Podział na wpisy zaczynające się od nazwiska
    entries = re.findall(r'([A-Z][^,]+?, [A-Z][^,]+?(?: de| von| da| d[ae]| E\.)?),\s+(.*?)(?=(?:[A-Z][^,]+?, [A-Z][^,]+?(?: de| von| da| d[ae]| E\.)?,|$))', text)

    for headword, body in entries:
        pages = []

        # Szukaj liczb i zakresów
        matches = re.findall(r'(\d+)(?:\s*[-–]\s*(\d+))?', body)
        for start, end in matches:
            start_pages = clean_page_number(start)
            if end:
                end_pages = clean_page_number(end)
                if start_pages and end_pages:
                    pages.extend(range(start_pages[0], end_pages[0] + 1))
            else:
                pages.extend(start_pages)

        if pages:
            results[headword].extend(pages)

    # Zamień defaultdict na zwykły słownik
    return dict(results)

#%%
folder = r'C:\Users\Cezary\Documents\IBL-PAN-Python\data\Peru\Monumenta Peruana 1\Index/'
files = [f for f in glob.glob(folder + '*.txt', recursive=True)]

final = dict()
for f in tqdm(files):
    text = open(f, encoding="utf-8").read()
    result = extract_index_entries(text)
    final.update(result)
    
import json
with open('data.json', 'w') as f:
    json.dump(final, f)

import json
from itertools import combinations
from collections import defaultdict
import pandas as pd

# === KROK 1: Wczytanie danych JSON ===
with open("data.json", "r", encoding="utf-8") as f:
    data = json.load(f)

# === KROK 2: Przycinanie wartości liczbowych do 3 cyfr ===
def trim_json_values(data):
    trimmed_data = {}
    for person, values in data.items():
        trimmed_values = [int(str(v)[:3]) for v in values if isinstance(v, int)]
        trimmed_data[person] = trimmed_values
    return trimmed_data

# === KROK 3: Znalezienie par osób z tymi samymi wartościami (z powtórzeniami) ===
def find_shared_values(trimmed_data):
    shared_values = defaultdict(list)
    people = list(trimmed_data.keys())
    for person1, person2 in combinations(people, 2):
        common = []
        list1 = trimmed_data[person1][:]
        list2 = trimmed_data[person2][:]
        for val in list1:
            if val in list2:
                common.append(val)
                list2.remove(val)  # usuń tylko jedno wystąpienie
        if common:
            shared_values[(person1, person2)].extend(common)
    return shared_values

# === KROK 4: Konwersja do pandas.DataFrame ===
def shared_values_to_dataframe(shared_values):
    rows = []
    for (p1, p2), values in shared_values.items():
        for val in values:
            rows.append({'Person 1': p1, 'Person 2': p2, 'Shared Value': val})
    return pd.DataFrame(rows)

# === Wykonanie pełnego przetwarzania ===
trimmed = trim_json_values(data)
shared = find_shared_values(trimmed)
df = shared_values_to_dataframe(shared)

# === Wyświetlenie wyników ===
print(df)

# (Opcjonalnie: zapis do pliku CSV)
df.to_excel("shared_values.xlsx", index=False)

graph_df = df[['Person 1', 'Person 2']]

graph_df["Byt_A"], graph_df["Byt_B"] = zip(*graph_df.apply(lambda row: sorted([row["Person 1"], row["Person 2"]]), axis=1))

graph_df = graph_df[['Byt_A', 'Byt_B']]

co_occurrence = graph_df.groupby(['Byt_A', 'Byt_B']).size().reset_index(name='weight')

# Tworzymy graf
G = nx.Graph()

# Dodajemy węzły i krawędzie z wagami
for _, row in tqdm(co_occurrence.iterrows(), total = len(co_occurrence)):
    person = row['Byt_A']
    topic = row['Byt_B']
    weight = row['weight']

    # Węzły
    G.add_node(person, type='Byt_A', color='#e41a1c', size=10)
    G.add_node(topic, type='Byt_B', color='#377eb8', size=7)

    # Krawędź z wagą
    G.add_edge(person, topic, weight=weight)


# Dodajemy stopnie jako metrykę (do rozmiarów i etykiet)
degree_dict = dict(G.degree())
nx.set_node_attributes(G, degree_dict, name='degree')

# ✅ Interaktywny podgląd w Jupyterze (opcjonalnie)
Sigma(
    G,
    node_color="tag",
    node_label_size="degree",
    node_size="degree"
)

# ✅ Eksport do HTML
Sigma.write_html(
    G,
    'data/Peru/Peru1.html',
    fullscreen=True,
    node_metrics=['louvain'],
    node_color='louvain',
    node_size='degree',
    node_size_range=(3, 20),
    max_categorical_colors=30,
    default_edge_type='curve',
    default_node_label_size=14,
    node_border_color_from='node'
)


pagerank = nx.pagerank(G)
nx.set_node_attributes(G, pagerank, name='pagerank')

# ✅ Eksport do HTML z własnymi metrykami
Sigma.write_html(
    G,
    'data/Peru/Peru1_pagerank_graph.html',
    fullscreen=True,
    node_color='pagerank',         # ręcznie przypisany
    node_size='degree',
    node_size_range=(3, 20),
    max_categorical_colors=30,
    default_edge_type='curve',
    default_node_label_size=14,
    node_border_color_from='node'
)





# Przykładowe dane wejściowe
text = """Abbreviationes in hoc volumine quid
significent , 53 .
Acapulco , portus , 610 27.
Acevedo , Ignatius de , S. I. , Borgia
dat ei instructionem missionariam ,
120 , 122 , 124 11.
Acllas , 21s .
300 ; pro
Acosta , Iosephus de , S. I., vita 299 ,
manifestat Borgiae suum desiderium
Indias adeundi , 300-302 ; pro
cleri educatione , 302 ; P. Ludovicum
Guzmán ad missiones praesentat
, 303 ; revelat P. Nadal voluntatem
suam ad missi ones indicas et
varia de se ipso , 300 , 301 10; conficit
litteras annuas ,
Roma vel Burgos destinatus , 322 ;
Peruae missionarius renuntiatus ,
3897 , 390, 39120, 371 ; nuntiat Borgiae
praeludia itineris , 439-442 ;
quaerit de P. Fonseca , 442 ; proponit
ad Sacros Ordines F. D. Martínez
, 442 ; Hispali versatur et Sanlúcar
, 440 ; in insula S. Ioannis
et S. Dominici , 443 ; eius iter a
Sacchini narratur , 47s .; Limam attigit,
36 , 505 ; confessarius in collegio
limensi et magister novitiorum
, 505 ; concionator Limae , 703 ;
missionarius per regionem de La
Plata 629 ³, 709 ; eius missio per Peruam
, 6293,706 ; ad Potosí , 709 ; quaestiones
morales cum eo conferendae ,
632 ; provincialis designatus Peruae,
37 ; eius actio in Congregationibus
provincialibus , 38 ; scriptor
, 321 ; informationes de eo ,
3019 ; laudatur , 505 , 507 , 509 ,
589 .
Acoza , C., canonicus hispalensis , 194 " .
Acta notarilia in hoc volumine , 42 .
Acuerdo, 25.
Admonitor in S. I. , quid sit , 506¹º .
Adrianus VI facultates missionariis
concedit , 91 , 117 ; concedit regulares
designari posse parochi indorum
, 454 , 578 .
Aeneidos libri , 141 10.
Africa , mancipia , 167 21.
Agnus Dei , petuntur a P. Portillo ,
131 ; ab eo recipiuntur , 140 .
Agra , collegium S. I. , 216 21.
Aguado , Petrus de , O. F. M. , scriptor,
130 25.
Agüero , Didacus , civis limensis 188 .
Aguila , Alphonsus , S. I. , 589 .
Aguilar , Elisabetha de , 2534 °, 280.
Aguilar de Campó , oppidum , 187 15.
Aguilera , Antonius de , e Consilio Indiarum
, 1287 , 456 .
Aicardo , Iosephus E. , S. I. , scriptor
, 278 , 338 .
Akha , v. Chicha .
Alba , dux de , missus in Flandriam ,
967 ; 210 , 220 18.
Albarracín , scholae S. I. , 308 ¹.
Alberro , Martinus , S. I. , ad Indias
missionarius proponitur , 89 ."""

# Uruchomienie ekstrakcji
result = extract_index_entries(text)



#%% podejście spacy
folder = r'C:\Users\Cezary\Documents\IBL-PAN-Python\data\Peru\Monumenta Peruana 1/'
files = [f for f in glob.glob(folder + '*.txt', recursive=True)]

# nlp = spacy.load("la_core_web_lg")
# nlp = spacy.load("xx_ent_wiki_sm")
nlp = spacy.load("latin-ner")

rows = []

for fname in tqdm(files):
    if fname.endswith(".txt"):
        text = open(os.path.join(folder, fname), encoding="utf-8").read()
        doc = nlp(text)
        entities = list({(ent.text, ent.label_) for ent in doc.ents})
        for e1, e2 in itertools.combinations(entities, 2):
            rows.append({
                "Plik": fname,
                "Byt_1": e1[0], "Typ_1": e1[1],
                "Byt_2": e2[0], "Typ_2": e2[1]
            })

df = pd.DataFrame(rows)

pattern = r'^[\p{L} \-]+$'

people_df = df.loc[(df['Typ_1'] == 'PER') &
                   (df['Typ_2'] == 'PER')]

people_df = people_df[~people_df['Byt_1'].str.contains(r'\d') & (people_df['Byt_1'].str.len() > 2) & ~people_df['Byt_2'].str.contains(r'\d') & (people_df['Byt_2'].str.len() > 2)]

people_df = people_df[
    people_df['Byt_1'].apply(lambda x: isinstance(x, str) and len(x) > 2 and not re.search(r'\d', x) and re.fullmatch(pattern, x) is not None) &
    people_df['Byt_2'].apply(lambda x: isinstance(x, str) and len(x) > 2 and not re.search(r'\d', x) and re.fullmatch(pattern, x) is not None)
]

graph_df = people_df[['Byt_1', 'Byt_2']].reset_index(drop=True)

graph_df["Byt_A"], graph_df["Byt_B"] = zip(*graph_df.apply(lambda row: sorted([row["Byt_1"], row["Byt_2"]]), axis=1))

graph_df = graph_df[['Byt_A', 'Byt_B']]

co_occurrence = graph_df.groupby(['Byt_A', 'Byt_B']).size().reset_index(name='weight')

# Tworzymy graf
G = nx.Graph()

# Dodajemy węzły i krawędzie z wagami
for _, row in tqdm(co_occurrence.iterrows(), total = len(co_occurrence)):
    person = row['Byt_A']
    topic = row['Byt_B']
    weight = row['weight']

    # Węzły
    G.add_node(person, type='Byt_A', color='#e41a1c', size=10)
    G.add_node(topic, type='Byt_B', color='#377eb8', size=7)

    # Krawędź z wagą
    G.add_edge(person, topic, weight=weight)


# Dodajemy stopnie jako metrykę (do rozmiarów i etykiet)
degree_dict = dict(G.degree())
nx.set_node_attributes(G, degree_dict, name='degree')

# ✅ Interaktywny podgląd w Jupyterze (opcjonalnie)
Sigma(
    G,
    node_color="tag",
    node_label_size="degree",
    node_size="degree"
)

# ✅ Eksport do HTML
Sigma.write_html(
    G,
    'Peru/Peru1.html',
    fullscreen=True,
    node_metrics=['louvain'],
    node_color='louvain',
    node_size='degree',
    node_size_range=(3, 20),
    max_categorical_colors=30,
    default_edge_type='curve',
    default_node_label_size=14,
    node_border_color_from='node'
)


pagerank = nx.pagerank(G)
nx.set_node_attributes(G, pagerank, name='pagerank')

# ✅ Eksport do HTML z własnymi metrykami
Sigma.write_html(
    G,
    'Peru/Peru1_pagerank_graph.html',
    fullscreen=True,
    node_color='pagerank',         # ręcznie przypisany
    node_size='degree',
    node_size_range=(3, 20),
    max_categorical_colors=30,
    default_edge_type='curve',
    default_node_label_size=14,
    node_border_color_from='node'
)

df.to_csv("cooccurrence_bytow.csv", index=False)
print("Gotowe: plik cooccurrence_bytow.csv")


















