import rdflib
import networkx as nx
from ipysigma import Sigma
from rdflib import Graph, Namespace, Literal
import pandas as pd
from tqdm import tqdm

#%%
# 1. Wczytanie pliku TTL za pomocą rdflib
g = rdflib.Graph()
ttl_file = 'jecal.ttl'
g.parse(ttl_file, format='ttl')

#%% przygotowanie

uris = set()

ps = set()
for s, p, o in g:
    ps.add(str(p))
    print(s, p, o)
    
    if isinstance(s, rdflib.term.URIRef):
        uris.add(str(s))
    if isinstance(p, rdflib.term.URIRef):
        uris.add(str(p))
    if isinstance(o, rdflib.term.URIRef):
        uris.add(str(o))

# 3. Posortowana lista i wypisanie
# for uri in sorted(uris):
#     print(uri)

SCHEMA = Namespace("http://schema.org/")

# Tworzenie słownika: {osoba: name}
people_names = {}

for subject, name in g.subject_objects(predicate=SCHEMA.name):
    people_names[subject] = str(name)

#
# Definicja przestrzeni nazw
JC = Namespace("https://example.org/jesuit_calvinist/")

# Filtracja zasobów, które mają editionType = "Original"
original_texts = []

for subject in g.subjects(predicate=JC.editionType, object=Literal("Original")):
    original_texts.append(subject)

# Wyświetlenie wyników
# print("Teksty z editionType = 'Original':")
# for text in original_texts:
#     print(text)

#%%
#(1) Text, Cited Author oraz Theme; 
# cited authors + Polemical Themes (Random Order)
# https://example.org/jesuit_calvinist/keyAuthorsCited
# https://example.org/jesuit_calvinist/polemicalTheme

text_author = dict()
text_theme = dict()
for s, p, o in g:
    if s in original_texts:
        if str(p) == 'https://example.org/jesuit_calvinist/keyAuthorsCited':
            if str(s) not in text_author:
                text_author[str(s)] = [people_names.get(o)]
            else: text_author[str(s)].append(people_names.get(o))
        elif str(p) == 'https://example.org/jesuit_calvinist/polemicalTheme':
            if str(s) not in text_theme:
                text_theme[str(s)] = [str(o)]
            else: text_theme[str(s)].append(str(o))

result_27a = []
for k, v in text_author.items():
    for ka, va in text_theme.items():
        if k == ka:
            for a in v:
                for t in va:
                    result_27a.append([a, t])
                    
df = pd.DataFrame(result_27a, columns= ['person', 'topic'])

co_occurrence = df.groupby(['person', 'topic']).size().reset_index(name='weight')

co_occurrence.to_excel('data/Michał Nowakowski/27a.xlsx', index=False)

# Tworzymy graf
G = nx.Graph()

# Dodajemy węzły i krawędzie z wagami
for _, row in tqdm(co_occurrence.iterrows(), total = len(co_occurrence)):
    person = row['person']
    topic = row['topic']
    weight = row['weight']

    # Węzły
    G.add_node(person, type='person', color='#e41a1c', size=10)
    G.add_node(topic, type='topic', color='#377eb8', size=7)

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
    '27a1.html',
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

#LUB

pagerank = nx.pagerank(G)

nx.set_node_attributes(G, pagerank, name='pagerank')

# ✅ Eksport do HTML z własnymi metrykami
Sigma.write_html(
    G,
    '27a2.html',
    fullscreen=True,
    node_color='pagerank',         # ręcznie przypisany
    node_size='degree',
    node_size_range=(3, 20),
    max_categorical_colors=30,
    default_edge_type='curve',
    default_node_label_size=14,
    node_border_color_from='node'
)

#%%

#(2) Text, Cited Author oraz Discussed Issue;  
# cited authors + Discussed Issues (Random Order)
# https://example.org/jesuit_calvinist/keyAuthorsCited
# https://example.org/jesuit_calvinist/discussedIssue

text_author = dict()
text_issue = dict()
for s, p, o in g:
    if s in original_texts:
        if str(p) == 'https://example.org/jesuit_calvinist/keyAuthorsCited':
            if str(s) not in text_author:
                text_author[str(s)] = [people_names.get(o)]
            else: text_author[str(s)].append(people_names.get(o))
        elif str(p) == 'https://example.org/jesuit_calvinist/discussedIssue':
            if str(s) not in text_issue:
                text_issue[str(s)] = [str(o)]
            else: text_issue[str(s)].append(str(o))

result_27b = []
for k, v in text_author.items():
    for ka, va in text_issue.items():
        if k == ka:
            for a in v:
                for t in va:
                    result_27b.append([a, t])
                    
df = pd.DataFrame(result_27b, columns= ['person', 'topic'])

co_occurrence = df.groupby(['person', 'topic']).size().reset_index(name='weight')

# Tworzymy graf
G = nx.Graph()

# Dodajemy węzły i krawędzie z wagami
for _, row in tqdm(co_occurrence.iterrows(), total = len(co_occurrence)):
    person = row['person']
    topic = row['topic']
    weight = row['weight']

    # Węzły
    G.add_node(person, type='person', color='#e41a1c', size=10)
    G.add_node(topic, type='topic', color='#377eb8', size=7)

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
    '27b1.html',
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

#LUB

pagerank = nx.pagerank(G)

nx.set_node_attributes(G, pagerank, name='pagerank')

# ✅ Eksport do HTML z własnymi metrykami
Sigma.write_html(
    G,
    '27b2.html',
    fullscreen=True,
    node_color='pagerank',         # ręcznie przypisany
    node_size='degree',
    node_size_range=(3, 20),
    max_categorical_colors=30,
    default_edge_type='curve',
    default_node_label_size=14,
    node_border_color_from='node'
)






# Opcjonalnie: możesz ustawić grubość krawędzi jako wagę
for u, v, d in G.edges(data=True):
    d['size'] = d['weight']  # Sigma używa 'size' do szerokości krawędzi

s = Sigma(G)

# Zapis do pliku HTML
s.save("graf.html")

            
    and p in ['https://example.org/jesuit_calvinist/keyAuthorsCited', 'https://example.org/jesuit_calvinist/polemicalTheme']:
        result_27a.add(str(s))
        res
        
    ps.add(str(p))
    print(s, p, o)
    
    if isinstance(s, rdflib.term.URIRef):
        uris.add(str(s))
    if isinstance(p, rdflib.term.URIRef):
        uris.add(str(p))
    if isinstance(o, rdflib.term.URIRef):
        uris.add(str(o))


#(3) Text, Historical Figure Mentioned oraz Theme; (4) Text, Historical Figure Mentioned oraz Discussed Issue
#cited authors + Discussed Issue
Historical Figure Mentioned + Polemical Themes (Random Order)
Historical Figure Mentioned + Discussed Issue

#%% rdf to lpg

uris = set()
for s, p, o in g:
    if isinstance(s, rdflib.term.URIRef):
        uris.add(str(s))
    if isinstance(p, rdflib.term.URIRef):
        uris.add(str(p))
    if isinstance(o, rdflib.term.URIRef):
        uris.add(str(o))

# 3. Posortowana lista i wypisanie
for uri in sorted(uris):
    print(uri)



# 2. Utworzenie grafu skierowanego w networkx
G = nx.DiGraph()

# 3. Dodawanie węzłów i krawędzi na podstawie potrójek RDF
triples = []
for s, p, o in g:
    s_str = str(s)
    p_str = str(p)
    o_str = str(o)
    G.add_node(s_str)
    G.add_node(o_str)
    G.add_edge(s_str, o_str, predicate=p_str)
    triples.append((s_str, p_str, o_str))

# 4. Podstawowe statystyki grafu
print(f"Liczba węzłów: {G.number_of_nodes()}")
print(f"Liczba krawędzi: {G.number_of_edges()}")

# Opcjonalnie: wyświetlenie przykładowych krawędzi
print("\nPrzykładowe krawędzie (z atrybutem 'predicate'):")
for u, v, data in list(G.edges(data=True))[:10]:
    print(f"{u} -[{data['predicate']}]-> {v}")


Sigma(G, 
      node_color="tag",
      node_label_size=G.degree,
      node_size=G.degree
     )

Sigma.write_html(
    G,
    'jecal.html',
    fullscreen=True,
    node_metrics=['louvain'],
    node_color='louvain',
    node_size_range=(3, 20),
    max_categorical_colors=30,
    default_edge_type='curve',
    node_border_color_from='node',
    default_node_label_size=14,
    node_size=G.degree
)

#%%



























