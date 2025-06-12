import rdflib
import networkx as nx
from ipysigma import Sigma

# 1. Wczytanie pliku TTL za pomocą rdflib
g = rdflib.Graph()
ttl_file = 'jecal.ttl'
g.parse(ttl_file, format='ttl')


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