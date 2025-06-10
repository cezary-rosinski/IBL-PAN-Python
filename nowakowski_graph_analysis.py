import sys
sys.path.insert(1, 'D:\IBL\Documents\IBL-PAN-Python')
# sys.path.insert(1, 'C:/Users/Cezary/Documents/IBL-PAN-Python')
import pandas as pd
import numpy as np
from dateutil import parser
import regex as re
from my_functions import gsheet_to_df
import geopandas as gpd
from shapely.geometry import Point
import plotly.express as px
from plotly.offline import plot
from rdflib import Graph, Namespace, RDF
from rdflib.namespace import DCTERMS
import matplotlib.pyplot as plt
from collections import Counter
from matplotlib import cycler

#%%
tableau_colors = cycler('color', plt.cm.tab10.colors)

#%% wczytanie grafu
# 1) Załaduj graf RDF
g = Graph()
g.parse("jecal.ttl", format="turtle")

# 2) Zdefiniuj przestrzenie nazw
FABIO = Namespace("http://purl.org/spar/fabio/")
JC = Namespace("https://example.org/jesuit_calvinist/")
SCH = Namespace("http://schema.org/")
#%% 3

query = """
PREFIX schema: <http://schema.org/>
PREFIX jc: <https://example.org/jesuit_calvinist/>

SELECT ?fullname (COUNT(?text) AS ?count)
WHERE {
  ?text a schema:Text ;
        jc:keyAuthorsCited ?author ;
        jc:editionType ?etype ;
        jc:confessionalProfile "Roman Catholic" ;
        jc:targetedConfession ?target .
  FILTER(CONTAINS(LCASE(str(?etype)), "original")) .
  FILTER(CONTAINS(LCASE(str(?target)), "reformed evangelical")) .
  
  ?author schema:name ?fullname
  
}
GROUP BY ?author
ORDER BY DESC(?count)
LIMIT 20
"""

# 3. Wykonanie zapytania i przygotowanie DataFrame
results = g.query(query)

data = [
    (str(author), int(count))
    for author, count in results
]

df = pd.DataFrame(data, columns=["author", "count"])

# 4. Wizualizacja
plt.figure(figsize=(12, 6))
colors = plt.cm.viridis(np.linspace(0, 1, len(df)))
plt.bar(df["author"], df["count"],
        color=colors,
        edgecolor='k',      # kolor obramowania słupków
        linewidth=0.5       # grubość linii obramowania
       )
plt.xticks(rotation=45, ha='right')
plt.ylabel("Liczba cytowań")
plt.title("Top 20 najczęściej cytowanych autorów; editionType = Original; \nconfessionalProfile = Roman Catholic; targetedConfession = Reformed Evangelical")
plt.tight_layout()
plt.show()

#%% 8

query = """
PREFIX schema: <http://schema.org/>
PREFIX jc: <https://example.org/jesuit_calvinist/>

SELECT ?fullname (COUNT(?text) AS ?count)
WHERE {
  ?text a schema:Text ;
        jc:keyAuthorsCited ?author ;
        jc:editionType ?etype ;
        jc:confessionalProfile "Reformed Evangelical" ;
        jc:targetedConfession ?target .
  FILTER(CONTAINS(LCASE(str(?etype)), "original")) .
  FILTER(CONTAINS(LCASE(str(?target)), "roman catholic")) .
  
  ?author schema:name ?fullname
  
}
GROUP BY ?author
ORDER BY DESC(?count)
LIMIT 20
"""

# 3. Wykonanie zapytania i przygotowanie DataFrame
results = g.query(query)

data = [
    (str(author), int(count))
    for author, count in results
]

df = pd.DataFrame(data, columns=["author", "count"])

# 4. Wizualizacja
plt.figure(figsize=(12, 6))
colors = plt.cm.viridis(np.linspace(0, 1, len(df)))
plt.bar(df["author"], df["count"],
        color=colors,
        edgecolor='k',      # kolor obramowania słupków
        linewidth=0.5       # grubość linii obramowania
       )
plt.xticks(rotation=45, ha='right')
plt.ylabel("Liczba cytowań")
plt.title("Top 20 najczęściej cytowanych autorów; editionType = Original; \nconfessionalProfile = Reformed Evangelical; targetedConfession = Roman Catholic")
plt.tight_layout()
plt.show()


#%% analiza networkx

import rdflib
import networkx as nx

# 1. Załaduj graf RDF z pliku TTL
g_rdflib = rdflib.Graph()
g_rdflib.parse("jecal.ttl", format="ttl")

# 2. Konwersja do NetworkX (graf skierowany)
G = nx.DiGraph()
for s, p, o in g_rdflib:
    # możesz filtrować krawędzie wg predykatów (p), jeśli chcesz
    G.add_edge(str(s), str(o), predicate=str(p))

# 3. Podstawowe statystyki
print("Wierzchołki:", G.number_of_nodes())
print("Krawędzie:", G.number_of_edges())
print("Gęstość grafu:", nx.density(G))

# 4. Rozkład stopni
degrees = dict(G.degree())
# np. histogram:
import matplotlib.pyplot as plt
plt.hist(list(degrees.values()), bins=0)
plt.xlabel("Stopień węzła")
plt.ylabel("Liczba węzłów")
plt.title("Rozkład stopni grafu")
plt.show()

# 5. Składowe spójności (dla grafu skierowanego)
weak_comps = list(nx.weakly_connected_components(G))
print("Liczba składowych słabo spójnych:", len(weak_comps))
print("Rozmiar największej składowej:", len(max(weak_comps, key=len)))

# 6. Centralność
deg_cent = nx.degree_centrality(G)
bet_cent = nx.betweenness_centrality(G)
# Najbardziej centralne węzły wg stopnia
top_deg = sorted(deg_cent.items(), key=lambda x: x[1], reverse=True)[:10]
print("Top 10 węzłów wg centralności stopniowej:")
for node, cent in top_deg:
    print(f"  {node}: {cent:.4f}")
    
# 7. Współczynnik skupień
avg_clust = nx.average_clustering(G.to_undirected())
print("Średni clustering coefficient:", avg_clust)


#%%

import rdflib
import networkx as nx
import matplotlib.pyplot as plt
import numpy as np

# Load RDF graph from Turtle file and convert to NetworkX directed graph
# Then create an undirected view for analyses that require it

def load_graph(ttl_file: str) -> nx.DiGraph:
    g_rdflib = rdflib.Graph()
    g_rdflib.parse(ttl_file, format='turtle')
    G = nx.DiGraph()
    for s, p, o in g_rdflib:
        G.add_edge(str(s), str(o))
    return G

# 1. Hub detection: highest degree and centrality

def detect_hubs(G: nx.DiGraph, top_n: int = 10):
    # Convert to undirected for degree-based measures
    G_ud = G.to_undirected()

    # Degree (total)
    deg_dict = dict(G_ud.degree())
    top_deg = sorted(deg_dict.items(), key=lambda x: x[1], reverse=True)[:top_n]

    # Degree centrality
    cent_dict = nx.degree_centrality(G_ud)
    top_cent = sorted(cent_dict.items(), key=lambda x: x[1], reverse=True)[:top_n]

    return top_deg, top_cent

# 2. Log-Log plot of degree distribution

def plot_degree_distribution(G: nx.DiGraph, output_file: str = 'degree_dist.png'):
    G_ud = G.to_undirected()
    hist = nx.degree_histogram(G_ud)
    k = np.arange(len(hist))
    Pk = np.array(hist) / sum(hist)

    # Filter zeros
    mask = Pk > 0
    k = k[mask]
    Pk = Pk[mask]

    plt.figure()
    plt.loglog(k, Pk, marker='o', linestyle='None')
    plt.xlabel('Degree k')
    plt.ylabel('P(k)')
    plt.title('Log-Log Degree Distribution')
    plt.grid(True)
    plt.savefig(output_file)
    plt.close()

# 3. Network resilience analysis by hub removal

def resilience_analysis(G: nx.DiGraph, fraction_steps: int = 20, output_file: str = 'resilience.png'):
    G_ud = G.to_undirected()
    N = G_ud.number_of_nodes()

    # Sort nodes by degree in undirected view
    deg_sorted = sorted(G_ud.degree(), key=lambda x: x[1], reverse=True)
    nodes_by_deg = [n for n, _ in deg_sorted]

    frac_removed = []
    largest_cc = []

    step = max(1, N // fraction_steps)
    for i in range(0, N + step, step):
        to_remove = nodes_by_deg[:i]
        G_temp = G_ud.copy()
        G_temp.remove_nodes_from(to_remove)
        # Compute size of largest connected component
        if G_temp.number_of_nodes() > 0:
            largest = max(len(c) for c in nx.connected_components(G_temp))
        else:
            largest = 0
        frac_removed.append(len(to_remove) / N)
        largest_cc.append(largest / N)

    plt.figure()
    plt.plot(frac_removed, largest_cc, marker='o')
    plt.xlabel('Fraction of nodes removed')
    plt.ylabel('Relative size of largest component')
    plt.title('Network Resilience under Hub Removal')
    plt.grid(True)
    plt.savefig(output_file)
    plt.close()

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Network analysis with NetworkX')
    parser.add_argument('jecall.ttl', help='Path to Turtle (.ttl) file')
    args = parser.parse_args()

    G = load_graph(args.ttl_file)

    # Detect hubs
    top_deg, top_cent = detect_hubs(G, top_n=10)
    print("Top 10 nodes by degree (undirected):")
    for node, deg in top_deg:
        print(f"{node}: degree = {deg}")
    print("\nTop 10 nodes by degree centrality (undirected):")
    for node, cent in top_cent:
        print(f"{node}: centrality = {cent:.4f}")

    # Plot degree distribution
    plot_degree_distribution(G)
    print("Saved degree distribution plot as 'degree_dist.png'")

    # Resilience analysis
    resilience_analysis(G)
    print("Saved resilience plot as 'resilience.png'")

#%%

import rdflib
import networkx as nx
from networkx.algorithms import community

# Load RDF graph from Turtle file
rdf_graph = rdflib.Graph()
rdf_graph.parse('jecal.ttl', format='ttl')

# # Convert RDF graph to NetworkX graph
# # We'll build an undirected graph connecting subjects and objects
# G = nx.Graph()
# for subj, pred, obj in rdf_graph:
#     # Only consider URIRefs or BNodes for community structure
#     if isinstance(subj, (rdflib.term.URIRef, rdflib.term.BNode)) and \
#        isinstance(obj, (rdflib.term.URIRef, rdflib.term.BNode)):
#         G.add_node(subj)
#         G.add_node(obj)
#         G.add_edge(subj, obj, predicate=str(pred))
        
G = nx.DiGraph()
for s, p, o in rdf_graph:
    # możesz filtrować krawędzie wg predykatów (p), jeśli chcesz
    G.add_edge(str(s), str(o), predicate=str(p))

# Check basic info
print(f"Nodes: {G.number_of_nodes()}, Edges: {G.number_of_edges()}")

# Community detection using the greedy modularity maximization
communities = community.greedy_modularity_communities(G)

# Display communities
display_communities = {}
for i, comm in enumerate(communities, start=1):
    display_communities.update({i:[str(e) for e in comm]})
    print(f"Community {i} ({len(comm)} nodes):")
    for node in comm:
        print(f"  - {node}")

# Alternatively, label propagation
alt_propagation = {}
lp_communities = community.asyn_lpa_communities(G)
print("\nLabel Propagation Communities:")
for i, comm in enumerate(lp_communities, start=1):
    alt_propagation.update({i:[str(e) for e in comm]})
    print(f"Community {i} ({len(comm)} nodes):")
    for node in comm:
        print(f"  - {node}")



#%%

import rdflib
import networkx as nx
from networkx.algorithms import community
import matplotlib.pyplot as plt

# Load RDF graph from Turtle file
rdf_graph = rdflib.Graph()
rdf_graph.parse('jecal.ttl', format='ttl')

G = nx.DiGraph()
for s, p, o in rdf_graph:
    # możesz filtrować krawędzie wg predykatów (p), jeśli chcesz
    G.add_edge(str(s), str(o), predicate=str(p))

print(f"Nodes: {G.number_of_nodes()}, Edges: {G.number_of_edges()}")

# Community detection using greedy modularity maximization
communities = list(community.greedy_modularity_communities(G))

# Assign community index to each node
node_community = {}
for idx, comm in enumerate(communities):
    for node in comm:
        node_community[node] = idx

# Prepare colors for visualization
colors = [node_community.get(node, -1) for node in G.nodes()]

# Compute layout
pos = nx.spring_layout(G, seed=42)

# Draw the graph
plt.figure(figsize=(12, 12))
nx.draw_networkx_nodes(G, pos,
                       node_color=colors,
                       cmap=plt.cm.tab20,
                       node_size=100)
nx.draw_networkx_edges(G, pos, alpha=0.5)
# Optionally, draw labels if graph is small
if G.number_of_nodes() <= 50:
    nx.draw_networkx_labels(G, pos, font_size=8)

plt.title('Community Structure (Greedy Modularity)')
plt.axis('off')
plt.show()

# For label propagation communities
lp_communities = list(community.asyn_lpa_communities(G))
node_lp = {}
for idx, comm in enumerate(lp_communities):
    for node in comm:
        node_lp[node] = idx

colors_lp = [node_lp.get(node, -1) for node in G.nodes()]

plt.figure(figsize=(12, 12))
nx.draw_networkx_nodes(G, pos,
                       node_color=colors_lp,
                       cmap=plt.cm.tab20,
                       node_size=100)
nx.draw_networkx_edges(G, pos, alpha=0.5)
if G.number_of_nodes() <= 50:
    nx.draw_networkx_labels(G, pos, font_size=8)

plt.title('Community Structure (Label Propagation)')
plt.axis('off')
plt.show()




















