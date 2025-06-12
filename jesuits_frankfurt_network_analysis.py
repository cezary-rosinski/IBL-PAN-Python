import pandas as pd
import networkx as nx
from ipysigma import Sigma

# 1. Load the CSV file
df = pd.read_csv(r"C:\Users\Cezary\Downloads\Frankfurt wikidata properties - osoby-relacje-byty.csv")
df = df.loc[(df['relation'].notnull()) &
            (df['person'].notnull()) &
            (df['entity'].notnull())]

# 2. Initialize a directed graph (use nx.Graph() for undirected)
G = nx.DiGraph()

# 3. Add edges with the 'relation' as edge attribute
for _, row in df.iterrows():
    G.add_edge(row['person'], row['entity'], relation=row['relation'])

# 4. Print basic graph info
print(f"Number of nodes: {G.number_of_nodes()}")
print(f"Number of edges: {G.number_of_edges()}")

# 5. Show a few sample edges with their relation labels
print("\nSample edges (person -> entity : relation):")
for u, v, data in list(G.edges(data=True))[:10]:
    print(f"{u} -> {v} : {data['relation']}")



Sigma(G, 
      node_color="tag",
      node_label_size=G.degree,
      node_size=G.degree
     )

Sigma.write_html(
    G,
    'jesuit_wikidata_network.html',
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