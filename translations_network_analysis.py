import csv
from operator import itemgetter
import networkx as nx
from networkx.algorithms import community
from collections import Counter
import pandas as pd
from itertools import product



with open('quakers_nodelist.csv', 'r') as nodecsv: # Open the file
    nodereader = csv.reader(nodecsv) # Read the csv
    # Retrieve the data (using Python list comprhension and list slicing to remove the header row, see footnote 3)
    nodes = [n for n in nodereader][1:]

node_names = [n[0] for n in nodes] # Get a list of only the node names

with open('quakers_edgelist.csv', 'r') as edgecsv: # Open the file
    edgereader = csv.reader(edgecsv) # Read the csv
    edges = [tuple(e) for e in edgereader][1:] # Retrieve the data

G = nx.Graph()
G.add_nodes_from(node_names)
G.add_edges_from(edges)


print(nx.info(G))

hist_sig_dict = {}
gender_dict = {}
birth_dict = {}
death_dict = {}
id_dict = {}

for node in nodes: # Loop through the list, one row at a time
    hist_sig_dict[node[0]] = node[1]
    gender_dict[node[0]] = node[2]
    birth_dict[node[0]] = node[3]
    death_dict[node[0]] = node[4]
    id_dict[node[0]] = node[5]

nx.set_node_attributes(G, hist_sig_dict, 'historical_significance')
nx.set_node_attributes(G, gender_dict, 'gender')
nx.set_node_attributes(G, birth_dict, 'birth_year')
nx.set_node_attributes(G, death_dict, 'death_year')
nx.set_node_attributes(G, id_dict, 'sdfb_id')

for n in G.nodes(): # Loop through every node, in our data "n" will be the name of the person
    print(n, G.nodes[n]['birth_year']) # Access every node by its name, and then by the attribute "birth_year"

density = nx.density(G)
print("Network density:", density)

fell_whitehead_path = nx.shortest_path(G, source="Margaret Fell", target="George Whitehead")

print("Shortest path between Fell and Whitehead:", fell_whitehead_path)

print("Length of that path:", len(fell_whitehead_path)-1)





network_df = translations_df[['001', 'author_id', 'work_id', 'simple_original_title', 'simple_target_title']]
network_df.loc[network_df['work_id'].isnull(), 'work_id'] = 0
network_df['work_id'] = network_df['work_id'].astype(np.int64)


nemcova = network_df[network_df['author_id'] == '56763450']

groupby = nemcova.groupby('work_id')
nemcova = pd.DataFrame()
for name, group in groupby:
    # name = 122727892
    # group = groupby.get_group(name)
    original_title = Counter(group['simple_original_title'].to_list()).most_common(1)[0][0]
    if pd.notnull(original_title):
        group['simple_original_title'] = original_title
    elif name != 0:
        group['simple_original_title'] = Counter(group['simple_target_title'].to_list()).most_common(1)[0][0]
    else:
        group['simple_original_title'] = '[no title]'
    nemcova = nemcova.append(group)

nemcova_nodes = nemcova.copy().values.tolist()
nemcova_node_names = [n[3] for n in nemcova_nodes]

groupby = nemcova.groupby('work_id')

nemcova_edges = []
for name, group in groupby:
    # name = 1496749
    # group = groupby.get_group(name)
    nemcova_edges.extend(list(product(group['simple_original_title'].to_list(), group['simple_original_title'].to_list())))
    
G = nx.Graph()
G.add_nodes_from(nemcova_node_names)
G.add_edges_from(nemcova_edges)    
    
print(nx.info(G))

density = nx.density(G)
print("Network density:", density)

nemcova.to_excel('nemcova.xlsx', index=False)



#Kundera
kundera = network_df[network_df['author_id'] == '51691735']

groupby = kundera.groupby('work_id')
kundera = pd.DataFrame()
for name, group in groupby:
    # name = 122727892
    # group = groupby.get_group(name)
    original_title = Counter(group['simple_original_title'].to_list()).most_common(1)[0][0]
    if pd.notnull(original_title):
        group['simple_original_title'] = original_title
    elif name != 0:
        group['simple_original_title'] = Counter(group['simple_target_title'].to_list()).most_common(1)[0][0]
    else:
        group['simple_original_title'] = '[no title]'
    kundera = kundera.append(group)

kundera_nodes = kundera.copy().values.tolist()
kundera_node_names = [n[3] for n in kundera_nodes]

groupby = kundera.groupby('work_id')

kundera_edges = []
for name, group in groupby:
    # name = 1496749
    # group = groupby.get_group(name)
    kundera_edges.extend(list(product(group['simple_original_title'].to_list(), group['simple_original_title'].to_list())))
    
G = nx.Graph()
G.add_nodes_from(kundera_node_names)
G.add_edges_from(kundera_edges)    
    
print(nx.info(G))

density = nx.density(G)
print("Network density:", density)

kundera.to_excel('kundera.xlsx', index=False)




























