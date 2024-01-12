from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET 
import lxml.etree
import pandas as pd
import requests
from tqdm import tqdm
from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import RDF, RDFS, FOAF, XSD, OWL
import networkx as nx
import json
import matplotlib.pyplot as plt
from rdflib.extras.external_graph_libs import rdflib_to_networkx_multidigraph

with open(r"C:\Users\Cezary\Desktop\fp_cytowania\Ner\Edward Balcerzan_„Narodowość” poetyki – dylematy typologiczne.txt", 'r', encoding='utf8') as f:
    data = f.read()

root = ET.fromstring(data)

for child in root:
    print(child.tag, child.attrib)

toks = []
for tok in root.iter('tok'):
    toks.append(tok)
    
ok = []
ints = []
4747, 4748, 4708, 4709, 4050
ok = []
for i, tok in enumerate(toks):
    # tok = toks[4747]
    # if i in ints:
    #     print(i, ET.tostring(tok))
    # ET.tostring(toks[0])
    # tok = toks[0]
    if tok.findall("./ann"):
        # ints.append(i)
        test = tok.findall("./ann")
        for ann in test:
            # ann = test[0]
            if 'nam_liv' in ann.attrib.values() and 'head' in ann.attrib.keys():
                text = tok.find("./lex/base").text
                ok.append((i, text, 1))
            elif 'nam_liv' in ann.attrib.values():
                text = tok.find("./lex/base").text
                ok.append((i, text, 0))
                
people = []
for i, name, head in ok:
    if name[0].isupper():
        people.append((i, name, head))
        
df = pd.DataFrame(people, columns=['index', 'name', 'head'])
result = []
help_indexes = []
for i, row in df.iterrows():
    # i = 1
    # row = df.iloc[i,:]
    if row['head'] == 1:
        result.append(row['name'])
        help_indexes.append(i)
    elif row['head'] == 0 and row['index'] == df.iloc[i-1,:]['index'] + 1 and i-1 in help_indexes:
        result[-1] = result[-1] + ' ' + row['name']
        help_indexes.append(i)
        
        
        
dictionary = {}
for term in tqdm(result):
    url = f'http://viaf.org/viaf/AutoSuggest?query={term}'
    try:
        r = requests.get(url).json()
        if r.get('result'):
            dictionary.update({term: {'id': r.get('result')[0].get('viafid'),
                                      'nametype': r.get('result')[0].get('nametype')}})
        else:
            dictionary.update({term: None})
    except:
        pass
    
final_dictionary = {k:v for k,v in dictionary.items() if v and v.get('nametype') == 'personal'}
        


VIAF = Namespace("http://viaf.org/viaf/")
bibo = Namespace("http://purl.org/ontology/bibo/")

g_id = Graph()
g_id.bind("bibo", bibo)
author_id = URIRef(VIAF + '29548791')
for k,v in final_dictionary.items():
    g_id.add((author_id, bibo.cites, URIRef(VIAF + v.get('id'))))
    
g_id = rdflib_to_networkx_multidigraph(g_id)
pos = nx.spring_layout(g_id, seed=42, k=0.9)
labels = nx.get_edge_attributes(g_id, 'label')
plt.figure(figsize=(12, 10))
nx.draw(g_id, pos, with_labels=True, font_size=10, node_size=700, node_color='lightblue', edge_color='gray', alpha=0.6)
nx.draw_networkx_edge_labels(g_id, pos, edge_labels=labels, font_size=8, label_pos=0.3, verticalalignment='baseline')
plt.title('Knowledge Graph')
plt.show()
    
    
g_label= nx.Graph()
author_label = 'Edward Balcerzan'
for k,v in final_dictionary.items():
    g_label.add_edge(author_label, k, label='cites')
    
# g_id.serialize("fp1_id.ttl", format = "turtle")
# g_label.draw("fp1_label.ttl", format = "turtle")
pos = nx.spring_layout(g_label, seed=42, k=0.9)
labels = nx.get_edge_attributes(g_label, 'label')
plt.figure(figsize=(12, 10))
nx.draw(g_label, pos, with_labels=True, font_size=10, node_size=700, node_color='lightblue', edge_color='gray', alpha=0.6)
nx.draw_networkx_edge_labels(g_label, pos, edge_labels=labels, font_size=8, label_pos=0.3, verticalalignment='baseline')
plt.title('Knowledge Graph')
plt.show()
















        
        
        

def row_checker(df, row):
    i = row.name
    index = row['index']
    if row['head'] == 1:
        indexes = []
        i = row.name
        while df.loc[i,:].name
    
        
                
                ann.attrib
                
                
                sprawdzić head i połączyć nazwę
                
                
                ok.append(text)
                
                
            for el in ann:
                ann.attrib
            print(ET.tostring(tok))
    
        for ann in tok.findall("./ann"):
            # ann = e.findall("./ann")
            for el in ann:
                if 'nam_liv' in el.attrib.values():
                    print(i)
                    break
                    tok = e
                    text = tok.find("./lex/base").text
                    ok.append(text)
                    
            ok.extend([e.text for e in ann if 'nam_liv' in e.attrib.values()])
            # ET.tostring(ann[0])
        

    if tok.findall("./ann"):
        ok.append(tok)

for e in ok:
    e = ok[0]
    ET.tostring(e)


    
    lex = tok.find('lex').find('base')
    ET.tostring(lex)
    lex.text
    
    
    
    dir(lex)


        
dir(tok)


    print(ET.tostring(tok))


for lev1 in root.getiterator():
    for lev2 in lev1.findall('chunk'):
        for lev3 in lev2.findall('sentence'):
            for lev4 in lev3.findall('tok'):
                ET.tostring(tok)




chunks = root.findall('chunk')
for chunk in chunks:
    sentences = chunk.findall('sentence')
    for sentence in sentences:
        toks = sentence.findall('tok')
        for tok in toks:
            break
            print(tok['orth'])
        print(tok.text)
        
    print(sentence)
    
   dir(tok) 

tok = chunks[0].findall('sentence')[0].findall('tok')[0]

for t in tok:
    print(t.attrib)


[elem.tag for elem in tok.iter()]
tok.text
tok.tag
tok.items()
tok.attrib   
tok.keys()

ET.tostring(tok)

lxml.etree.tostring(tok, encoding="unicode", pretty_print=True)



num liv, któRe nie mają 0
num liv, któRe mają 0 --> patrzysz, co mają jeszcze, co inne niż 0
wszystko, co ma num live 0 i ma coś innego, gdzie head == 0 może być potencjalnie człowiekiem