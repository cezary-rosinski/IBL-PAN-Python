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



















