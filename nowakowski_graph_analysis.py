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
from rdflib import Graph, Namespace
from rdflib.namespace import DCTERMS
import matplotlib.pyplot as plt

#%% wczytanie grafu
# 1) Załaduj graf RDF
g = Graph()
g.parse("jecal.ttl", format="turtle")

# 2) Zdefiniuj przestrzenie nazw
FABIO = Namespace("http://purl.org/spar/fabio/")
JC = Namespace("https://example.org/jesuit_calvinist/")
#%% 3

query = """
PREFIX schema: <http://schema.org/>
PREFIX jc: <https://example.org/jesuit_calvinist/>

SELECT ?author (COUNT(DISTINCT ?text) AS ?count)
WHERE {
  ?text a schema:Text ;
        jc:keyAuthorsCited ?author .

  # upewniamy się, że przynajmniej jedna wartość editionType to "Original"
  FILTER EXISTS {
    ?text jc:editionType ?otype .
    FILTER(LCASE(str(?otype)) = "original")
  }

  # przynajmniej jedna wartość confessionalProfile to "Roman Catholic"
  FILTER EXISTS {
    ?text jc:confessionalProfile ?cp .
    FILTER(str(?cp) = "Roman Catholic")
  }

  # przynajmniej jedna wartość targetedConfession to "Reformed Evangelical"
  FILTER EXISTS {
    ?text jc:targetedConfession ?tc .
    FILTER(LCASE(str(?tc)) = "reformed evangelical")
  }
}
GROUP BY ?author
ORDER BY DESC(?count)
LIMIT 20

"""

# 3. Wykonanie zapytania i przygotowanie DataFrame
results = g.query(query)
df = pd.DataFrame(results, columns=["author", "count"])
df["author"] = df["author"].astype(str)
df["count"] = df["count"].astype(int)

# 4. Wizualizacja
plt.figure(figsize=(12, 6))
plt.bar(df["author"], df["count"])
plt.xticks(rotation=45, ha='right')
plt.ylabel("Liczba cytowań")
plt.title("Top 20 najczęściej cytowanych autorów")
plt.tight_layout()
plt.show()










# 3) Zapytanie SPARQL
query = """
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX fabio:   <http://purl.org/spar/fabio/>
PREFIX jc:      <https://example.org/jesuit_calvinist/>

SELECT ?author (COUNT(?citation) AS ?count)
WHERE {
  ?text dcterms:hasEdition ?edition .
  ?edition jc:editionType ?editionType .
  FILTER(CONTAINS(LCASE(str(?editionType)), "original")) .
  ?text jc:confessionalProfile "Roman Catholic" .
  ?text jc:targetedConfession "Reformed Evangelical" .
  ?text fabio:cites ?citation .
  ?citation fabio:hasAuthor ?author .
}
GROUP BY ?author
ORDER BY DESC(?count)
LIMIT 20
"""

# 4) Wykonaj zapytanie i przetwórz wyniki
results = g.query(query)
data = [
    (str(row.author).rsplit('/', 1)[-1].replace('_', ' '), int(row.count))
    for row in results
]
df = pd.DataFrame(data, columns=["Autor", "Liczba_cytowań"])

# 5) Wyświetl tabelę w konsoli
print(df.to_string(index=False))

# 6) Wykres słupkowy
plt.figure(figsize=(12, 6))
plt.bar(df["Autor"], df["Liczba_cytowań"])
plt.xticks(rotation=45, ha='right')
plt.xlabel("Autor")
plt.ylabel("Liczba cytowań")
plt.title("Top 20 najczęściej cytowanych autorów; editionType = Original; \nconfessionProfile = Roman Catholic; targetedConfression = Reformed Evangelical")
plt.tight_layout()
plt.show()