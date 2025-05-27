import sys
sys.path.insert(1, 'D:\IBL\Documents\IBL-PAN-Python')
# sys.path.insert(1, 'C:/Users/Cezary/Documents/IBL-PAN-Python')
import pandas as pd
from rdflib import Graph, Namespace, URIRef, Literal, BNode
from rdflib.namespace import RDF, RDFS, XSD, FOAF, OWL
import datetime
import regex as re
from my_functions import gsheet_to_df
from ast import literal_eval

#%%
# --- CONFIG ---
JECAL = Namespace('https://example.org/jesuit_calvinist/')
dcterms = Namespace("http://purl.org/dc/terms/")
rdf = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
FABIO = Namespace("http://purl.org/spar/fabio/")
BIRO = Namespace("http://purl.org/spar/biro/")
VIAF = Namespace("http://viaf.org/viaf/")
geo = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")
bibo = Namespace("http://purl.org/ontology/bibo/")
schema = Namespace("http://schema.org/")
WDT = Namespace("http://www.wikidata.org/entity/")
OUTPUT_TTL = "data/jecal.ttl"

#%% --- LOAD ---
df_novels     = gsheet_to_df('1iU-u4xjotqa3ZLijF5bMU7xWv-Hxgq1n8N3i-7UCdfU', 'novels')
df_people    = gsheet_to_df('1iU-u4xjotqa3ZLijF5bMU7xWv-Hxgq1n8N3i-7UCdfU', 'authors')
df_places     = gsheet_to_df('1iU-u4xjotqa3ZLijF5bMU7xWv-Hxgq1n8N3i-7UCdfU', 'places')
df_institutions = gsheet_to_df('1iU-u4xjotqa3ZLijF5bMU7xWv-Hxgq1n8N3i-7UCdfU', 'institutions')
df_prizes     = gsheet_to_df('1iU-u4xjotqa3ZLijF5bMU7xWv-Hxgq1n8N3i-7UCdfU', 'prizes')
#%% --- GRAPH ---


















