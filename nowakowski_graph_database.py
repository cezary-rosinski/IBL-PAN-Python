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

#%% def

def get_birthyear(birthdate):
    try:
        if birthdate[0].isnumeric():
            b = int(birthdate.split('-')[0])
        else: b = -int(birthdate.split('-')[1])
    except TypeError:
        b = None
    return b

def get_deathyear(deathdate):
    try:
        if deathdate[0].isnumeric():
            d = int(deathdate.split('-')[0])
        else: d = -int(deathdate.split('-')[1])
    except TypeError:
        d = None
    return d

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
df_texts = df_people = gsheet_to_df('1M2gc-8cGZ8gh8TTnm4jl430bL4tccdri9nw-frUWYLQ', 'texts')
df_people = df_people = gsheet_to_df('1M2gc-8cGZ8gh8TTnm4jl430bL4tccdri9nw-frUWYLQ', 'people')
#%% --- GRAPH ---
g = Graph()

g.bind("jesuit_calvinist", JECAL)
g.bind("dcterms", dcterms)
g.bind("fabio", FABIO)
g.bind("geo", geo)
g.bind("bibo", bibo)
g.bind("sch", schema)
g.bind("biro", BIRO)
g.bind("foaf", FOAF)
g.bind("wdt", WDT)
g.bind("owl", OWL)

# 4) Person
def add_person(row):
    pid = str(r["person_id"])
    person = JECAL[f"Person/{pid}"]
    g.add((person, RDF.type, schema.Person))
    g.add((person, schema.name, Literal(r["person_name"])))
    if pd.notnull(row["person_wikidata_id"]):
        g.add((person, OWL.sameAs, WDT[row["person_wikidata_id"]]))
    if pd.notnull(row['birthdate']):
        year = get_birthyear(row['birthdate'])
        g.add((person, schema.birthDate, Literal(year, datatype=XSD.gYear)))
    if pd.notnull(row['deathdate']):
        year = get_deathyear(row['deathdate'])
        g.add((person, schema.deathDate, Literal(year, datatype=XSD.gYear)))
    if pd.notnull(row['productivity_century']):
        g.add((person, JECAL.productivityCentury, Literal(row['productivity_century'])))
    # if pd.notnull(r["birthplace"]):
    #     # relation Person->Place
    #     place_id = str(row["birthplace"])
    #     g.add((person, schema.birthPlace, RECH[f"Place/{place_id}"]))
    # if pd.notnull(r["deathplace"]):
    #     # relation Person->Place
    #     place_id = str(row["deathplace"])
    #     g.add((person, schema.deathPlace, RECH[f"Place/{place_id}"]))

for _, r in df_people.iterrows():
    add_person(r)   

# 5) Text 
def add_text(row):
# for _, row in df_novels.iterrows():
    tid = str(row["Work ID"])
    text = JECAL[f"Text/{tid}"]
    g.add((text, RDF.type, schema.Text))
    g.add((text, schema.title, Literal(row["Title"])))
    for a in row['Author ID'].split(';'):
        g.add((text, schema.author, JECAL[f"Person/{a.strip()}"]))
    # g.add((text, dcterms.publisher, RECH[f"Organization/{row['institution_id']}"]))
    g.add((text, dcterms.publisher, Literal(row['Publisher'])))
    g.add((text, schema.datePublished, Literal(row['Date'], datatype=XSD.gYear)))
    g.add((text, schema.genre, Literal(row['genre'])))
    g.add((text, schema.inLanguage, Literal(row['language'])))
    g.add((text, RECH.whichNovel, Literal(row['debut novel/further novel'])))
    g.add((text, schema.about, Literal(row['subject'])))
    if pd.notnull(row['reason for violence']):
        for rfv in row['reason for violence'].split(','):
            g.add((text, RECH.reasonForViolence, Literal(rfv.strip())))
    if pd.notnull(row['art of violence']):
        for afv in row['art of violence'].split(','):
            g.add((text, RECH.reasonForViolence, Literal(afv.strip())))
    if pd.notnull(row['klappentext']):
        g.add((text, schema.description, Literal(row['klappentext'])))
    if pd.notnull(row['rezensionsnotiz']):
        try:
            rec = literal_eval(row['rezensionsnotiz'])
            for r in rec:
                g.add((text, schema.review, Literal(r)))
        except SyntaxError:
            g.add((text, schema.review, Literal(row['rezensionsnotiz'])))
    if pd.notnull(row['keywords']):
        for k in row['keywords'].split('|'):
            g.add((text, schema.keywords, Literal(k)))
        
for _, r in df_novels.iterrows():
    add_text(r)    

# --- EXPORT ---
g.serialize(destination=OUTPUT_TTL, format="turtle")
print(f"RDF triples written to {OUTPUT_TTL}")






























