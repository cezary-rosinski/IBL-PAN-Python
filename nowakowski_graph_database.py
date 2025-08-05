import sys
# sys.path.insert(1, 'D:\IBL\Documents\IBL-PAN-Python')
sys.path.insert(1, 'C:/Users/Cezary/Documents/IBL-PAN-Python')
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
OUTPUT_TTL = "jecal.ttl"

#%% --- LOAD ---
df_texts = gsheet_to_df('1M2gc-8cGZ8gh8TTnm4jl430bL4tccdri9nw-frUWYLQ', 'texts')
texts_ids = df_texts['Work ID'].to_list()
df_people = gsheet_to_df('1M2gc-8cGZ8gh8TTnm4jl430bL4tccdri9nw-frUWYLQ', 'people')
df_places = gsheet_to_df('1M2gc-8cGZ8gh8TTnm4jl430bL4tccdri9nw-frUWYLQ', 'places')
df_events = gsheet_to_df('1M2gc-8cGZ8gh8TTnm4jl430bL4tccdri9nw-frUWYLQ', 'events')
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

# 1) Place
def add_place(row):
    pid = str(row["place_id"])
    place = JECAL[f"Place/{pid}"]
    g.add((place, RDF.type, schema.Place))
    g.add((place, schema.name, Literal(row["name"])))
    if pd.notnull(row["wikidata_id"]):
        g.add((place, OWL.sameAs, WDT[row["wikidata_id"]]))
    if pd.notnull(row['latitude']):
        latitude = Literal(row['latitude'], datatype=XSD.float)
        g.add((place, geo.lat, latitude))
        longitude = Literal(row['longitude'], datatype=XSD.float)
        g.add((place, geo.long, longitude))

for _, r in df_places.iterrows():
    add_place(r)

# 2) Event

def add_event(row):
    pid = str(row['event_id'])
    event = JECAL[f"Event/{pid}"]
    g.add((event, RDF.type, schema.Event))
    g.add((event, schema.name, Literal(row['name'])))
    if pd.notnull(row["place_id"]):
        g.add((event, schema.location, JECAL[f"Place/{row['place_id']}"]))
    g.add((event, schema.startDate, Literal(row['year'], datatype=XSD.gYear)))
    g.add((event, schema.endDate, Literal(row['year'], datatype=XSD.gYear)))
    
    Literal('0410', datatype=XSD.gYear)
    g.add((event, schema.additionalType, Literal(row['type'])))

for _, r in df_events.iterrows():
    add_event(r)

# 3) Person
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
    if pd.notnull(row["birthplace_id"]):
        place_id = str(row["birthplace_id"])
        g.add((person, schema.birthPlace, JECAL[f"Place/{place_id}"]))
    if pd.notnull(r["deathplace_id"]):
        # relation Person->Place
        place_id = str(row["deathplace_id"])
        g.add((person, schema.deathPlace, JECAL[f"Place/{place_id}"]))

for _, r in df_people.iterrows():
    add_person(r)

# 4) Text 
def add_text(row):
# for _, row in df_novels.iterrows():
    tid = str(row["Work ID"])
    text = JECAL[f"Text/{tid}"]
    g.add((text, RDF.type, schema.Text))
    for a in row['Author ID'].split(';'):
        g.add((text, schema.author, JECAL[f"Person/{a.strip()}"]))
    if pd.notnull(row['Additional Authors ID']):
        for a in row['Additional Authors ID'].split(';'):
            if a.strip() != 'no id':
                g.add((text, JECAL.additionalAuthor, JECAL[f"Person/{a.strip()}"]))  
    g.add((text, schema.title, Literal(row["Title"])))
    g.add((text, schema.datePublished, Literal(row['Date'], datatype=XSD.gYear)))
    if pd.notnull(row['Date Additional Info']):
        g.add((text, JECAL.dateAdditionalInfo, Literal(row['Date Additional Info'])))
    if pd.notnull(row["place_id"]):
        g.add((text, FABIO.hasPlaceOfPublication, JECAL[f"Place/{row['place_id']}"]))
    if pd.notnull(row['Place Additional Info']):
        g.add((text, JECAL.placeAdditionalInfo, Literal(row['Place Additional Info'])))
    g.add((text, dcterms.publisher, Literal(row['Publisher'])))
    g.add((text, schema.inLanguage, Literal(row['Languages'])))
    g.add((text, JECAL.documentType, Literal(row['Document Type'])))
    if pd.notnull(row['Genre']):
        for genre in row['Genre'].split(';'):
            g.add((text, schema.genre, Literal(genre.strip())))
    if row['Number of Pages'] != 'No Data':
        g.add((text, FABIO.hasPageCount, Literal(row['Number of Pages'])))
    for p in row['Digitized Copy'].split(';'):
        g.add((text, JECAL.digitizedCopy, Literal(p.strip())))
    for p in row['Existing Physical Copy'].split(';'):
        g.add((text, JECAL.existingPhysicalCopy, Literal(p.strip())))
    g.add((text, JECAL.confessionalProfile, Literal(row['Confessional Profile'])))
    for a in row['Targeted Confession'].split(';'):
        g.add((text, JECAL.targetedConfession, Literal(a.strip())))
    if row['Preface'] != 'No Data':
        g.add((text, JECAL.preface, Literal(row['Preface'])))   
    if pd.notnull(row['Dedicated to ID']):
        for a in row['Dedicated to ID'].split(';'):
            if a.strip() != 'no id':
                g.add((text, JECAL.dedicatedTo, JECAL[f"Person/{a.strip()}"]))
    if pd.notnull(row['Key Historical Figures Mentioned ID']):
        for a in row['Key Historical Figures Mentioned ID'].split(';'):
            if a.strip() != 'no id':
                g.add((text, JECAL.keyHistoricalFiguresMentioned, JECAL[f"Person/{a.strip()}"]))
    if pd.notnull(row['Key Authors Cited ID']):
        for a in row['Key Authors Cited ID'].split(';'):
            if a.strip() != 'no id':
                g.add((text, JECAL.keyAuthorsCited, JECAL[f"Person/{a.strip()}"]))
    if pd.notnull(row['Tumults and Mentioned']):
        for p in row['Tumults and Mentioned'].split(';'):
            g.add((text, JECAL.tumultMentioned, JECAL[f"Event/{p.strip()}"]))
    if row['Polemical Themes (Random Order)'] != 'No Data':
        for p in row['Polemical Themes (Random Order)'].split(';'):
            g.add((text, JECAL.polemicalTheme, Literal(p.strip())))
    if row['Categorized Polemical Themes (Random Order)'] != 'No Data':
        for p in row['Categorized Polemical Themes (Random Order)'].split(';'):
            g.add((text, JECAL.categorizedPolemicalTheme, Literal(p.strip())))
    if pd.notnull(row['Discussed Issues (Random Order)']) and row['Discussed Issues (Random Order)'] != 'No Data':
        for p in row['Discussed Issues (Random Order)'].split(';'):
            g.add((text, JECAL.discussedIssue, Literal(p.strip())))
    if pd.notnull(row['Response to Author ID']):
        for a in row['Response to Author ID'].split(';'):
            if a.strip() != 'no id':
                g.add((text, JECAL.responseToAuthor, JECAL[f"Person/{a.strip()}"]))
    if pd.notnull(row['Response to Work']):
        for p in row['Response to Work'].split(';'):
            if p.strip() in texts_ids:
                g.add((text, JECAL.responseToWork, JECAL[f"Text/{p.strip()}"]))
            elif p.strip() != 'No Data':
                g.add((text, JECAL.responseToWork, Literal(p.strip())))
    if row['Format'] != "No Data":
        g.add((text, FABIO.hasFormat, Literal(row['Format'])))
    if pd.notnull(row['Notes']):
        g.add((text, JECAL.notes, Literal(row['Notes'])))
    for p in row['Edition Type'].split(';'):
        g.add((text, JECAL.editionType, Literal(p.strip())))
           
for _, r in df_texts.iterrows():
    add_text(r)

# --- EXPORT ---
g.serialize(destination=OUTPUT_TTL, format="turtle")
print(f"RDF triples written to {OUTPUT_TTL}")



























