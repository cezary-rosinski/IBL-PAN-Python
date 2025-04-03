from my_functions import gsheet_to_df
import pandas as pd
import os
import pandas as pd
from neo4j import GraphDatabase

#%%

# Folder docelowy
output_dir = "data/neo4j_wotton"
os.makedirs(output_dir, exist_ok=True)

# Wczytanie i czyszczenie nazw kolumn
df = gsheet_to_df('1kxnFmIx1-DW2oIpb67l-uTSWcEzn-k9yykLJ1TE1TKQ', 'Sheet1')
df.columns = df.columns.str.strip()

# Mapy do tworzenia ID
person_map, place_map, language_map = {}, {}, {}

# Wynikowe listy
letters, persons, letter_person = [], [], []
places, letter_place = [], []
languages, letter_language = [], []

# Funkcja do generowania unikalnych ID
def get_id(mapping, key, prefix):
    if key not in mapping:
        mapping[key] = f"{prefix}_{len(mapping) + 1}"
    return mapping[key]

# Główna pętla przetwarzająca dane
for idx, row in df.iterrows():
    letter_id = f"L_{idx+1}"

    # Dodaj list
    letters.append({
        "letter_id": letter_id,
        "date": row["Date"],
        "incipit": row["Incipit"],
        "abstract": row["Abstract"],
        "print_info": row["Print information"],
        "digital_info": row["Digital information"],
        "notes": row["Notes"]
    })

    # Funkcja pomocnicza do dodania osoby
    def add_person(name, uri, role):
        if pd.isna(name):
            return
        key = f"{name}_{role}"
        pid = get_id(person_map, key, "P")
        persons.append({"person_id": pid, "name": name.strip(), "uri": uri, "type": role})
        letter_person.append({"letter_id": letter_id, "person_id": pid, "relation": role.upper()})

    # Autor i odbiorca
    add_person(row["Author"], row["Author URI"], "author")
    add_person(row["Recipient"], row["Recipient URI"], "recipient")

    # Postacie historyczne wspomniane
    figures = row["Historical Figures Mentioned"]
    if pd.notna(figures):
        for entry in str(figures).split("|"):
            if "@" in entry:
                name, uri = entry.split("@", 1)
            elif "$" in entry:
                name = entry.split("$")[0]
                uri = ""
            else:
                name, uri = entry, ""
            add_person(name.strip(), uri.strip(), "historical_figure")

    # Miejsca: nadania i docelowe
    for label, uri, rel in [
        (row["Origin"], row["Origin URI"], "SENT_FROM"),
        (row["Destination"], row["Destination URI"], "SENT_TO")
    ]:
        if pd.isna(label):
            continue
        place_id = get_id(place_map, label, "PL")
        places.append({"place_id": place_id, "name": label.strip(), "uri": uri})
        letter_place.append({"letter_id": letter_id, "place_id": place_id, "relation": rel})

    # Język
    lang = row["Language"]
    if pd.notna(lang):
        lang_id = get_id(language_map, lang.strip(), "LNG")
        languages.append({"language_id": lang_id, "name": lang.strip()})
        letter_language.append({"letter_id": letter_id, "language_id": lang_id})

# Funkcja do zapisu CSV
def save_csv(data, name):
    df_out = pd.DataFrame(data).drop_duplicates()
    df_out.to_csv(os.path.join(output_dir, name), index=False)

# Zapis plików
save_csv(letters, "letters.csv")
save_csv(persons, "persons.csv")
save_csv(letter_person, "letter_person.csv")
save_csv(places, "places.csv")
save_csv(letter_place, "letter_place.csv")
save_csv(languages, "languages.csv")
save_csv(letter_language, "letter_language.csv")

#%%

# Konfiguracja połączenia z neo4j
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"  # zmień na swoje hasło

# Ścieżka do pliku Excel
# EXCEL_FILE = "Wotton_Jes_Letters.xlsx"

# 1. Wczytaj dane z pliku Excel
# df = pd.read_excel(EXCEL_FILE)
df = gsheet_to_df('1kxnFmIx1-DW2oIpb67l-uTSWcEzn-k9yykLJ1TE1TKQ', 'Sheet1')

# 2. Utwórz połączenie z bazą neo4j
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def import_document(tx, record):
    """
    Tworzy węzeł Document z właściwościami odczytanymi z rekordu.
    Zakładamy, że rekord posiada kolumny: title, author, date, text, tags.
    """
    query = """
    CREATE (d:Document {
        title: $title,
        author: $author,
        date: $date,
        text: $text,
        tags: $tags
    })
    """
    tx.run(query,
           title=record.get("title"),
           author=record.get("author"),
           date=record.get("date"),
           text=record.get("text"),
           tags=record.get("tags"))

# 3. Importuj dane do neo4j
with driver.session() as session:
    for _, row in df.iterrows():
        session.write_transaction(import_document, row.to_dict())

# 4. Tworzenie relacji na podstawie tagów
# Zakładamy, że kolumna tags zawiera tekst z tagami oddzielonymi przecinkami
with driver.session() as session:
    # Utworzenie węzłów Tag oraz relacji HAS_TAG
    session.run("""
    MATCH (d:Document)
    WHERE d.tags IS NOT NULL
    WITH d, split(d.tags, ',') as tagList
    UNWIND tagList as tag
    MERGE (t:Tag {name: trim(tag)})
    MERGE (d)-[:HAS_TAG]->(t)
    """)
    # Utworzenie relacji między dokumentami, które dzielą ten sam tag
    session.run("""
    MATCH (d1:Document)-[:HAS_TAG]->(t:Tag)<-[:HAS_TAG]-(d2:Document)
    WHERE id(d1) < id(d2)
    MERGE (d1)-[r:SIMILAR_TAGS]->(d2)
    ON CREATE SET r.commonTags = 1
    ON MATCH SET r.commonTags = r.commonTags + 1
    """)

# 5. Projekcja grafu i uruchomienie algorytmu node2vec
with driver.session() as session:
    # Projekcja grafu do pamięci (używamy węzłów Document i relacji SIMILAR_TAGS)
    session.run("""
    CALL gds.graph.project(
      'documentGraph',
      'Document',
      {
        SIMILAR_TAGS: {
          properties: 'commonTags'
        }
      }
    )
    """)
    
    # Uruchomienie node2vec - uzyskujemy wektory osadzeń dla dokumentów
    print("Wyniki node2vec:")
    result = session.run("""
    CALL gds.node2vec.stream('documentGraph', {embeddingDimension: 16, iterations: 10})
    YIELD nodeId, embedding
    RETURN gds.util.asNode(nodeId).title AS title, embedding
    """)
    for record in result:
        print(f"Dokument: {record['title']}, Embedding: {record['embedding']}")

# 6. Klasteryzacja przy użyciu Louvain
with driver.session() as session:
    print("\nWyniki Louvain (klastry):")
    result = session.run("""
    CALL gds.louvain.stream('documentGraph')
    YIELD nodeId, communityId
    RETURN gds.util.asNode(nodeId).title AS title, communityId
    ORDER BY communityId
    """)
    for record in result:
        print(f"Dokument: {record['title']}, Klaster: {record['communityId']}")

# Zamknięcie połączenia
driver.close()



