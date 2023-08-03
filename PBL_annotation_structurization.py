# pip install spacy scikit-learn
# python -m spacy download en_core_web_lg

import spacy
import numpy as np
from sklearn.cluster import KMeans
import cx_Oracle
from pbl_credentials import pbl_user, pbl_password
import pandas as pd
from tqdm import tqdm

#%% PBL connection
dsn_tns = cx_Oracle.makedsn('pbl.ibl.poznan.pl', '1521', service_name='xe')
connection = cx_Oracle.connect(user=pbl_user, password=pbl_password, dsn=dsn_tns, encoding='windows-1250')

pbl_query = """select z.za_type, za_adnotacje
from pbl_zapisy z
where za_adnotacje is not null"""
pbl_query = pd.read_sql(pbl_query, con=connection).fillna(value = np.nan)
test = pbl_query.sample(1000)

#%%
# Sample data (replace this with your actual 300,000 text fields)
all_text_fields = list(set(test['ZA_ADNOTACJE'].to_list()))

# Load spaCy's pre-trained model with large word vectors
nlp = spacy.load('pl_core_news_lg')

# Preprocess the text data and get Word Embeddings
def preprocess_and_embed(text_fields):
    embeddings = []
    for text in text_fields:
        doc = nlp(text)
        embedding = doc.vector.astype(np.float32)  # Convert to float32
        embeddings.append(embedding)
    return np.array(embeddings)

# Set the KMeans clustering parameters
num_clusters = 70

# Batch processing function
def process_batches(all_text_fields, batch_size):
    num_batches = len(all_text_fields) // batch_size + 1
    for batch_idx in range(num_batches):
        start_idx = batch_idx * batch_size
        end_idx = min((batch_idx + 1) * batch_size, len(all_text_fields))
        batch_text_fields = all_text_fields[start_idx:end_idx]
        embeddings = preprocess_and_embed(batch_text_fields)
        yield embeddings

# Concatenate embeddings from all batches
all_embeddings = np.vstack(list(process_batches(all_text_fields, batch_size=1000)))

# Initialize KMeans clustering with proper parameters
clusterer = KMeans(n_clusters=num_clusters)

# Fit the model to the data
clusterer.fit(all_embeddings)

# Get the cluster labels
labels = clusterer.labels_

# Organize the text fields into clusters
clustered_texts = {}
for i, text in enumerate(all_text_fields):
    cluster_id = labels[i]
    if cluster_id not in clustered_texts:
        clustered_texts[cluster_id] = []
    clustered_texts[cluster_id].append(text)

# Print the clusters
for cluster_id, text_fields in clustered_texts.items():
    print(f"Cluster {cluster_id}:")
    for text_field in text_fields:
        print(text_field)
    print()
















