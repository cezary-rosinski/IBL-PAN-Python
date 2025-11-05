#code by Patryk Hubar-Kołodziejczyk
import pandas as pd

import nltk
nltk.download('punkt')
nltk.download('punkt_tab')

import os
import glob

from sentence_transformers import SentenceTransformer

from umap import UMAP
from hdbscan import HDBSCAN
from sklearn.feature_extraction.text import CountVectorizer

from bertopic import BERTopic
from bertopic.representation import KeyBERTInspired, MaximalMarginalRelevance, OpenAI, PartOfSpeech
from tqdm import tqdm

import spacy
nlp = spacy.load("pl_core_news_lg")
import time



#%%
folder_path = r"data\poloniści\pełne teksty/"
txt_files = glob.glob(os.path.join(folder_path, "*.txt"))
processed_texts = []

for file_path in tqdm(txt_files[:100]):
    with open(file_path, "r", encoding="utf-8") as file:
        lines = file.readlines()
        cleaned = [line.strip() for line in lines if line.strip()]
        processed_texts.extend(cleaned)

print(f"Wczytano {len(processed_texts)} linii z {len(txt_files)} plików.")

def load_stopwords(file_path: str, encoding: str = "utf-8") -> list[str]:
    with open(file_path, "r", encoding=encoding) as file:
        return [line.strip() for line in file if line.strip()]


POLISH_STOPWORDS = load_stopwords(r"data\poloniści\stopwords-pl.txt")
#%%
polish_st = "sdadas/st-polish-paraphrase-from-distilroberta"
embedding_model = SentenceTransformer(polish_st)
embeddings = embedding_model.encode(processed_texts, show_progress_bar=True)
#%%
umap_model = UMAP(
    n_neighbors=15,
    n_components=5,
    min_dist=0.0,
    metric='cosine',
    random_state=42
)


hdbscan_model = HDBSCAN(
    min_cluster_size=25,
    metric='euclidean',
    cluster_selection_method='eom',
    prediction_data=True
)


vectorizer_model = CountVectorizer(
    stop_words=POLISH_STOPWORDS,
    min_df=2,
    ngram_range=(1, 3)
)


# KeyBERT
keybert_model = KeyBERTInspired()

#polski dziad
pos_model = PartOfSpeech("pl_core_news_lg")

# MMR
mmr_model = MaximalMarginalRelevance(diversity=0.3)

representation_model = {
    "KeyBERT": keybert_model,
    "MMR": mmr_model,
    "POS": pos_model
}
#%%
start = time.time()
topic_model = BERTopic(

  embedding_model=embedding_model,
  umap_model=umap_model,
  hdbscan_model=hdbscan_model,
  vectorizer_model=vectorizer_model,
  representation_model=representation_model,

  # parametry
  top_n_words=10,
  verbose=True
)

topics, probs = topic_model.fit_transform(processed_texts, embeddings)
#%%
topic_model.get_topic_info().to_csv("data/poloniści/topiki.csv", encoding='utf-8')
end = time.time()

print(f"Czas wykonania: {end - start:.4f} sekundy")
















