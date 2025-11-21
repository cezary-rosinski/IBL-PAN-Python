#%% CR -- kod własciwy

import os
import glob
import time
from typing import List

import numpy as np
import pandas as pd
from tqdm import tqdm

from sentence_transformers import SentenceTransformer
from umap import UMAP
from hdbscan import HDBSCAN
from sklearn.feature_extraction.text import CountVectorizer

from bertopic import BERTopic
from bertopic.representation import (
    KeyBERTInspired,
    MaximalMarginalRelevance,
    PartOfSpeech,
)

#%%
# =========================
# KONFIGURACJA ŚCIEŻEK
# =========================
# STOPWORDS_PATH = r"data\poloniści\stopwords-pl.txt"
STOPWORDS_PATH = r"data\poloniści\stopwords-pl_21112025.txt"
FOLDER_PATH = r"data\poloniści\pełne teksty"
OUTPUT_XLSX = r"data\poloniści\topiki_bertopic_v_2.xlsx"

# Maksymalna liczba plików do przetworzenia (None = wszystkie)
MAX_FILES = 100  # możesz zmienić na None, jeśli chcesz wszystkie
start_file, end_file = 0, 100


# =========================
# FUNKCJE POMOCNICZE
# =========================
def load_stopwords(file_path: str, encoding: str = "utf-8") -> List[str]:
    with open(file_path, "r", encoding=encoding) as file:
        return [line.strip() for line in file if line.strip()]


# =========================
# WSTĘPNA KONFIGURACJA MODELI
# =========================

# Stopwordy
POLISH_STOPWORDS = load_stopwords(STOPWORDS_PATH)

# Model embeddingów po polsku (ten sam co u Ciebie)
polish_st = "sdadas/st-polish-paraphrase-from-distilroberta"
embedding_model = SentenceTransformer(polish_st)

# UMAP
umap_model = UMAP(
    n_neighbors=15,
    n_components=5,
    min_dist=0.0,
    metric="cosine",
    random_state=42,
)

# HDBSCAN
hdbscan_model = HDBSCAN(
    min_cluster_size=25,
    metric="euclidean",
    cluster_selection_method="eom",
    prediction_data=True,
)

# Vectorizer – tak jak w Twoim pierwotnym kodzie
vectorizer_model = CountVectorizer(
    stop_words=POLISH_STOPWORDS,
    min_df=2,
    ngram_range=(1, 3),
)

# Reprezentacje tematów
keybert_model = KeyBERTInspired()
pos_model = PartOfSpeech("pl_core_news_lg")
mmr_model = MaximalMarginalRelevance(diversity=0.3)

representation_model = {
    "KeyBERT": keybert_model,
    "MMR": mmr_model,
    "POS": pos_model,
}

#%%

# =========================
# WŁAŚCIWY PIPELINE
# =========================
def main():
    # --- Wczytywanie tekstów: KAŻDA NIEPUSTA LINIA = OSOBNY DOKUMENT ---
    start = time.time()

    txt_files = glob.glob(os.path.join(FOLDER_PATH, "*.txt"))
    txt_files = sorted(txt_files)

    if MAX_FILES is not None:
        txt_files = txt_files[start_file:end_file]

    processed_texts: List[str] = []
    processed_texts_ids: List[str] = []

    for file_path in tqdm(txt_files, desc="Wczytywanie tekstów (linie jako dokumenty)"):
        with open(file_path, "r", encoding="utf-8") as file:
            lines = file.readlines()
            cleaned = [line.strip() for line in lines if line.strip()]
            text_id = os.path.basename(file_path).replace(".txt", "")

            # KLUCZ: każda linia to osobny dokument (jak w Twoim pierwotnym kodzie)
            for line in cleaned:
                processed_texts.append(line)
                # jako ID dokumentu zapisujemy ID pliku (może się powtarzać)
                processed_texts_ids.append(text_id)

    print(f"Wczytano {len(processed_texts)} dokumentów (linii) z {len(txt_files)} plików.")

    if not processed_texts:
        raise ValueError("Brak tekstów do przetworzenia. Sprawdź ścieżkę FOLDER_PATH.")

    # --- Topic modeling (BERTopic) ---
    topic_model = BERTopic(
        embedding_model=embedding_model,
        umap_model=umap_model,
        hdbscan_model=hdbscan_model,
        vectorizer_model=vectorizer_model,
        representation_model=representation_model,
        top_n_words=10,           # tak jak w Twoim CSV
        calculate_probabilities=True,
        verbose=True,
    )

    # Uczymy model – dostajemy topics i probs
    topics, probs = topic_model.fit_transform(processed_texts)

    end = time.time()
    print(
        f"Czas wykonania dla {len(processed_texts)} dokumentów (linii): "
        f"{end - start:.2f} sekundy."
    )

    # =========================
    # GENEROWANIE ARKUSZY XLSX
    # =========================

    # 1) Słowniki tematów BERTopica
    topics_dict = topic_model.get_topics()  # {topic_id: [(word, weight), ...]}

    if not topics_dict:
        raise ValueError(
            "BERTopic nie wygenerował żadnych tematów (topics_dict jest puste). "
            "Spróbuj zmienić parametry HDBSCAN albo sprawdzić teksty."
        )

    # Wszystkie topic_id (mogą zawierać -1)
    all_topic_ids = sorted(topics_dict.keys())

    # Tematy "normalne" (bez -1) i z niepustą reprezentacją
    valid_topic_ids = sorted(
        t for t in all_topic_ids
        if t != -1 and topics_dict[t]
    )

    # Jeśli nie ma żadnych tematów poza -1, traktujemy -1 jako Topic 1
    if not valid_topic_ids:
        valid_topic_ids = sorted(
            t for t in all_topic_ids
            if topics_dict[t]
        )
        print(
            "Uwaga: BERTopic nie wygenerował żadnych tematów poza -1 "
            "(wszystko trafiło do szumu). Eksportuję -1 jako Topic 1."
        )

    if not valid_topic_ids:
        raise ValueError(
            "BERTopic nie wygenerował żadnych niepustych tematów. "
            "Spróbuj zmienić parametry (np. min_cluster_size) "
            "lub sprawdzić, czy dokumenty nie są zbyt krótkie/puste."
        )

    # Mapowanie: wewnętrzny topic_id -> etykieta 1..K (jak w Excelu)
    topic_id_to_label = {topic_id: i + 1 for i, topic_id in enumerate(valid_topic_ids)}

    # =========================
    # PRZYGOTOWANIE MACIERZY PROB (THETA)
    # =========================
    n_docs = len(processed_texts)
    n_valid_topics = len(valid_topic_ids)
    topic_id_to_idx = {tid: i for i, tid in enumerate(valid_topic_ids)}

    theta = np.zeros((n_docs, n_valid_topics), dtype=float)

    probs_mode = "none"
    if probs is not None:
        probs_array = np.asarray(probs, dtype=object)

        # przypadek: 2D (pełna macierz prawdopodobieństw)
        if probs_array.ndim == 2:
            probs_mode = "2d"
            if probs_array.dtype == object:
                probs_array = np.vstack(probs_array)

            # zakładamy, że kolumny odpowiadają tematowi w kolejności rosnących topic_id bez -1
            # i że liczba kolumn = liczbie valid_topic_ids
            if probs_array.shape[1] >= n_valid_topics:
                theta = probs_array[:, :n_valid_topics]
            else:
                # na wszelki wypadek: bierzemy tyle, ile jest
                m = probs_array.shape[1]
                theta[:, :m] = probs_array

        # przypadek: 1D – np. tylko P(przypisany_topik)
        elif probs_array.ndim == 1:
            probs_mode = "1d"
            assigned_probs = np.array(
                [float(x) for x in probs_array], dtype=float
            )
            for i, (t, p) in enumerate(zip(topics, assigned_probs)):
                if t in topic_id_to_idx:
                    theta[i, topic_id_to_idx[t]] = p

    # jeśli probs = None albo nic nie weszło, robimy twarde 1.0 dla przypisanego topiku
    if probs_mode == "none":
        for i, t in enumerate(topics):
            if t in topic_id_to_idx:
                theta[i, topic_id_to_idx[t]] = 1.0

    # =========================
    # ARKUSZE
    # =========================

    # ---------- Arkusz 1: "top 15 słówtopik" ----------
    # model ma top_n_words=10, więc do 15 dobijamy None
    N_TOP_WORDS_WIDE = 15

    top_words_wide = {}
    for topic_id in valid_topic_ids:
        label_idx = topic_id_to_label[topic_id]
        words_weights = topics_dict[topic_id][:N_TOP_WORDS_WIDE]
        words = [w for (w, _) in words_weights]
        if len(words) < N_TOP_WORDS_WIDE:
            words += [None] * (N_TOP_WORDS_WIDE - len(words))
        top_words_wide[f"Topic {label_idx}"] = words

    df_top15 = pd.DataFrame(top_words_wide)

    # ---------- Arkusz 2: "top 10 słów + beta" ----------
    N_TOP_WORDS_LONG = 10

    rows_long = []
    for topic_id in valid_topic_ids:
        label_idx = topic_id_to_label[topic_id]
        for word, weight in topics_dict[topic_id][:N_TOP_WORDS_LONG]:
            rows_long.append(
                {
                    "topic": label_idx,
                    "term": word,
                    "beta": float(weight),
                }
            )

    if rows_long:
        df_top10_beta = pd.DataFrame(rows_long)[["topic", "term", "beta"]]
    else:
        df_top10_beta = pd.DataFrame(columns=["topic", "term", "beta"])

    # ---------- Arkusz 3: "interpretacja MMME" ----------
    rows_interp = []
    for topic_id in valid_topic_ids:
        label_idx = topic_id_to_label[topic_id]
        top_words = topics_dict[topic_id]
        temat = top_words[0][0] if top_words else f"Temat_{label_idx}"
        rows_interp.append({"Topik": f"C{label_idx}", "Temat": temat})

    df_interpretacja = pd.DataFrame(rows_interp)[["Topik", "Temat"]]

    # ---------- Arkusz 4: "topikiteksty" ----------
    max_probs = theta.max(axis=1)
    best_topic_idx = theta.argmax(axis=1)

    top_topics_labels = []
    for i, max_p in enumerate(max_probs):
        if max_p > 0:
            label = topic_id_to_label[valid_topic_ids[best_topic_idx[i]]]
        else:
            label = None
        top_topics_labels.append(label)

    df_topiki_teksty = pd.DataFrame(
        {
            "dokument": processed_texts_ids,
            "Top topik": top_topics_labels,
        }
    )

    # ---------- Arkusz 5: "theta" ----------
    theta_df = pd.DataFrame(
        theta,
        columns=[topic_id_to_label[topic_id] for topic_id in valid_topic_ids],
    )
    theta_df = theta_df[sorted(theta_df.columns)]
    theta_df.insert(0, "dokument", processed_texts_ids)

    # ---------- Zapis do Excela ----------
    os.makedirs(os.path.dirname(OUTPUT_XLSX), exist_ok=True)

    with pd.ExcelWriter(OUTPUT_XLSX, engine="xlsxwriter") as writer:
        df_top15.to_excel(writer, sheet_name="top 15 słówtopik", index=False)
        df_top10_beta.to_excel(writer, sheet_name="top 10 słów + beta", index=False)
        df_interpretacja.to_excel(writer, sheet_name="interpretacja MMME", index=False)
        df_topiki_teksty.to_excel(writer, sheet_name="topikiteksty", index=False)
        theta_df.to_excel(writer, sheet_name="theta", index=False)

    print(f"Zapisano rezultaty do: {OUTPUT_XLSX}")

#%%
start = time.time()

if __name__ == "__main__":
    main()

end = time.time()

print(f"Czas wykonania dla {MAX_FILES} tekstów: {end - start:.4f} sekundy.")
#%%
import os
import glob
import time
from typing import List
from collections import defaultdict

import numpy as np
import pandas as pd
from tqdm import tqdm

from sentence_transformers import SentenceTransformer
from umap import UMAP
from hdbscan import HDBSCAN
from sklearn.feature_extraction.text import CountVectorizer

from bertopic import BERTopic
from bertopic.representation import (
    KeyBERTInspired,
    MaximalMarginalRelevance,
    PartOfSpeech,
)


# =========================
# KONFIGURACJA ŚCIEŻEK
# =========================
STOPWORDS_PATH = r"data\poloniści\stopwords-pl_21112025.txt"
FOLDER_PATH = r"data\poloniści\pełne teksty"
OUTPUT_XLSX = r"data\poloniści\topiki_bertopic_po_spotkaniu_v2.xlsx"

# Maksymalna liczba plików do przetworzenia (None = wszystkie)
MAX_FILES = 100  # możesz zmienić na None, jeśli chcesz wszystkie


# =========================
# FUNKCJE POMOCNICZE
# =========================
def load_stopwords(file_path: str, encoding: str = "utf-8") -> List[str]:
    with open(file_path, "r", encoding=encoding) as file:
        return [line.strip() for line in file if line.strip()]


# =========================
# WSTĘPNA KONFIGURACJA MODELI
# =========================

# Stopwordy
POLISH_STOPWORDS = load_stopwords(STOPWORDS_PATH)

# Model embeddingów po polsku (ten sam, co u Ciebie)
polish_st = "sdadas/st-polish-paraphrase-from-distilroberta"
embedding_model = SentenceTransformer(polish_st)

# UMAP
umap_model = UMAP(
    n_neighbors=15,
    n_components=5,
    min_dist=0.0,
    metric="cosine",
    random_state=42,
)

# HDBSCAN
hdbscan_model = HDBSCAN(
    min_cluster_size=25,
    metric="euclidean",
    cluster_selection_method="eom",
    prediction_data=True,
)

# Vectorizer – jak w Twoim pierwotnym kodzie
vectorizer_model = CountVectorizer(
    stop_words=POLISH_STOPWORDS,
    min_df=2,
    ngram_range=(1, 3),
)

# Reprezentacje tematów
keybert_model = KeyBERTInspired()
pos_model = PartOfSpeech("pl_core_news_lg")
mmr_model = MaximalMarginalRelevance(diversity=0.3)

representation_model = {
    "KeyBERT": keybert_model,
    "MMR": mmr_model,
    "POS": pos_model,
}


# =========================
# WŁAŚCIWY PIPELINE
# =========================
def main():
    # --- Wczytywanie tekstów: KAŻDA NIEPUSTA LINIA = OSOBNY DOKUMENT ---
    start = time.time()

    txt_files = glob.glob(os.path.join(FOLDER_PATH, "*.txt"))
    txt_files = sorted(txt_files)

    if MAX_FILES is not None:
        txt_files = txt_files[:MAX_FILES]

    processed_texts: List[str] = []      # linie (dokumenty)
    processed_texts_ids: List[str] = []  # id pliku dla każdej linii
    doc_ids_unique: List[str] = []       # unikalne ID plików (teksty)

    for file_path in tqdm(txt_files, desc="Wczytywanie tekstów (linie jako dokumenty)"):
        with open(file_path, "r", encoding="utf-8") as file:
            lines = file.readlines()
            cleaned = [line.strip() for line in lines if line.strip()]
            text_id = os.path.basename(file_path).replace(".txt", "")

            # zapamiętujemy unikalne ID tekstu (raz na plik)
            doc_ids_unique.append(text_id)

            # każda linia to osobny dokument
            for line in cleaned:
                processed_texts.append(line)
                processed_texts_ids.append(text_id)

    print(f"Wczytano {len(processed_texts)} dokumentów (linii) z {len(txt_files)} plików.")

    if not processed_texts:
        raise ValueError("Brak tekstów do przetworzenia. Sprawdź ścieżkę FOLDER_PATH.")

    # --- Topic modeling (BERTopic) ---
    topic_model = BERTopic(
        embedding_model=embedding_model,
        umap_model=umap_model,
        hdbscan_model=hdbscan_model,
        vectorizer_model=vectorizer_model,
        representation_model=representation_model,
        top_n_words=20,           # więcej słów, żeby spokojnie mieć 15
        calculate_probabilities=True,
        verbose=True,
    )

    topics, probs = topic_model.fit_transform(processed_texts)

    end = time.time()
    print(
        f"Czas wykonania dla {len(processed_texts)} dokumentów (linii): "
        f"{end - start:.2f} sekundy."
    )

    # =========================
    # GENEROWANIE ARKUSZY XLSX
    # =========================

    topics_dict = topic_model.get_topics()  # {topic_id: [(word, weight), ...]}

    if not topics_dict:
        raise ValueError(
            "BERTopic nie wygenerował żadnych tematów (topics_dict jest puste). "
            "Spróbuj zmienić parametry HDBSCAN albo sprawdzić teksty."
        )

    all_topic_ids = sorted(topics_dict.keys())

    # Tematy "normalne" (bez -1) i z niepustą reprezentacją
    valid_topic_ids = sorted(
        t for t in all_topic_ids
        if t != -1 and topics_dict[t]
    )

    if not valid_topic_ids:
        valid_topic_ids = sorted(
            t for t in all_topic_ids
            if topics_dict[t]
        )
        print(
            "Uwaga: BERTopic nie wygenerował żadnych tematów poza -1 "
            "(wszystko trafiło do szumu). Eksportuję -1 jako Topic 1."
        )

    if not valid_topic_ids:
        raise ValueError(
            "BERTopic nie wygenerował żadnych niepustych tematów. "
            "Spróbuj zmienić parametry (np. min_cluster_size) "
            "lub sprawdzić, czy dokumenty nie są zbyt krótkie/puste."
        )

    # Mapowanie: wewnętrzny topic_id -> etykieta 1..K (jak w Excelu)
    topic_id_to_label = {topic_id: i + 1 for i, topic_id in enumerate(valid_topic_ids)}

    # =========================
    # PRZYGOTOWANIE MACIERZY PROB (THETA) NA POZIOMIE LINII
    # =========================
    n_docs_lines = len(processed_texts)
    n_valid_topics = len(valid_topic_ids)
    topic_id_to_idx = {tid: i for i, tid in enumerate(valid_topic_ids)}

    theta_lines = np.zeros((n_docs_lines, n_valid_topics), dtype=float)

    probs_mode = "none"
    if probs is not None:
        probs_array = np.asarray(probs, dtype=object)

        # 2D (pełna macierz)
        if probs_array.ndim == 2:
            probs_mode = "2d"
            if probs_array.dtype == object:
                probs_array = np.vstack(probs_array)

            # zakładamy: kolumny = tematy (bez -1), w kolejności rosnącej
            if probs_array.shape[1] >= n_valid_topics:
                theta_lines = probs_array[:, :n_valid_topics]
            else:
                m = probs_array.shape[1]
                theta_lines[:, :m] = probs_array

        # 1D – tylko P(przypisany_topik)
        elif probs_array.ndim == 1:
            probs_mode = "1d"
            assigned_probs = np.array(
                [float(x) for x in probs_array], dtype=float
            )
            for i, (t, p) in enumerate(zip(topics, assigned_probs)):
                if t in topic_id_to_idx:
                    theta_lines[i, topic_id_to_idx[t]] = p

    # jeśli probs = None albo nic nie weszło: twarde przypisanie
    if probs_mode == "none":
        for i, t in enumerate(topics):
            if t in topic_id_to_idx:
                theta_lines[i, topic_id_to_idx[t]] = 1.0

    # =========================
    # AGREGACJA: Z LINII DO POZIOMU TEKSTU
    # =========================
    doc_to_indices = defaultdict(list)
    for idx_line, doc_id in enumerate(processed_texts_ids):
        doc_to_indices[doc_id].append(idx_line)

    n_docs_texts = len(doc_ids_unique)
    theta_texts = np.zeros((n_docs_texts, n_valid_topics), dtype=float)

    for j, doc_id in enumerate(doc_ids_unique):
        idxs = doc_to_indices.get(doc_id, [])
        if not idxs:
            continue
        theta_texts[j] = theta_lines[idxs].mean(axis=0)

    # =========================
    # ARKUSZE
    # =========================

    # ---------- Arkusz 1: "top 15 słówtopik" ----------
    N_TOP_WORDS_WIDE = 15

    top_words_wide = {}
    for topic_id in valid_topic_ids:
        label_idx = topic_id_to_label[topic_id]
        # bierzemy pierwsze 15 słów z reprezentacji (a mamy top_n_words=20)
        words_weights = topics_dict[topic_id][:N_TOP_WORDS_WIDE]
        words = [w for (w, _) in words_weights]
        # gdyby jakimś cudem było mniej niż 15, dopadamy None
        if len(words) < N_TOP_WORDS_WIDE:
            words += [None] * (N_TOP_WORDS_WIDE - len(words))
        top_words_wide[f"Topic {label_idx}"] = words

    df_top15 = pd.DataFrame(top_words_wide)

    # ---------- Arkusz 2: "top 10 słów + beta" ----------
    N_TOP_WORDS_LONG = 10

    rows_long = []
    for topic_id in valid_topic_ids:
        label_idx = topic_id_to_label[topic_id]
        for word, weight in topics_dict[topic_id][:N_TOP_WORDS_LONG]:
            rows_long.append(
                {
                    "topic": label_idx,
                    "term": word,
                    "beta": float(weight),
                }
            )

    if rows_long:
        df_top10_beta = pd.DataFrame(rows_long)[["topic", "term", "beta"]]
    else:
        df_top10_beta = pd.DataFrame(columns=["topic", "term", "beta"])

    # ---------- Arkusz 3: "topikiteksty" ----------
    max_probs_text = theta_texts.max(axis=1)
    best_topic_idx_text = theta_texts.argmax(axis=1)

    top_topics_labels_text = []
    for i, max_p in enumerate(max_probs_text):
        if max_p > 0:
            label = topic_id_to_label[valid_topic_ids[best_topic_idx_text[i]]]
        else:
            label = None
        top_topics_labels_text.append(label)

    df_topiki_teksty = pd.DataFrame(
        {
            "dokument": doc_ids_unique,       # każdy tekst dokładnie raz
            "Top topik": top_topics_labels_text,
        }
    )

    # ---------- Arkusz 4: "theta" ----------
    theta_df = pd.DataFrame(
        theta_texts,
        columns=[topic_id_to_label[topic_id] for topic_id in valid_topic_ids],
    )
    theta_df = theta_df[sorted(theta_df.columns)]
    theta_df.insert(0, "dokument", doc_ids_unique)

    # ---------- Zapis do Excela ----------
    os.makedirs(os.path.dirname(OUTPUT_XLSX), exist_ok=True)

    with pd.ExcelWriter(OUTPUT_XLSX, engine="xlsxwriter") as writer:
        df_top15.to_excel(writer, sheet_name="top 15 słówtopik", index=False)
        df_top10_beta.to_excel(writer, sheet_name="top 10 słów + beta", index=False)
        df_topiki_teksty.to_excel(writer, sheet_name="topikiteksty", index=False)
        theta_df.to_excel(writer, sheet_name="theta", index=False)

    print(f"Zapisano rezultaty do: {OUTPUT_XLSX}")


if __name__ == "__main__":
    main()



#%%

# #%%
# #code by Patryk Hubar-Kołodziejczyk
# import pandas as pd

# import nltk
# nltk.download('punkt')
# nltk.download('punkt_tab')

# import os
# import glob

# from sentence_transformers import SentenceTransformer

# from umap import UMAP
# from hdbscan import HDBSCAN
# from sklearn.feature_extraction.text import CountVectorizer

# from bertopic import BERTopic
# from bertopic.representation import KeyBERTInspired, MaximalMarginalRelevance, OpenAI, PartOfSpeech
# from tqdm import tqdm

# import spacy
# nlp = spacy.load("pl_core_news_lg")
# import time


# #%%
# def load_stopwords(file_path: str, encoding: str = "utf-8") -> list[str]:
#     with open(file_path, "r", encoding=encoding) as file:
#         return [line.strip() for line in file if line.strip()]


# POLISH_STOPWORDS = load_stopwords(r"data\poloniści\stopwords-pl.txt")
# #%%
# polish_st = "sdadas/st-polish-paraphrase-from-distilroberta"
# embedding_model = SentenceTransformer(polish_st)
# # embeddings = embedding_model.encode(processed_texts, show_progress_bar=True)
# #%%
# umap_model = UMAP(
#     n_neighbors=15,
#     n_components=5,
#     min_dist=0.0,
#     metric='cosine',
#     random_state=42
# )


# hdbscan_model = HDBSCAN(
#     min_cluster_size=25,
#     metric='euclidean',
#     cluster_selection_method='eom',
#     prediction_data=True
# )


# vectorizer_model = CountVectorizer(
#     stop_words=POLISH_STOPWORDS,
#     min_df=2,
#     ngram_range=(1, 3)
# )


# # KeyBERT
# keybert_model = KeyBERTInspired()

# #polski dziad
# pos_model = PartOfSpeech("pl_core_news_lg")

# # MMR
# mmr_model = MaximalMarginalRelevance(diversity=0.3)

# representation_model = {
#     "KeyBERT": keybert_model,
#     "MMR": mmr_model,
#     "POS": pos_model
# }
# #%%
# start = time.time()

# folder_path = r"data\poloniści\pełne teksty/"
# txt_files = glob.glob(os.path.join(folder_path, "*.txt"))
# processed_texts = []
# processed_texts_ids = []

# for file_path in tqdm(txt_files[:100]):
#     with open(file_path, "r", encoding="utf-8") as file:
#         lines = file.readlines()
#         cleaned = [line.strip() for line in lines if line.strip()]
#         processed_texts.extend(cleaned)
#         text_id = file_path.split('\\')[-1].replace('.txt', '')
#         processed_texts_ids.append(text_id)

# print(f"Wczytano {len(processed_texts)} linii z {len(txt_files)} plików.")

# embeddings = embedding_model.encode(processed_texts, show_progress_bar=True)

# topic_model = BERTopic(

#   embedding_model=embedding_model,
#   umap_model=umap_model,
#   hdbscan_model=hdbscan_model,
#   vectorizer_model=vectorizer_model,
#   representation_model=representation_model,

#   # parametry
#   top_n_words=10,
#   verbose=True
# )

# topics, probs = topic_model.fit_transform(processed_texts, embeddings)

# topic_model.get_topic_info().to_csv("data/poloniści/topiki_100.csv", encoding='utf-8')
# end = time.time()

# print(f"Czas wykonania dla {len(processed_texts_ids)} tekstów: {end - start:.4f} sekundy.\nPrzetworzone teksty: {processed_texts_ids}")
#%%

# #%% testy patryka

# # -*- coding: utf-8 -*-
# """Topic modelling dla Czarka – wersja z datasetem i nazwami plików w wynikach."""

# # Commented out IPython magic to ensure Python compatibility.
# # Jeśli uruchamiasz w czystym Colabie, odkomentuj poniższe:
# # capture
# # !pip install bertopic
# # !pip install datasets
# # !pip install openai
# # !pip -q install -U transformers sentence-transformers torch
# # !python -m spacy download pl_core_news_lg

# import os
# import glob

# import nltk
# import pandas as pd

# from datasets import Dataset
# from sentence_transformers import SentenceTransformer
# from umap import UMAP
# from hdbscan import HDBSCAN
# from sklearn.feature_extraction.text import CountVectorizer

# from bertopic import BERTopic
# from bertopic.representation import KeyBERTInspired, MaximalMarginalRelevance, OpenAI, PartOfSpeech

# from tqdm import tqdm


# # ===== 1. NLTK – zasoby =====
# nltk.download("punkt")
# nltk.download("punkt_tab")


# # ===== 2. Funkcja do wczytywania stopwords =====
# def load_stopwords(file_path: str, encoding: str = "utf-8") -> list[str]:
#     """Wczytaj listę polskich stopwords z pliku tekstowego (jedno słowo na linię)."""
#     with open(file_path, "r", encoding=encoding) as f:
#         return [line.strip() for line in f if line.strip()]


# # Ścieżka do stopwords (jak w Twoim notebooku)
# POLISH_STOPWORDS = load_stopwords(r"data\poloniści\stopwords-pl.txt")


# # ===== 3. Wczytywanie plików .txt jako dataset (jeden plik = jeden dokument) =====
# # UZUPEŁNIJ ŚCIEŻKĘ:
# folder_path = r"data\poloniści\pełne teksty/"

# txt_files = glob.glob(os.path.join(folder_path, "*.txt"))

# texts = []
# filenames = []

# for file_path in tqdm(txt_files[:100]):
#     with open(file_path, "r", encoding="utf-8") as f:
#         lines = f.readlines()
#         cleaned = [line.strip() for line in lines if line.strip()]
#         # Wersja: JEDEN PLIK = JEDEN DOKUMENT
#         full_doc = " ".join(cleaned)
#         texts.append(full_doc)
#         filenames.append(os.path.basename(file_path))

# print(f"Wczytano {len(texts)} dokumentów z {len(txt_files)} plików.")

# # Jeśli wolisz: JEDNA LINIA = JEDEN DOKUMENT, użyj zamiast powyższej pętli:
# # texts = []
# # filenames = []
# # for file_path in txt_files:
# #     with open(file_path, "r", encoding="utf-8") as f:
# #         lines = f.readlines()
# #         cleaned = [line.strip() for line in lines if line.strip()]
# #         for line in cleaned:
# #             texts.append(line)
# #             filenames.append(os.path.basename(file_path))

# # Tworzymy dataset (tekst + nazwa pliku)
# dataset = Dataset.from_dict({
#     "text": texts[:100],
#     "filename": filenames
# })


# # ===== 4. Model embeddingowy =====
# polish_st = "sdadas/st-polish-paraphrase-from-distilroberta"
# embedding_model = SentenceTransformer(polish_st)

# embeddings = embedding_model.encode(dataset["text"], show_progress_bar=True)


# # ===== 5. Modele UMAP, HDBSCAN, wektoryzacja =====
# umap_model = UMAP(
#     n_neighbors=15,
#     n_components=5,
#     min_dist=0.0,
#     metric="cosine",
#     random_state=42
# )

# hdbscan_model = HDBSCAN(
#     min_cluster_size=25,
#     metric="euclidean",
#     cluster_selection_method="eom",
#     prediction_data=True
# )

# # hdbscan_model = HDBSCAN(
# #     min_cluster_size=5,
# #     min_samples=4,
# #     metric="euclidean",
# #     cluster_selection_method="eom",
# #     prediction_data=True
# # )

# vectorizer_model = CountVectorizer(
#     stop_words=POLISH_STOPWORDS,
#     min_df=1,
#     ngram_range=(1, 3)
# )


# # ===== 6. Modele reprezentacji (KeyBERT, POS, MMR) =====
# keybert_model = KeyBERTInspired()
# pos_model = PartOfSpeech("pl_core_news_lg")
# mmr_model = MaximalMarginalRelevance(diversity=0.3)

# representation_model = {
#     "KeyBERT": keybert_model,
#     "MMR": mmr_model,
#     "POS": pos_model,
#     # "OpenAI": OpenAI("gpt-4o")  # opcjonalnie, jeśli chcesz używać OpenAI do reprezentacji
# }


# # ===== 7. Inicjalizacja BERTopic =====
# topic_model = BERTopic(
#     embedding_model=embedding_model,
#     umap_model=umap_model,
#     hdbscan_model=hdbscan_model,
#     vectorizer_model=vectorizer_model,
#     representation_model=representation_model,
#     top_n_words=10,
#     verbose=True
# )

# # ===== 8. Trenowanie BERTopic =====
# topics, probs = topic_model.fit_transform(dataset["text"], embeddings)


# # ===== 9. Informacje o topikach i dokumentach =====
# # Informacje o samych topikach (globalnie)
# topic_info = topic_model.get_topic_info()
# topic_info.to_csv("data/poloniści/bert_topic_info.csv", index=False)

# # Informacje na poziomie dokumentów
# doc_info = topic_model.get_document_info(dataset["text"])
# doc_info = doc_info.drop(columns=['Document', 'Representative_Docs'])

# # Dodajemy nazwy plików do doc_info (kolejność jest ta sama co w dataset)
# doc_info["filename"] = dataset["filename"]

# # Zapis: każdy dokument + jego topik + prawdopodobieństwo + nazwa pliku
# doc_info.to_csv("data/poloniści/bert_topic_documents_with_filenames.csv", index=False)

# # ===== 10. Mapowanie: TOPIK -> LISTA PLIKÓW, W KTÓRYCH WYSTĘPUJE =====
# topic_to_files = (
#     doc_info
#     .groupby("Topic")["filename"]
#     .unique()
#     .reset_index()
#     .rename(columns={"filename": "files"})
# )

# topic_to_files.to_csv("data/poloniści/topics_to_files.csv", index=False)


# # ===== 11. Przykładowe filtrowanie (opcjonalne) =====
# # Przykład: wyświetl dokumenty z topiku 3
# example_topic_id = 3
# docs_in_topic_3 = doc_info[doc_info["Topic"] == example_topic_id][
#     ["filename", "Document", "Probability"]
# ]
# print(f"\nPrzykładowe dokumenty w topiku {example_topic_id}:")
# print(docs_in_topic_3.head())


# #%% testy CR

# import os
# import glob
# import time

# import pandas as pd
# from tqdm import tqdm

# from sentence_transformers import SentenceTransformer
# from umap import UMAP
# from hdbscan import HDBSCAN
# from sklearn.feature_extraction.text import CountVectorizer

# from bertopic import BERTopic
# from bertopic.representation import KeyBERTInspired, MaximalMarginalRelevance, PartOfSpeech


# def load_stopwords(file_path: str, encoding: str = "utf-8") -> list[str]:
#     with open(file_path, "r", encoding=encoding) as file:
#         return [line.strip() for line in file if line.strip()]


# # stopwordy
# POLISH_STOPWORDS = load_stopwords(r"data\poloniści\stopwords-pl.txt")

# # model embeddingów PL
# polish_st = "sdadas/st-polish-paraphrase-from-distilroberta"
# embedding_model = SentenceTransformer(polish_st)

# # UMAP
# umap_model = UMAP(
#     n_neighbors=15,
#     n_components=5,
#     min_dist=0.0,
#     metric='cosine',
#     random_state=42
# )

# # HDBSCAN
# hdbscan_model = HDBSCAN(
#     min_cluster_size=25,
#     metric='euclidean',
#     cluster_selection_method='eom',
#     prediction_data=True
# )

# # Vectorizer
# vectorizer_model = CountVectorizer(
#     stop_words=POLISH_STOPWORDS,
#     min_df=1,
#     ngram_range=(1, 3)
# )

# # Reprezentacje tematów
# keybert_model = KeyBERTInspired()
# pos_model = PartOfSpeech("pl_core_news_lg")
# mmr_model = MaximalMarginalRelevance(diversity=0.3)

# representation_model = {
#     "KeyBERT": keybert_model,
#     "MMR": mmr_model,
#     "POS": pos_model
# }

# # --- Wczytywanie tekstów ---
# start = time.time()

# folder_path = r"data\poloniści\pełne teksty"
# txt_files = glob.glob(os.path.join(folder_path, "*.txt"))

# processed_texts = []
# processed_texts_ids = []

# for file_path in tqdm(txt_files[:100]):
#     with open(file_path, "r", encoding="utf-8") as file:
#         lines = file.readlines()
#         cleaned = [line.strip() for line in lines if line.strip()]
#         full_text = " ".join(cleaned)   # cały artykuł jako jeden dokument
#         processed_texts.append(full_text)
#         text_id = os.path.basename(file_path).replace('.txt', '')
#         processed_texts_ids.append(text_id)

# print(f"Wczytano {len(processed_texts)} tekstów z {len(txt_files)} plików.")

# # --- Topic modeling ---
# topic_model = BERTopic(
#     embedding_model=embedding_model,
#     umap_model=umap_model,
#     hdbscan_model=hdbscan_model,
#     vectorizer_model=vectorizer_model,
#     representation_model=representation_model,
#     top_n_words=10,
#     verbose=True
# )

# topics, probs = topic_model.fit_transform(processed_texts)

# # zapis info o tematach
# topic_info = topic_model.get_topic_info()
# topic_info.to_csv("data/poloniści/topiki_100_po-zmianach.csv", encoding='utf-8', index=False)

# end = time.time()
# print(f"Czas wykonania dla {len(processed_texts)} tekstów: {end - start:.2f} sekundy.")
# print(f"ID tekstów: {processed_texts_ids}")



































