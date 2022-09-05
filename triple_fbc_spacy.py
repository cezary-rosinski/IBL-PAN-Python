from my_functions import simplify_string
from tqdm import tqdm
import json
import matplotlib.pyplot as plt
from wordcloud import WordCloud
import pandas as pd
from collections import Counter
import spacy


df = pd.read_csv(r"C:\Users\Cezary\Downloads\data_fbc_pol_with_lemmas.csv")
lemmas = df['LEMMAS'].to_list()

test = lemmas[0]

nlp = spacy.load('pl_core_news_sm')

def get_ents(row):
    entities = []
    doc = nlp(row)
    for ent in doc.ents:
        entities.append(f"{ent.text}, {ent.label_}")
    return entities

def get_lemmas(row):
    lemmas = []
    doc = nlp(row)
    for token in doc:
        lemmas.append(token.lemma_)
    lemmas_string = " ".join(lemmas)
    return lemmas_string

counter = Counter(lemmas).most_common(10000) 
lemmas_ents = [get_ents(e[0].strip()) for e in tqdm(counter) if not isinstance(e[0], float)]

lemmas_ok = [counter[i] for i, e in enumerate(lemmas_ents) if len(e) == 0]
test = [e for e in lemmas_ents if e]

   
    
word_ordered_dict = {}
for item in lemmas_ok:
    if not isinstance(item[0], float):
        word_ordered_dict[item[0]] = item[1]

wordcloud = WordCloud(width=1000, height=1000, random_state=21, colormap='Pastel1', max_font_size=750, background_color='black').generate_from_frequencies(word_ordered_dict)
plt.figure(figsize=(12, 12))
plt.imshow(wordcloud, interpolation="bilinear")
plt.axis('off')
plt.show()
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    