from spacy import displacy
import spacy
from spacy.lang.de.examples import sentences
from glob import glob
from tqdm import tqdm
import pandas as pd

#%%
nlp = spacy.load("de_core_news_sm")

path = r"D:\IBL\Gortych\NER\txt/"
files = [f for f in glob(f"{path}*", recursive=True)]

errors = []
results = []
for file in tqdm(files):
    # file = files[1]
    text_id = file.split('\\')[-1].replace('.txt', '')
    
    with open(file, 'rt', encoding='utf-8') as fh:
        lines = ' '.join([e.strip() for e in fh.readlines() if e.strip()])
        
    if len(lines) < 300:
        errors.append(text_id)
    else:
        doc = nlp(lines)
        for ent in doc.ents:
            temp_dict = {'text_id': text_id,
                         'entity': ent.text,
                         'entity_label': ent.label_}
            results.append(temp_dict)
            
df = pd.DataFrame(results)

df_stats = df.groupby(['entity', 'entity_label']).size().reset_index(name='count').sort_values('count', ascending=False)

df.to_excel('data/Gortych_ner_full.xlsx', index=False)
df_stats.to_excel('data/Gortych_ner_stats.xlsx', index=False)

nlp.get_pipe("ner").labels
