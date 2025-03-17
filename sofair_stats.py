import pandas as pd
import glob
from tqdm import tqdm
import xml.etree.ElementTree as ET 
import requests
from concurrent.futures import ThreadPoolExecutor
import pickle
from my_functions import gsheet_to_df

#%% def
# sofair_metadata = []
# # def harvest_metadata(doi):
# for doi in tqdm(sofair_dois):
#     # doi = list(sofair_dois)[0]
#     url = 'https://api.crossref.org/works/' + doi
#     metadata = requests.get(url).json()
#     sofair_metadata.append({doi: metadata})

#%% dependencies

sofair_folder = r"D:\IBL\Documents\IBL-PAN-Python\data\SoFAIR/"

domain_dict = {
    'Engineering': 'SoFAIR_AD_Engineering_papers',
    'Comp_science': 'SoFAIR_AD_Comp_Sci_papers',
    'Energy_Sciences': 'SoFAIR_AD_Energy_Sciences_papers',
    'Mathematics': 'SoFAIR_AD_Mathematics_papers',
    'Chemistry': 'SoFAIR_AD_Chemistry_papers',
    'Neuroscience': 'SoFAIR_AD_Neuroscience_papers',
    'Medicine': 'SoFAIR_AD_Medicine_papers',
    'Biochemistry': 'SoFAIR_AD_BioChem_papers',
    'Arts and Humanities': 'SoFAIR_Arts_and_Humanities_papers',
    'Sociology_and_Political_Science': 'SoFAIR_Sociology_and_Political_Science_papers',
    'Language_and_Linguistics': 'SoFAIR_Language_and_Linguistics_papers',
    'Cultural Studies': 'SoFAIR_Cultural_Studies_papers',
    'Business_Management': 'SoFAIR_AD_BMA_papers',
    'Physics': 'SoFAIR_AD_Physics_papers',
    'Materials_Sciences': 'SoFAIR_AD_Materials_Science_papers',
    'Earth_Planet_Sciences': 'SoFAIR_AD_Earth_Planet_Sciences_papers',
    'Environmental_Science': 'SoFAIR_AD_Environmental_Science_papers',
    'Digital Humanities': 'SoFAIR_AD_Digital_Humanties_papers'
    }

sofair_articles_1 = pd.read_excel(f"{sofair_folder}SoFAIR_Annotation_Dataset_Paper_Data_first.xlsx")
sofair_articles_2 = pd.read_excel(f"{sofair_folder}SoFAIR_Annotation_Dataset_Paper_Data_Extension.xlsx")
sofair_articles_3 = pd.read_excel(f"{sofair_folder}SoFAIR_Humanities_paper_list.xlsx")
sofair_articles_4 = pd.read_excel(f"{sofair_folder}SoFAIR_Digital_Humanities_paper_list.xlsx")

sofair_articles_1['file_id'] = sofair_articles_1[['Domain', 'ID']].apply(lambda x: domain_dict.get(x['Domain'], x['Domain']) + '_' + str(x['ID']), axis=1)
sofair_articles_2['file_id'] = sofair_articles_2[['Domain', 'ID']].apply(lambda x: domain_dict.get(x['Domain'], x['Domain']) + '_' + str(x['ID']), axis=1)
sofair_articles_3['file_id'] = sofair_articles_3[['Domain', 'ID']].apply(lambda x: domain_dict.get(x['Domain'], x['Domain']) + '_' + str(x['ID']), axis=1)
sofair_articles_4['file_id'] = sofair_articles_4[['Domain', 'ID']].apply(lambda x: domain_dict.get(x['Domain'], x['Domain']) + '_' + str(x['ID']), axis=1)

sofair_ids = dict(zip(sofair_articles_1['file_id'].to_list() + sofair_articles_2['file_id'].to_list() + sofair_articles_3['file_id'].to_list() + sofair_articles_4['file_id'].to_list(), sofair_articles_1['DOI'].to_list() + sofair_articles_2['DOI'].to_list() + sofair_articles_3['DOI'].to_list() + sofair_articles_4['DOI'].to_list()))

sofair_dois = set(sofair_ids.values())

df_sofair_annotations_info = gsheet_to_df('1CHJpIGtI5kUsuW0gQwEn5QM2yJEF24-pyRVfON0RRkM', 'all documents')
df_sofair_annotations_info['file_id'] = df_sofair_annotations_info[['Subcorpus', 'File']].apply(lambda x: x['Subcorpus'] + '_' + x['File'].split('\\')[-1].split('.')[0], axis=1)

df_sofair_annotations_info['DOI'] = df_sofair_annotations_info['file_id'].apply(lambda x: sofair_ids.get(x))

sofair_files_id = set(df_sofair_annotations_info['file_id'].to_list())
    
#%% metadata harvesting

# sofair_metadata = []
# with ThreadPoolExecutor() as excecutor:
#     list(tqdm(excecutor.map(harvest_metadata, sofair_dois),total=len(sofair_dois)))   
    
# with open('data/sofair_metadata.p', 'wb') as fp:
#     pickle.dump(sofair_metadata, fp, protocol=pickle.HIGHEST_PROTOCOL)
    
# with open('data/sofair_metadata.p', 'rb') as fp:
#     sofair_metadata = pickle.load(fp)
    
# #supplement
# sofair_dois_supplement = [e for e in sofair_dois if e not in [list(e.keys())[0] for e in sofair_metadata]]

# sofair_metadata2 = []
# for doi in tqdm(sofair_dois_supplement):
#     # doi = list(sofair_dois)[0]
#     url = 'https://api.crossref.org/works/' + doi
#     metadata = requests.get(url).json()
#     sofair_metadata2.append({doi: metadata})
    
# sofair_metadata.extend(sofair_metadata2)

# with open('data/sofair_metadata.p', 'wb') as fp:
#     pickle.dump(sofair_metadata, fp, protocol=pickle.HIGHEST_PROTOCOL)

#%% metadata processing

with open(f'{sofair_folder}sofair_metadata.p', 'rb') as fp:
    sofair_metadata = pickle.load(fp)

sofair_articles_metadata = []

for e in tqdm(sofair_metadata):
    for k,v in e.items():
        # k = '10.3389/fnhum.2023.1133367'
        # v = sofair_metadata[0].get(k)
        iter_dict = {
            'DOI': k,
            'no_of_authors': len(v.get('message').get('author')) if 'author' in v.get('message') else 0,
            'year_of_publication': v.get('message').get('published').get('date-parts')[0][0] if 'published' in v.get('message') else v.get('message').get('created').get('date-parts')[0][0],
            'no_of_references': v.get('message').get('reference-count'),
            'is_referenced_by': v.get('message').get('is-referenced-by-count')}
        sofair_articles_metadata.append(iter_dict)

df_sofair_articles_metadata = pd.DataFrame(sofair_articles_metadata)

df_sofair_articles_info = pd.merge(df_sofair_annotations_info, df_sofair_articles_metadata, how='left', on='DOI')

###duplikaty!!!
# df_sofair_articles_info['article_id'] = df_sofair_articles_info['file_id'].apply(lambda x: x.split('_')[-1])
# duplicates = df_sofair_articles_info[df_sofair_articles_info.duplicated(['article_id'], keep=False)]

                         

#%% annotation statistics

path = r'D:\IBL\Documents\Dataset\documents\tei-annotated/'
folders = glob.glob(path + '*' + '/', recursive=True)

namespaces = {'{http://www.w3.org/XML/1998/namespace}id': 'id',
              '{http://www.tei-c.org/ns/1.0}rs': 'rs',
              'type': 'type',
              'corresp': 'corresp',
              'subtype': 'subtype'}

software_mentions = []
for folder in tqdm(folders):
    # folder = folders[0]
    files = [f for f in glob.glob(folder + '*.xml', recursive=True)]
    for file in files:
        # file = files[0]
        file_id = file.split('\\')[-2] + '_' + file.split('\\')[-1].split('.')[0]
        tree = ET.parse(file)
        root = tree.getroot()
        
        software_mentions_iter = [[{namespaces.get(k):v for k,v in elem.attrib.items()}, {'text': elem.text}] for elem in root.iter() if elem.tag == '{http://www.tei-c.org/ns/1.0}rs']
        for i, e in enumerate(software_mentions_iter):
            software_mentions_iter[i] = e[0] | e[-1]
            software_mentions_iter[i].update({'file_id': file_id})
        
        software_mentions.extend(software_mentions_iter)
        
        
df_annotated = pd.DataFrame(software_mentions)        
        
#%% save files

df_annotated.to_excel(f'{sofair_folder}sofair_annotation_statistics_detailed.xlsx', index=False)
df_sofair_articles_info.to_excel(f'{sofair_folder}sofair_articles_info.xlsx', index=False)
        
#%% annotation statistics

path = r'D:\IBL\Documents\Dataset\documents\tei-pre-annotated/'
folders = glob.glob(path + '*' + '/', recursive=True)

namespaces = {'{http://www.w3.org/XML/1998/namespace}id': 'id',
              '{http://www.tei-c.org/ns/1.0}rs': 'rs',
              'type': 'type',
              'corresp': 'corresp',
              'subtype': 'subtype'}

pre_software_mentions = []
for folder in tqdm(folders):
    # folder = folders[0]
    files = [f for f in glob.glob(folder + '*.xml', recursive=True)]
    for file in files:
        # file = files[0]
        file_id = file.split('\\')[-2] + '_' + file.split('\\')[-1].split('.')[0]
        tree = ET.parse(file)
        root = tree.getroot()
        
        software_mentions_iter = [[{namespaces.get(k):v for k,v in elem.attrib.items()}, {'text': elem.text}] for elem in root.iter() if elem.tag == '{http://www.tei-c.org/ns/1.0}rs']
        for i, e in enumerate(software_mentions_iter):
            software_mentions_iter[i] = e[0] | e[-1]
            software_mentions_iter[i].update({'file_id': file_id})
        
        pre_software_mentions.extend(software_mentions_iter)
        
        
df_pre_annotated = pd.DataFrame(pre_software_mentions)
df_pre_annotated = df_pre_annotated.loc[df_pre_annotated['file_id'].isin(set(df_pre_annotated['file_id'].to_list()))]

df_pre_annotated.groupby('type').size()

#%% analiza

#które narzędzia są najpopularniejsze


#które narzędzie obecne jest w wielu dyscyplinach


#czy liczba autorów lub odwołań różni się w zalezności od liczby wzmianek o oprogramowaniu?\

    
#Jak liczba wzmianek o oprogramowaniu zmieniała się w kolejnych latach


#Czy pewne narzędzia są związane z określonymi obszarami badań


#%% supplement of articles metadata on google drive

sofair_articles = pd.concat([sofair_articles_1, sofair_articles_2, sofair_articles_3, sofair_articles_4])
        
sofair_articles = sofair_articles[['file_id', 'Title']]

articles_metadata = gsheet_to_df('1AuOxW23zYzk0LT8zF7H919RXi0ZULfJdUDE_TabiITg', 'Sheet1')
articles_metadata = pd.merge(articles_metadata, sofair_articles, how='left', on='file_id')

# articles_metadata.to_excel('data/sofair_articles_info.xlsx', index=False)

dois = articles_metadata['DOI'].to_list()

sofair_metadata = []
for doi in tqdm(dois):
    # doi = list(sofair_dois)[0]
    url = 'https://api.crossref.org/works/' + doi
    metadata = requests.get(url).json()
    sofair_metadata.append({doi: metadata})

sofair_articles_metadata = []

for e in tqdm(sofair_metadata):
    for k,v in e.items():
        # k = '10.3389/fnhum.2023.1133367'
        # v = sofair_metadata[0].get(k)
        iter_dict = {
            'DOI': k,
            'Title': v.get('message').get('title')[0]}
        sofair_articles_metadata.append(iter_dict)
    
df_metadata = pd.DataFrame(sofair_articles_metadata)
df_metadata = df_metadata.merge(df_sofair_articles_info, on='DOI', how='left')
df_metadata.to_excel(f'{sofair_folder}sofair_articles_info.xlsx', index=False)

#%% połączenie zasobów

# Filtrowanie tylko wzmianek dotyczących oprogramowania
software_mentions_df = df_annotated[df_annotated["type"] == "software"]

# Łączenie danych na podstawie file_id
merged_df = df_metadata.merge(df_annotated, on="file_id", how="left")

# Sprawdzenie struktury połączonych danych
merged_df.head()

merged_df.to_excel(f'{sofair_folder}sofair_full_data.xlsx', index=False)

#%% visualization -- software mentions in discipline and time

import matplotlib.pyplot as plt
import seaborn as sns

# Grupowanie danych: liczba wzmianek o oprogramowaniu w podziale na rok i dyscyplinę
software_trends = merged_df.groupby(["year_of_publication", "Subcorpus"]).size().reset_index(name="mention_count")

# Wykres
plt.figure(figsize=(12, 6))
sns.lineplot(data=software_trends, x="year_of_publication", y="mention_count", hue="Subcorpus", marker="o")

# Stylizacja wykresu
plt.title("Software mentions in discipline and time", fontsize=14)
plt.xlabel("Year", fontsize=12)
plt.ylabel("Software mentions", fontsize=12)
plt.xticks(rotation=45)
plt.legend(title="Discipline")
plt.grid(True, linestyle="--", alpha=0.6)

# Wyświetlenie wykresu
plt.show()

#%%

import pandas as pd
import re
import matplotlib.pyplot as plt

# Load data
df = merged_df.copy()

# Clean discipline names
def clean_subcorpus_name(name):
    name = re.sub(r'^SoFAIR_', '', name)       # remove the "SoFAIR_" prefix
    name = re.sub(r'_papers$', '', name)       # remove the "_papers" suffix
    name = re.sub(r'^AD_', '', name)           # remove any leading "AD_"
    return name

df['discipline_clean'] = df['Subcorpus'].apply(clean_subcorpus_name)

# Filter only software-type mentions
df_software = df[df['type'] == 'software'].copy()

# Group by discipline and year
df_grouped = df_software.groupby(['discipline_clean', 'year_of_publication']).size()
df_grouped = df_grouped.reset_index(name='software_mentions')

# Pivot table: rows = year, columns = discipline, values = software mentions
df_pivot = df_grouped.pivot(index='year_of_publication',
                            columns='discipline_clean',
                            values='software_mentions').fillna(0)

# Plot: line chart
plt.figure(figsize=(10, 6))
for discipline in df_pivot.columns:
    plt.plot(df_pivot.index, df_pivot[discipline], marker='o', label=discipline)

plt.xlabel('Year of Publication')
plt.ylabel('Number of Software Mentions')
plt.title('Software Mentions by Discipline Over Time')
plt.legend(title='Discipline', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()
plt.show()

#%% 

import pandas as pd
import re
import matplotlib.pyplot as plt

# --- 1. Load and preprocess data ---

df = merged_df.copy()

def clean_subcorpus_name(name):
    name = re.sub(r'^SoFAIR_', '', name)
    name = re.sub(r'_papers$', '', name)
    name = re.sub(r'^AD_', '', name)
    return name

df['discipline_clean'] = df['Subcorpus'].apply(clean_subcorpus_name)

# Filter to software-type mentions
df_software = df[df['type'] == 'software'].copy()

# Group by discipline and year
grouped = df_software.groupby(
    ['discipline_clean','year_of_publication']
).size().reset_index(name='software_mentions')

# Pivot: rows = year, columns = discipline
df_pivot = grouped.pivot(
    index='year_of_publication',
    columns='discipline_clean',
    values='software_mentions'
).fillna(0)

# --- 2. Pick top disciplines to plot (by total software mentions) ---

# Calculate total mentions per discipline across all years
totals = df_pivot.sum().sort_values(ascending=False)

# Here we select the top N (e.g., top 6). Adjust as needed.
top_disciplines = totals.head(6).index

# Filter the pivot table to include only these top disciplines
df_top = df_pivot[top_disciplines]

# Optional: rename columns to shorter labels for clarity
rename_dict = {
    'Arts_and_Humanities': 'Arts/Hum',
    'Sociology_and_Political_Science': 'Soc/Pol Sci',
    'Digital_Humanties': 'Digital Hum',
    'Environmental_Science': 'Env Sci',
    'Language_and_Linguistics': 'Lang/Ling',
    'Materials_Science': 'Mat Sci',
    'Earth_Planet_Sciences': 'Earth/Planet',
    'Energy_Sciences': 'Energy Sci',
    'Comp_Sci': 'Comp Sci',
    'BioChem': 'BioChem',
    # etc. for any that appear in your top 6
}

df_top = df_top.rename(columns=rename_dict)

# --- 3. Plot the data as a cleaner line chart ---

plt.figure(figsize=(12, 6))
for discipline in df_top.columns:
    plt.plot(df_top.index, df_top[discipline], marker='o', label=discipline)

plt.title("Top Software-Mentioning Disciplines Over Time")
plt.xlabel("Year of Publication")
plt.ylabel("Number of Software Mentions")
plt.grid(True)
plt.legend(title="Discipline", bbox_to_anchor=(1.05, 1), loc="upper left")
plt.tight_layout()
plt.show()

        
#%%  most popular software (per discipline)



# 2. Clean discipline names (optional, helps readability)
def clean_subcorpus_name(name):
    name = re.sub(r'^SoFAIR_', '', name)
    name = re.sub(r'_papers$', '', name)
    name = re.sub(r'^AD_', '', name)
    return name

df['discipline_clean'] = df['Subcorpus'].apply(clean_subcorpus_name)

# 3. Keep only "software" mentions
df_software = df[df['type'] == 'software'].copy()

# 4. Group by software name, then count how often each appears across the entire dataset
software_count = (
    df_software
    .groupby('text')
    .size()
    .reset_index(name='mention_count')
    .sort_values('mention_count', ascending=False)
)

# 5. Pick the top N most frequently mentioned software (e.g., top 10)
top_n = 10
top_software_names = software_count.head(top_n)['text'].tolist()

# 6. Filter original data to keep only these top N software
df_top_software = df_software[df_software['text'].isin(top_software_names)].copy()

# 7. For each software and discipline, count mentions
grouped_disciplines = (
    df_top_software
    .groupby(['text', 'discipline_clean'])
    .size()
    .reset_index(name='mention_count')
)

# 8. Pivot so that rows are software names, columns are disciplines, values are mention counts
df_pivot = grouped_disciplines.pivot(
    index='text', 
    columns='discipline_clean',
    values='mention_count'
).fillna(0)

# 9. Plot a stacked bar chart
plt.figure(figsize=(10,6))
df_pivot.plot(kind='bar', stacked=True, figsize=(10,6))

plt.title(f"Top {top_n} Most Frequently Mentioned Software by Discipline")
plt.xlabel("Software")
plt.ylabel("Number of Mentions")
plt.xticks(rotation=45, ha='right')
plt.legend(title="Discipline", bbox_to_anchor=(1.05, 1), loc="upper left")
plt.tight_layout()
plt.show()






















