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
    'Earth_Planet_Sciences': 'SoFAIR_AD_Earth_Planet_Sciences_papers'}

sofair_articles_1 = pd.read_excel(r"C:\Users\Cezary\Downloads\SoFAIR_Annotation_Dataset_Paper_Data_first.xlsx")
sofair_articles_2 = pd.read_excel(r"C:\Users\Cezary\Downloads\SoFAIR_Annotation_Dataset_Paper_Data_Extension.xlsx")
sofair_articles_3 = pd.read_excel(r"C:\Users\Cezary\Downloads\SoFAIR_Humanities_paper_list.xlsx")

sofair_articles_1['file_id'] = sofair_articles_1[['Domain', 'ID']].apply(lambda x: domain_dict.get(x['Domain'], x['Domain']) + '_' + str(x['ID']), axis=1)
sofair_articles_2['file_id'] = sofair_articles_2[['Domain', 'ID']].apply(lambda x: domain_dict.get(x['Domain'], x['Domain']) + '_' + str(x['ID']), axis=1)
sofair_articles_3['file_id'] = sofair_articles_3[['Domain', 'ID']].apply(lambda x: domain_dict.get(x['Domain'], x['Domain']) + '_' + str(x['ID']), axis=1)

sofair_ids = dict(zip(sofair_articles_1['file_id'].to_list() + sofair_articles_2['file_id'].to_list() + sofair_articles_3['file_id'].to_list(), sofair_articles_1['DOI'].to_list() + sofair_articles_2['DOI'].to_list() + sofair_articles_3['DOI'].to_list()))

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
    
with open('data/sofair_metadata.p', 'rb') as fp:
    sofair_metadata = pickle.load(fp)

#%% metadata processing

with open('data/sofair_metadata.p', 'rb') as fp:
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
                         

#%% annotation statistics

path = r'C:\Users\Cezary\Documents\Dataset\documents\tei-annotated/'
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

df_annotated.to_excel('data/sofair_annotation_statistics_detailed.xlsx', index=False)
df_sofair_articles_info.to_excel('data/sofair_articles_info.xlsx', index=False)
        
        
#%% analiza

#które narzędzia są najpopularniejsze


#które narzędzie obecne jest w wielu dyscyplinach


#czy liczba autorów lub odwołań różni się w zalezności od liczby wzmianek o oprogramowaniu?\

    
#Jak liczba wzmianek o oprogramowaniu zmieniała się w kolejnych latach


#Czy pewne narzędzia są związane z określonymi obszarami badań


#%%     
        
        
        
        
        
        
        
      





