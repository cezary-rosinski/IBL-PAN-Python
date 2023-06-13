import glob
from tqdm import tqdm
import sys
sys.path.insert(1, 'C:/Users/Cezary/Documents/Global-trajectories-of-Czech-Literature')
from marc_functions import read_mrk, mrk_to_df
from my_functions import marc_parser_dict_for_field
import json

#%%

path = r"F:\Cezary\Documents\IBL\BN\bn_all\2023-01-23/"
files = [f for f in glob.glob(path + '*.mrk', recursive=True)]

# rok publikacji 1990-2020
# deskryptor "Teoria literatury"
# tekst tłumaczony na język polski $apol + $h

#lista deskryptorów z https://dbn.bn.org.pl/descriptor-details/a0000003588344 + 'Teoria literatury'
descriptors_bn = ["Teoria literatury", "Close reading", "Ekonomia literatury", "Geopoetyka", "Intentio operis (literaturoznawstwo)", "Intermedialność", "Intersemiotyczność", "Intertekstualność", "Karnawalizacja", "Krytyka archetypowa", "Krytyka feministyczna", "Krytyka kerygmatyczna", "Krytyka postkolonialna", "Krytyka psychoanalityczna", "Krytyka somatyczna", "Krytyka tematyczna", "New Criticism", "Narratologia", "Nowy historyzm", "Poetyka", "Poetyka przestrzeni", "Poststrukturalizm", "Proces historycznoliteracki", "Rosyjska szkoła formalna", "Socjokrytyka", "Strukturalizm", "Animal studies", "Antropologia literatury", "Chronotop", "Chwyt (literaturoznawstwo)", "Dekonstrukcja (filozofia)", "Fenomenologia literatury", "Filozofia literatury", "Fokalizacja", "Groteska", "Hermeneutyka", "Heterotopie", "Język poetycki", "Katharsis", "Kāvyapāka", "Komunikacja literacka", "Konkretyzacja dzieła literackiego", "Krytyka literacka", "Lęk przed wpływem (teoria)", "Literackość", "Literatura mniejsza", "Miejsca autobiograficzne", "Mimesis", "Mise en abyme", "Nie-miejsca", "Odbiór dzieła literackiego", "Paleontologia semantyczna", "Posthumanizm", "Pragnienie mimetyczne", "Quasi-sądy", "Queer theory", "Retoryczność", "Suspens", "Symultanizm (literatura)", "Teologia literatury", "Teoria afektów", "Widmontologia", "Wzniosłość"]

result_years = {}
result_languages = {}
    
for file in tqdm(files):
    # file = files[0]
    file = read_mrk(file)
    for dictionary in file:
        
        year = int(dictionary.get('008')[0][7:11]) if '008' in dictionary and dictionary.get('008')[0][7:11].isnumeric() else None
        # descriptors = any([e for e in dictionary.get('650') if 'Teoria literatury' in e]) if '650' in dictionary else False
        descriptors = any([e for e in dictionary.get('650') if any(el in e for el in descriptors_bn)]) if '650' in dictionary else False
        is_translation = all(e in dictionary.get('041')[0] for e in ['$apol', '$h']) if '041' in dictionary else False
        original_language = [e.get('$h') for e in marc_parser_dict_for_field(dictionary.get('041')[0], '\\$') if '$h' in e and e.get('$h') != 'pol'] if '041' in dictionary and '$h' in dictionary.get('041')[0] else None
        if year in range(1990,2023) and descriptors == True and is_translation == True and original_language:
            result_years.setdefault(year, set()).add(dictionary.get('001')[0])
            if len(original_language) == 1:
                result_languages.setdefault(original_language[0], set()).add(dictionary.get('001')[0])
            else: result_languages.setdefault('multilingual', set()).add(dictionary.get('001')[0])
            
with open('opus_result_years.json', 'w') as outfile:
    json.dump({k:list(v) for k,v in result_years.items()}, outfile, ensure_ascii=True)
   
with open('opus_result_languages.json', 'w') as outfile:
    json.dump({k:list(v) for k,v in result_languages.items()}, outfile, ensure_ascii=True) 
   
    
   
    
   
    
   
    
   
    
   
    