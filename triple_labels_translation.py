# thanks to Nikodem Wołczuk and Patryk Hubar
import pandas as pd
from googletrans import Translator
import requests
from bs4 import BeautifulSoup
import regex as re
import time
from ast import literal_eval

lcsh_ssh = pd.read_csv('C:/Users/Cezary/Downloads/SSH-LCSH.csv', sep=';', index_col=False).sort_values('en').reset_index(drop=True)

urls = lcsh_ssh['LCSH_URI'].tolist()
en_label = []

for i, url in enumerate(urls):
    print(f"{i}/{len(urls)}")
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    try:
        text = soup.find('span', property='madsrdf:authoritativeLabel skos:prefLabel').get_text()
        en_label.append(text)
    except AttributeError:
        text = soup.find('a', property='madsrdf:authoritativeLabel skos:prefLabel').get_text()
        en_label.append(text)
        
translation_table = lcsh_ssh.copy()[['SEMANTICS_URI', 'LCSH_URI']]
translation_table['en'] = en_label

print('Harvesting done.')

translation_table.to_excel('triple_labels_translation.xlsx', index=False)

translation_table = pd.read_excel('triple_labels_translation.xlsx')

def main_translation(x):
  print(f"main - {ind} - {x.name}/{len(translation_table)}")
  while True:
    try:
      main_trans = translator.translate(x['en'], src='en', dest=language).text
    except TypeError:
      time.sleep(1)
      continue
    break
  return main_trans

def other_translations(x):
  print(f"other - {ind} - {x.name}/{len(translation_table)}")
  while True:
    try:
      other_trans = translator.translate(x['en'], src='en', dest=language).extra_data['possible-translations'][0][2]
      other_trans = [sublist[0] for sublist in other_trans]
    except TypeError:
      time.sleep(1)
      continue
    except IndexError:
      other_trans = []
    break
  return other_trans

# googletrans

translator = Translator()

languages_list = ['el', 'fr', 'nl', 'pl', 'de', 'it', 'pt', 'es', 'hr']
       
for ind, language in enumerate(languages_list):
    print(f"{ind}/{len(languages_list)}")
    translation_table[language] = translation_table.apply(lambda x: main_translation(x), axis=1)
    translation_table[f"{language}_other"] = translation_table.apply(lambda x: other_translations(x), axis=1)

translation_table.to_excel('triple_labels_translation_all_languages.xlsx', index=False)

print('Translation done.')

final_df = pd.read_excel('triple_labels_translation_all_languages.xlsx')

def proper_main_and_other(x):
    print(f"{language}: {x.name}")
    if x[language] == x['en']:
        main = x[lang_other][0]
        try:
            other = '❦'.join(x[lang_other][1:])
        except IndexError:
            other = ''
    elif x[language] == x[lang_other][0]:
        main = x[language]
        try:
            other = '❦'.join(x[lang_other][1:])
        except IndexError:
            other = ''
    else:
        main = x[language]
        other = '❦'.join(x[lang_other])
    return main, other

for language in languages_list:
    lang_other = f"{language}_other"
    final_df[lang_other] = final_df[lang_other].apply(lambda x: literal_eval(x))
    final_df[language], final_df[lang_other] = zip(*final_df.apply(lambda x: proper_main_and_other(x), axis=1))
    
final_df.to_excel('triple_labels_translation_final.xlsx', index=False)

print('Correction done.')
    


