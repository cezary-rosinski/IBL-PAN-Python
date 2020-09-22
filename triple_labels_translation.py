# thanks to Nikodem Wołczuk
import pandas as pd
from googletrans import Translator
import requests
from bs4 import BeautifulSoup
import regex as re
from json.decoder import JSONDecodeError

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

translation_table.to_excel('triple_labels_translation.xlsx', index=False)

translation_table = pd.read_excel('triple_labels_translation.xlsx')

def other_translations(x):
  print(str(ind) + ':    ' + str(x.name))
  other_trans = translator.translate(x['en'], src='en', dest=language).extra_data['possible-translations']
  try:
    for i in other_trans:
      sub_list = i[2]
    other_trans_list = []
    for sublist in sub_list:
        for i, item in enumerate(sublist):
          if i == 0:
            other_trans_list.append(item)
    try:
      other_trans_list = other_trans_list[1:]
    except IndexError:
      other_trans_list = []
    val = '❦'.join(other_trans_list)
  except TypeError:
    val = ''
  return val

# googletrans

translator = Translator()

languages_list = ['el', 'fr', 'nl', 'pl', 'de', 'it', 'pt', 'es', 'hr']
       
for ind, language in enumerate(languages_list):
    print(f"{ind}/{len(languages_list)}")
    translation_table[language] = translation_table['en'].apply(lambda x: translator.translate(x, src='en', dest=language).text)
    translation_table[f"{language}_other"] = translation_table.apply(lambda x: other_translations(x), axis=1)

translation_table.to_excel('triple_labels_translation_all_languages.xlsx', index=False)

print('Done')





