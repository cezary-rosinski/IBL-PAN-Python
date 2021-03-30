import pandas as pd
import requests
import xml.etree.ElementTree as et
from my_functions import xml_to_mrk
import regex as re
from tqdm import tqdm
import io
import datetime
import json

#%% date
now = datetime.datetime.now()
year = now.year
month = '{:02d}'.format(now.month)
day = '{:02d}'.format(now.day)

#%% Swedish harvesting (Swedish translations)
encoding = 'UTF-8'
final_list = []
url = 'http://libris.kb.se/xsearch?query=(SPR:swe)&format_level=full&format=marcxml&start=1&n=200'
response = requests.get(url)
with open('test.xml', 'wb') as file:
    file.write(response.content)
tree = et.parse('test.xml')
root = tree.getroot()
number_of_records = int(root.attrib['records'])
total_records_range = range(1, number_of_records + 1, 200)

for page in tqdm(total_records_range):
    
    if page == 1:
        xml_to_mrk('test.xml', 'test.mrk')
        marc_list = io.open('test.mrk', 'rt', encoding = encoding).read().splitlines()

        mrk_list = []
        for row in marc_list:
            if row.startswith('=LDR'):
                mrk_list.append([row])
            else:
                if row:
                    mrk_list[-1].append(row)
                    
        for lista in mrk_list:
            slownik = {}
            for el in lista:
                if el[1:4] in slownik:
                    slownik[el[1:4]] += f"❦{el[6:]}"
                else:
                    slownik[el[1:4]] = el[6:]
            final_list.append(slownik)
        
    else:
        link = f'http://libris.kb.se/xsearch?query=(SPR:swe)&format_level=full&format=marcxml&start={page}&n=200'
        response = requests.get(link)
        with open('test.xml', 'wb') as file:
            file.write(response.content)
        xml_to_mrk('test.xml', 'test.mrk')
        marc_list = io.open('test.mrk', 'rt', encoding = encoding).read().splitlines()

        mrk_list = []
        for row in marc_list:
            if row.startswith('=LDR'):
                mrk_list.append([row])
            else:
                if row:
                    mrk_list[-1].append(row)
                    
        for lista in mrk_list:
            slownik = {}
            for el in lista:
                if el[1:4] in slownik:
                    slownik[el[1:4]] += f"❦{el[6:]}"
                else:
                    slownik[el[1:4]] = el[6:]
            final_list.append(slownik)

# sorted_items = sorted(final_list.items(), key = lambda item : len(item[1]), reverse=True)
# newd = dict(sorted_items)
# with open("swe_harvesting.json", 'w', encoding='utf-8') as f: 
#     json.dump(final_list, f, ensure_ascii=False, indent=4)

final_list = [d for d in final_list if '041' in d.keys() and '$h' in d['041']]       
marc_df = pd.DataFrame(final_list)
fields = marc_df.columns.tolist()
fields = [i for i in fields if 'LDR' in i or re.compile('\d{3}').findall(i)]
marc_df = marc_df.loc[:, marc_df.columns.isin(fields)]
fields.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
marc_df = marc_df.reindex(columns=fields)   

marc_df.to_excel(f'Translation_into_Swedish_{year}-{month}-{day}.xlsx', index=False)



















