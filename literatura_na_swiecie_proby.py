import pandas as pd
import io
import requests
from sickle import Sickle
import xml.etree.ElementTree as et
import lxml.etree
import pdfplumber
import json
from tqdm import tqdm
import regex as re


# with open("lns1_1971-2014.pdf", 'wb') as fd:
#             fd.write(r.content)
with pdfplumber.open("lns1_1971-2014.pdf") as pdf:
    pl_txt = ''
    for page in tqdm(pdf.pages):
        pl_txt += '\n' + page.extract_text()
txt = io.open("lns1_1971-2014.txt", 'wt', encoding='UTF-8')
txt.write(pl_txt)
txt.close()


test = io.open('lns1_1971-2014.txt', encoding='utf8').readlines()


lista = []
for row in test:
    try:
        if re.findall('^\p{Lu} \p{Lu} \p{Lu}', row):
            lista.append([row])
        else:
            if row:
                lista[-1].append(row)
    except IndexError:
        pass

slownik = {}
for row in test:
    try:
        if re.findall('^\p{Lu} \p{Lu} \p{Lu}', row):
            nazwa_literatury = re.findall('^\p{Lu} \p{Lu} \p{Lu}.+', row)[0].strip()
            slownik[nazwa_literatury] = [row]
        else:
            if row:
                slownik[nazwa_literatury].append(row)
    except (KeyError, NameError):
        pass

argentyna = slownik['A R G E N T Y N A   AR']
argentyna_lista_list = []
for index, lista in enumerate(argentyna):
    try:
        if re.findall('^ {0,10}\.+', lista):
            argentyna_lista_list.append([lista])
        else:
            argentyna_lista_list[-1].append(lista)
    except IndexError:
        pass
        

# for k,v in slownik.items():
#     for element in v:
#         try:
#             if re.findall('^ {0,10}\.+', element):
#                 slownik[k].append([element])
#             else:
#                 slownik[k][-1].append(element)
#         except (TypeError, AttributeError):
#             pass













ttt = test[:20000]

lista = re.split('(^\p{Lu} \p{Lu})', ttt)
