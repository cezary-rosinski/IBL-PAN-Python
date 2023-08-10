# pliki hocr artykułów tutaj: https://drive.google.com/file/d/1HtJdNdfXn4Ih_CRAy0CfCqze_43THEzq/view?usp=drive_link
# przygotowanie kodu w Pythonie, który pracuje na usłudze BB i pozyskuje abstrakty dla tekstów → https://converter-hocr.services.clarin-pl.eu/docs
# spakowane abstrakty w formacie .zip przesłać do usługi https://services.clarin-pl.eu/login i pobrać JSON z wynikami

from glob import glob
import requests
import xml.etree.ElementTree as et
import lxml.etree
from bs4 import BeautifulSoup
from tqdm import tqdm
import regex as re
import zipfile
import os
from concurrent.futures import ThreadPoolExecutor
import json
import pandas as pd

#%%
path = r"D:\IBL\Biblioteka Nauki\Dariah.lab hOCR\hOCR/"
files_hocr = [f for f in glob(f"{path}*", recursive=True)]


url = 'https://converter-hocr.services.clarin-pl.eu/convert/'

# curl -X 'POST' \
#   'https://converter-hocr.services.clarin-pl.eu/convert/' \
#   -H 'accept: application/json' \
#   -H 'Content-Type: multipart/form-data' \
#   -F 'file=@bibliotekanauki_87574.alto.hOCR'
  


headers = {
    'accept': 'application/json',
    # requests won't add a boundary if this header is set when you pass files=
    # 'Content-Type': 'multipart/form-data',
}

# results = {}
def segment_hocr(file):
# for file in tqdm(files_hocr):
    # file = files_hocr[0]
    files = {
        # 'file': open('bibliotekanauki_87574.alto.hOCR', 'rb'),
        'file': open(file, 'rb')}
    file_name = re.findall('(bibliotekanauki_.+?)(?=\.)', file)[0]

    response = requests.post(url, headers=headers, files=files)

    soup = BeautifulSoup(response.content, 'xml')
    try:
        results.update({file_name: {'abstract_pl': soup.find("div", {"class": "abstract"}).text.strip()}})
    except AttributeError:
        results.update({file_name: {'abstract_pl': None}})
        
    try:
        results[file_name].update({'abstract_en': soup.find("div", {"class": "abstract_en"}).text.strip()})
    except AttributeError:
        results[file_name].update({'abstract_en': None})
        
results = {}
with ThreadPoolExecutor() as executor:
    list(tqdm(executor.map(segment_hocr,files_hocr), total=len(files_hocr)))        
        
for k,v in tqdm(results.items()):
    if 'abstract_pl' in v and v.get('abstract_pl'):
        with open(f"data/bibliotekanauki/pl/{k}.txt", 'wt', encoding='utf-8') as f:
            f.write(v.get('abstract_pl'))
    if 'abstract_en' in v and v.get('abstract_en'):
        with open(f"data/bibliotekanauki/en/{k}.txt", 'wt', encoding='utf-8') as f:
            f.write(v.get('abstract_en'))


def save_zip(path, zip_name, extension: list):
    name = path
    zip_name = name + zip_name
    with zipfile.ZipFile(zip_name, 'w', zipfile.ZIP_DEFLATED) as zip_ref:
        for folder_name, subfolders, filenames in os.walk(name):
            for filename in filenames:
                if filename.split('.')[-1] in extension:
                    file_path = os.path.join(folder_name, filename)
                    zip_ref.write(file_path, arcname=os.path.relpath(file_path, name))
    zip_ref.close()

save_zip('data/bibliotekanauki/pl/', 'bibliotekanauki_pl.zip', ['txt'])
save_zip('data/bibliotekanauki/en/', 'bibliotekanauki_en.zip', ['txt'])
  
  
#%% keywords

path = r"C:\Users\Cezary\Documents\IBL-PAN-Python\data\bibliotekanauki\pl\keywords/"
files = [f for f in glob(f"{path}*", recursive=True)]

keywords_dict = {}
for file in tqdm(files):
    with open(file, encoding='utf-8') as f:
        file_contents = json.load(f)
    file_name = re.findall('(bibliotekanauki_.+?)(?=\.)', file)[0]
    keywords_dict.update({file_name: file_contents.get('clarin').get('labels')})
  
df = pd.DataFrame().from_dict(keywords_dict, orient='index')
df.to_excel('data/bibliotekanauki/clarin_keywords_bibliotekanauki_probka.xlsx')
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  