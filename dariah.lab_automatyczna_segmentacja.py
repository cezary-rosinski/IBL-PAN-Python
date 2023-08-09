pliki hocr artykułów tutaj: https://drive.google.com/file/d/1HtJdNdfXn4Ih_CRAy0CfCqze_43THEzq/view?usp=drive_link
przygotowanie kodu w Pythonie, który pracuje na usłudze BB i pozyskuje abstrakty dla tekstów → https://converter-hocr.services.clarin-pl.eu/docs
spakowane abstrakty w formacie .zip przesłać do usługi https://services.clarin-pl.eu/login i pobrać JSON z wynikami

from glob import glob
import requests
import xml.etree.ElementTree as et
import lxml.etree


path = r"D:\IBL\Biblioteka Nauki\Dariah.lab hOCR\hOCR/"
files_hocr = [f for f in glob(f"{path}*", recursive=True)]


url = 'https://converter-hocr.services.clarin-pl.eu/docs'

url = 'https://converter-hocr.services.clarin-pl.eu/convert/'

curl -X 'POST' \
  'https://converter-hocr.services.clarin-pl.eu/convert/' \
  -H 'accept: application/json' \
  -H 'Content-Type: multipart/form-data' \
  -F 'file=@bibliotekanauki_87574.alto.hOCR'
  


headers = {
    'accept': 'application/json',
    # requests won't add a boundary if this header is set when you pass files=
    # 'Content-Type': 'multipart/form-data',
}

files = {
    # 'file': open('bibliotekanauki_87574.alto.hOCR', 'rb'),
    'file': open(files_hocr[0], 'rb'),
}

response = requests.post('https://converter-hocr.services.clarin-pl.eu/convert/', headers=headers, files=files)

dir(response)

xml = et.fromstring(response.text)
test = xml.findall(".//[@class='abstract_en']")

for el in xml[-1]:
    print(el.tag)
    print(el.attrib)
    print(el.text)

len(xml[-1][1])

for el in xml[-1][1]:
    print(el.tag)
    print(el.attrib)
    print(el.text)
  
for e in xml[-1]: #poziom strony
    for el in e: #poziom elementu na stronie
        for ele in el:
            if el.attrib.get('class') in ['abstract_en', 'abstract_pl']:
                print(el.attrib.get('class'))
                print(ele.text)
            print(el.attrib.get('class'))
            print(ele.text)
            
            print(ele.tag)
            print(ele.attrib)
            print(ele.text)
        

    
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  