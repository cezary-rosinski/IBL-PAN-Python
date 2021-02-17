import pandas as pd
import io
import requests
from sickle import Sickle
import xml.etree.ElementTree as et
import lxml.etree
import pdfplumber

oai_url = 'http://pressto.amu.edu.pl/index.php/fp/oai'
sickle = Sickle(oai_url)

records = sickle.ListRecords(metadataPrefix='oai_dc')

# tree_pretty = lxml.etree.parse('test.xml')
# pretty = lxml.etree.tostring(tree, encoding="unicode", pretty_print=True)

forum_poetyki_list_of_dicts = []
for record in records:
    with open('test.xml', 'wt', encoding='UTF-8') as file:
        file.write(str(record))
    tree = et.parse('test.xml')
    root = tree.getroot()
    article = root.findall('.//{http://www.openarchives.org/OAI/2.0/}metadata/{http://www.openarchives.org/OAI/2.0/oai_dc/}dc/')
    forum_poetyki_dict = {}
    for node in article:
        try:
            if (node.tag.split('}')[-1], [v for k,v in node.attrib.items()][0]) in forum_poetyki_dict:
                forum_poetyki_dict[(node.tag.split('}')[-1], [v for k,v in node.attrib.items()][0])] += f"❦{node.text}"
            else:
                forum_poetyki_dict[(node.tag.split('}')[-1], [v for k,v in node.attrib.items()][0])] = node.text
        except IndexError:
            if node.tag.split('}')[-1] in forum_poetyki_dict:
                forum_poetyki_dict[node.tag.split('}')[-1]] += f"❦{node.text}"
            else:
                forum_poetyki_dict[node.tag.split('}')[-1]] = node.text  
    forum_poetyki_list_of_dicts.append(forum_poetyki_dict)

df = pd.DataFrame(forum_poetyki_list_of_dicts)
df = df[df['relation'].notnull()].reset_index(drop=True)

#os.mkdir('/forum_poetyki_txt')
for i, row in df.iterrows():
    print(f"{i+1}/{len(df)}")
    if len(row['relation'].split('❦')) == 2:
        pl_pdf = row['relation'].split('❦')[0].replace('article/view', 'article/download')
        r = requests.get(pl_pdf, stream=True)
        with open('test.pdf', 'wb') as fd:
            fd.write(r.content)
        with pdfplumber.open('test.pdf') as pdf:
            pl_txt = ''
            for page in pdf.pages:
                pl_txt += '\n' + page.extract_text()
        txt = io.open(f"forum_poetyki_pl_{i+1}.txt", 'wt', encoding='UTF-8')
        txt.write(pl_txt)
        txt.close()
        
        eng_pdf = row['relation'].split('❦')[1].replace('article/view', 'article/download')
        r = requests.get(eng_pdf, stream=True)
        with open('test.pdf', 'wb') as fd:
            fd.write(r.content)
        with pdfplumber.open('test.pdf') as pdf:
            eng_txt = ''
            for page in pdf.pages:
                eng_txt += '\n' + page.extract_text()
        txt = io.open(f"forum_poetyki_eng_{i+1}.txt", 'wt', encoding='UTF-8')
        txt.write(eng_txt)
        txt.close()
        









































