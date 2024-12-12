import pandas as pd
import glob
from tqdm import tqdm
import xml.etree.ElementTree as ET 

#%% main
    
paths = ['D:\\IBL\\PALOMERA\\documents/', 'D:\\IBL\\PALOMERA\\interviews/']

files = []
for path in paths:
    # path = paths[0]
    folders = glob.glob(path + '*' + '/', recursive=True)
    for folder in folders:
        # folder = folders[0]
        file = glob.glob(folder + 'dublin_core.xml')
        files.extend(file)

final_list = []
for file in tqdm(files):
    # file = files[0]
    tree = ET.parse(file)
    root = tree.getroot()
    
    # for child in root:
    #     print(child.attrib, child.text)
    # file_dict = {}
    file_list = []
    for child in root:
        file_list.append((child.attrib.get('element') + "|" + child.attrib.get('qualifier'), child.text))
        # file_dict.update({child.attrib.get('element') + "|" + child.attrib.get('qualifier'): child.text})
    file_list.append(('folder', '-'.join(file.split('\\')[3:5])))
    file_dict = {}
    for d, t in file_list:
        group = file_dict.setdefault(d, [])
        group.append(t)
    file_dict = {k:' | '.join(v) for k,v in file_dict.items()}
        
        # file_dict.update({'folder': '-'.join(file.split('\\')[3:5])})
    final_list.append(file_dict)    
    
df = pd.DataFrame(final_list)
df.to_excel('data/PALOMERA_dataset.xlsx', index=False)
        







