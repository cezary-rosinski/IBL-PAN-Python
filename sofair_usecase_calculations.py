import glob
from collections import Counter

#%%
path = r'C:\Users\Cezary\Downloads\sofair texts/'
folders = [f for f in glob.glob(path + '**', recursive=True)]

titles = set([el for el in [e.split('\\')[5] for e in folders] if el])

files = [f for f in folders if 'pdf' in f]
titles = [el for el in [e.split('\\')[5] for e in files] if el]

Counter(titles)









