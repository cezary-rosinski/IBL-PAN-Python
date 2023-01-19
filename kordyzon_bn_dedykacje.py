import glob
from tqdm import tqdm
from my_functions import mrc_to_mrk
import sys
sys.path.insert(1, 'C:/Users/Cezary/Documents/IBL-PAN-Python')
from marc_functions import read_mrk

#%% main
#mrc to mrk 
path = r"F:\Cezary\Documents\IBL\BN\bn_all\2023-01-19/"
files = [f for f in glob.glob(path + '*.mrc', recursive=True)]
for file_path in tqdm(files):
    path_mrk = file_path.replace('.mrc', '.mrk')
    mrc_to_mrk(file_path, path_mrk)
    
#zrobić wykres z podziałem na stulecia z liczbą rekordów z dedykacją + relacja dedykacja vs. niededykacja

# Wojtek mówi, że dedykacja w polach 700 i 600, w 500 są luźne uwagi, nieustrukturyzowane
# Starsze w 700, w 600 nowe deskryptory
