from my_functions import gsheet_to_df 
import requests
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import pandas as pd





authority_df = gsheet_to_df('1yKcilB7SEVUkcSmqiTPauPYtALciDKatMvgqiRoEBso', 'Sheet1')
viafs_set = set(authority_df['proper_viaf_id'].to_list())

def get_viaf_gender(viaf_id):
    url = f'https://viaf.org/viaf/{viaf_id}/viaf.json'
    r = requests.get(url).json()
    if r['mainHeadings']:
        gender = r['fixed']['gender']
        viaf_gender_resp[viaf_id] = gender
    elif r['redirect']:
        new_viaf = r['redirect']['directto']
        get_viaf_gender(new_viaf)
        
viaf_gender_resp = {}
with ThreadPoolExecutor(max_workers=50) as executor:
    list(tqdm(executor.map(get_viaf_gender, viafs_set), total=len(viafs_set)))
    
gender_dict = {'a': 'female',
               'b': 'male',
               'u': 'unknown'}

viaf_gender_resp = {k:gender_dict[v] for k,v in viaf_gender_resp.items()}
df_viaf_gender = pd.DataFrame.from_dict(viaf_gender_resp, orient='index')
































