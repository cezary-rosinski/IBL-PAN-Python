from my_functions import gsheet_to_df, marc_parser_1_field, create_google_worksheet
import requests
from collections import Counter
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import gspread as gs
from gspread_dataframe import set_with_dataframe, get_as_dataframe

#%%
def link_locsh(x):
    try: return f'http://id.loc.gov/authorities/subjects/{x.strip()}' if not(isinstance(x, float)) and x.strip()[0] != 'h' else x.strip().replace('https', 'http').replace('.html', '') if isinstance(x, str) else x
    except IndexError:
        print(x)
        
def get_desk_name(el):
    # el = deskryptory_list[0]
    el_json = f'{el}.json'
    try:
        res = requests.get(el_json).json()
        desk_name = [el for el in [e for e in res if e['@id'] == el][0]['http://www.loc.gov/mads/rdf/v1#authoritativeLabel']][0]['@value']
        deskryptory_dict[el] = desk_name
    except (ValueError, IndexError):
        errors.append(el)
#%%

deskryptory = pd.DataFrame()
worksheets = ['BD', 'BL', 'AW']
for wsh in worksheets:
    temp_df = gsheet_to_df('1NDckQjp8OpqSKJN4-AWYJRpPPkFqL7toKy3Emafpdug', wsh)
    deskryptory = deskryptory.append(temp_df)
    
deskryptory['link LCSH'] = deskryptory['link LCSH'].apply(lambda x: link_locsh(x))

deskryptory_ok = deskryptory[deskryptory['link LCSH'].notnull()]

deskryptory_list = deskryptory_ok['link LCSH'].to_list()
desk_count = dict(Counter(deskryptory_list))

deskryptory_dict = {}
errors = []
    
desk_set = set(deskryptory_list)    
    
with ThreadPoolExecutor() as executor:
    list(tqdm(executor.map(get_desk_name, desk_set), total=len(desk_set)))

desk_df = pd.DataFrame.from_dict(deskryptory_dict, orient='index').reset_index().rename(columns={0:'eng name', 'index':'link LCSH'})

df = pd.merge(deskryptory_ok[['desk BN', 'link LCSH']], desk_df, on='link LCSH')[['link LCSH', 'desk BN', 'eng name']].sort_values('link LCSH')

#%% connect google drive

#autoryzacja do tworzenia i edycji plików
gc = gs.oauth()
#autoryzacja do penetrowania dysku
gauth = GoogleAuth()
gauth.LocalWebserverAuth()
drive = GoogleDrive(gauth)

sheet = gc.open_by_key('1NDckQjp8OpqSKJN4-AWYJRpPPkFqL7toKy3Emafpdug')
try:
    set_with_dataframe(sheet.worksheet('do analizy'), df)
except gs.WorksheetNotFound:
    sheet.add_worksheet(title="do analizy", rows="100", cols="20")
    set_with_dataframe(sheet.worksheet('do analizy'), df)

worksheet = sheet.worksheet('do analizy')

sheet.batch_update({
    "requests": [
        {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": worksheet._properties['sheetId'],
                    "dimension": "ROWS",
                    "startIndex": 0,
                    #"endIndex": 100
                },
                "properties": {
                    "pixelSize": 20
                },
                "fields": "pixelSize"
            }
        }
    ]
})

worksheet.freeze(rows=1)
worksheet.set_basic_filter()


#%%
deskryptory = pd.DataFrame()
worksheets = ['BD', 'BL', 'AW']
for wsh in worksheets:
    temp_df = gsheet_to_df('1NDckQjp8OpqSKJN4-AWYJRpPPkFqL7toKy3Emafpdug', wsh)
    deskryptory = deskryptory.append(temp_df)
    
deskryptory_ok = gsheet_to_df('1NDckQjp8OpqSKJN4-AWYJRpPPkFqL7toKy3Emafpdug', 'do analizy')

deskryptory_rest = deskryptory[~deskryptory['desk BN'].isin(deskryptory_ok['desk BN'])]
deskryptory_rest = deskryptory_rest[(deskryptory_rest['eng label'].notnull()) |
                                    (deskryptory_rest['Dodatkowe linki'].notnull()) |
                                    (deskryptory_rest['Uwagi'].notnull())]


#%%

deskryptory_ok = gsheet_to_df('1NDckQjp8OpqSKJN4-AWYJRpPPkFqL7toKy3Emafpdug', 'do analizy')
deskryptory_ok = deskryptory_ok[(~deskryptory_ok['uwagi'].str.contains('out', na=False)) &
                                (deskryptory_ok['genre/forms'].isna())]

deskryptory_ok['link LCSH'] = deskryptory_ok[['uwagi', 'link LCSH']].apply(lambda x: x['uwagi'] if not(isinstance(x['uwagi'],float)) else x['link LCSH'], axis=1)

deskryptory_list = deskryptory_ok['link LCSH'].to_list()
deskryptory_dict = {}
errors = []
desk_set = set(deskryptory_list)    
    
with ThreadPoolExecutor() as executor:
    list(tqdm(executor.map(get_desk_name, desk_set), total=len(desk_set)))

desk_df = pd.DataFrame.from_dict(deskryptory_dict, orient='index').reset_index().rename(columns={0:'eng name', 'index':'link LCSH'})

df = pd.merge(deskryptory_ok[['desk BN', 'link LCSH']], desk_df, on='link LCSH')[['link LCSH', 'desk BN', 'eng name']].sort_values('link LCSH')

create_google_worksheet(sheet, 'gotowe', df)













































