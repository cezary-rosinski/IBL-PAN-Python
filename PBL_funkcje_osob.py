import cx_Oracle
from pbl_credentials import pbl_user, pbl_password
import pandas as pd
from my_functions import gsheet_to_df
import numpy as np
from tqdm import tqdm
import gspread as gs
from pydrive.auth import GoogleAuth
from gspread_dataframe import set_with_dataframe, get_as_dataframe

#%% pbl database

dsn_tns = cx_Oracle.makedsn('pbl.ibl.poznan.pl', '1521', service_name='xe')
connection = cx_Oracle.connect(user=pbl_user, password=pbl_password, dsn=dsn_tns, encoding='windows-1250')

#%%
#autoryzacja do tworzenia i edycji plików
gc = gs.oauth()
#autoryzacja do penetrowania dysku
gauth = GoogleAuth()
gauth.LocalWebserverAuth()

#%% data
old_df = gsheet_to_df('12HySgQiyEFDvsZYsy3nax7-n2fpJajtGsdKXKE_McFU', 'Export Worksheet')

pbl_query = """select z.ZA_ZAPIS_ID, os.OS_OSOBA_ID, os.OS_NAZWISKO, os.OS_IMIE, fo.FO_NAZWA, z.ZA_OPIS_WSPOLTWORCOW
from IBL_OWNER.PBL_OSOBY os, pbl_zapisy z, IBL_OWNER.PBL_FUNKCJE_OSOB fo, IBL_OWNER.PBL_UDZIALY_OSOB uo
where uo.UO_FO_SYMBOL=fo.FO_SYMBOL
and uo.UO_OS_OSOBA_ID=os.OS_OSOBA_ID
and uo.UO_ZA_ZAPIS_ID=z.ZA_ZAPIS_ID
and z.ZA_TYPE='KS'
order by z.ZA_ZAPIS_ID asc"""

pbl_result = pd.read_sql(pbl_query, con=connection).fillna(value = np.nan)

old_df_limited = old_df[pbl_result.columns]
old_df_limited['ZA_ZAPIS_ID'] = old_df_limited['ZA_ZAPIS_ID'].astype(np.int64)
old_df_limited['OS_OSOBA_ID'] = old_df_limited['OS_OSOBA_ID'].astype(np.int64)

new_df = pbl_result.loc[~pbl_result['ZA_ZAPIS_ID'].isin(old_df_limited['ZA_ZAPIS_ID'].to_list())]

duplicates = pbl_result.loc[pbl_result['ZA_ZAPIS_ID'].isin(old_df_limited['ZA_ZAPIS_ID'].to_list())]

duplicates_ids = set(duplicates['ZA_ZAPIS_ID'].to_list())

records_to_check = []
for i in tqdm(duplicates_ids):
    # i = list(duplicates_ids)[0]
    d_df = duplicates.loc[duplicates['ZA_ZAPIS_ID'] == i]
    o_df = old_df_limited.loc[old_df_limited['ZA_ZAPIS_ID'] == i]

    #merge two dataFrames and add indicator column
    all_df = pd.merge(d_df, o_df, on=list(d_df.columns.values), how='left', indicator='exists')

    #add column to show if each row in first DataFrame exists in second
    all_df['exists'] = np.where(all_df.exists == 'both', True, False)
    for i, row in all_df.iterrows():
        # row = all_df.iloc[0,:]
        if row['exists'] == False:
            row = row.drop('exists')
            records_to_check.append(row)

test = pd.DataFrame(records_to_check).drop_duplicates()

#%% upload
sheet_id = gc.open_by_key('12HySgQiyEFDvsZYsy3nax7-n2fpJajtGsdKXKE_McFU')

sheet_id.add_worksheet(title="nowe", rows="100", cols="20")
set_with_dataframe(sheet_id.worksheet('nowe'), duplicates)
sheet_id.add_worksheet(title="duplikaty", rows="100", cols="20")
set_with_dataframe(sheet_id.worksheet('duplikaty'), test)

worksheets = ['nowe', 'duplikaty']
for worksheet in worksheets:
    worksheet = sheet_id.worksheet(worksheet)
    
    sheet_id.batch_update({
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







        
        
        
        
        
        
        
