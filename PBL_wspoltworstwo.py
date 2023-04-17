import cx_Oracle
import sys
sys.path.insert(1, 'C:/Users/Cezary/Documents/IBL-PAN-Python')
from pbl_credentials import pbl_user, pbl_password
from my_functions import gsheet_to_df
import pandas as pd
import numpy as np
from tqdm import tqdm
import regex as re
from SPARQLWrapper import SPARQLWrapper, JSON
from urllib.error import HTTPError, URLError
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import pickle
import requests
from my_functions import gsheet_to_df

dsn_tns = cx_Oracle.makedsn('pbl.ibl.poznan.pl', '1521', service_name='xe')
connection = cx_Oracle.connect(user=pbl_user, password=pbl_password, dsn=dsn_tns, encoding='windows-1250')

pbl_query = """select z.ZA_ZAPIS_ID, os.OS_OSOBA_ID, os.OS_NAZWISKO, os.OS_IMIE, fo.FO_NAZWA, z.ZA_OPIS_WSPOLTWORCOW
from IBL_OWNER.PBL_OSOBY os, pbl_zapisy z, IBL_OWNER.PBL_FUNKCJE_OSOB fo, IBL_OWNER.PBL_UDZIALY_OSOB uo
where uo.UO_FO_SYMBOL=fo.FO_SYMBOL
and uo.UO_OS_OSOBA_ID=os.OS_OSOBA_ID
and uo.UO_ZA_ZAPIS_ID=z.ZA_ZAPIS_ID
and z.ZA_TYPE='KS'
order by z.ZA_ZAPIS_ID asc"""
pbl_query = pd.read_sql(pbl_query, con=connection).fillna(value = np.nan)

temp_df = gsheet_to_df('12HySgQiyEFDvsZYsy3nax7-n2fpJajtGsdKXKE_McFU', 'Export Worksheet')
temp_df_selected_columns = temp_df.copy()[['ZA_ZAPIS_ID', 'OS_OSOBA_ID', 'OS_NAZWISKO', 'OS_IMIE', 'FO_NAZWA', 'ZA_OPIS_WSPOLTWORCOW']]
s = temp_df_selected_columns.iloc[0,:]

s.isin(pbl_query)

pbl_query.columns.values

test = pbl_query.loc[pbl_query['ZA_ZAPIS_ID'] == 84620]

if(s.values.tolist() in test.values.tolist()):
    print("Series present in  Datframe ")
else:
    print("Series NOT present in  Datframe ")