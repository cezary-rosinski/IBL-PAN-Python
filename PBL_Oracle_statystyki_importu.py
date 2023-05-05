import cx_Oracle
import sys
from pbl_credentials import pbl_user, pbl_password
import pandas as pd
import warnings
warnings.filterwarnings("ignore", category=UserWarning)

#%% connection
dsn_tns = cx_Oracle.makedsn('pbl.ibl.poznan.pl', '1521', service_name='xe')
connection = cx_Oracle.connect(user=pbl_user, password=pbl_password, dsn=dsn_tns, encoding='windows-1250')

#%% Additional tables

period = range(2004,2017)

for year in period:
    pbl_query = f"""select count(*) from IBL_OWNER.pbl_zapisy z
    where z.za_type like 'KS'
    and z.za_ro_rok = {year}
    and (z.za_status_imp like 'IOK' or z.za_status_imp is null)"""
    pbl_result = pd.read_sql(pbl_query, con=connection)['COUNT(*)'].values[0]
    print(f'{pbl_result} rekordów ksiazek w {year}')
    
    pbl_query = f"""select count(*) from IBL_OWNER.pbl_zapisy z
    where z.za_type like 'KS'
    and z.za_ro_rok = {year}
    and z.za_uzytk_wpisal like 'IMPORT'
    and z.za_status_imp like 'IOK'"""
    pbl_result = pd.read_sql(pbl_query, con=connection)['COUNT(*)'].values[0]
    print(f'{pbl_result} rekordów ksiazek z importu w {year}')
    
    pbl_query = f"""select count(*) from IBL_OWNER.pbl_zapisy z
    where z.za_type like 'KS'
    and z.za_ro_rok = {year}
    and z.za_uzytk_wpisal like 'IMPORT'
    and (z.za_status_imp like 'IOK' or z.za_status_imp is null)
    and z.za_uzytk_modyf is null"""
    pbl_result = pd.read_sql(pbl_query, con=connection)['COUNT(*)'].values[0]
    print(f'{pbl_result} rekordów ksiazek z importu w {year}, które powstaly w pelni automatycznie')
