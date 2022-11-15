import cx_Oracle
import pandas as pd
import numpy as np
from my_functions import gsheet_to_df

#%% SQL connection

dsn_tns = cx_Oracle.makedsn('pbl.ibl.poznan.pl', '1521', service_name='xe')
connection = cx_Oracle.connect(user='IBL_SELECT', password='CR333444', dsn=dsn_tns, encoding='windows-1250')

#%% PBL query

pbl_query = """select z.za_zapis_id, z.za_ro_rok, dz.dz_dzial_id, dz.dz_nazwa, rz.rz_nazwa, z.za_tytul, z.za_adnotacje, z.za_opis_imprezy, z.za_organizator
                from IBL_OWNER.pbl_zapisy z
                full join IBL_OWNER.pbl_rodzaje_zapisow rz on rz.rz_rodzaj_id = z.za_rz_rodzaj1_id
                full join IBL_OWNER.pbl_dzialy dz on dz.dz_dzial_id = z.za_dz_dzial1_id
                where z.za_type = 'IR'
                and rz.rz_rodzaj_id = 301"""                  
pbl_sh_query1 = """select hpz.hz_za_zapis_id,hp.hp_nazwa,khp.kh_nazwa
                from IBL_OWNER.pbl_hasla_przekrojowe hp
                join IBL_OWNER.pbl_hasla_przekr_zapisow hpz on hpz.hz_hp_haslo_id=hp.hp_haslo_id
                join IBL_OWNER.pbl_klucze_hasla_przekr khp on khp.kh_hp_haslo_id=hp.hp_haslo_id
                join IBL_OWNER.pbl_hasla_zapisow_klucze hzk on hzk.hzkh_hz_hp_haslo_id=hp.hp_haslo_id and hzk.hzkh_kh_klucz_id=khp.kh_klucz_id and hzk.hzkh_hz_za_zapis_id=hpz.hz_za_zapis_id"""               
pbl_sh_query2 = """select hpz.hz_za_zapis_id,hp.hp_nazwa,to_char(null) "KH_NAZWA"
                from IBL_OWNER.pbl_hasla_przekrojowe hp
                join IBL_OWNER.pbl_hasla_przekr_zapisow hpz on hpz.hz_hp_haslo_id=hp.hp_haslo_id"""

pbl_persons_index_query = """select odi_za_zapis_id, odi_nazwisko, odi_imie
                from IBL_OWNER.pbl_osoby_do_indeksu"""

pbl_literary_awards = pd.read_sql(pbl_query, con=connection).fillna(value = np.nan)
pbl_persons_index = pd.read_sql(pbl_persons_index_query, con=connection).fillna(value = np.nan)
pbl_sh1 = pd.read_sql(pbl_sh_query1, con=connection).fillna(value = np.nan)
pbl_sh2 = pd.read_sql(pbl_sh_query2, con=connection).fillna(value = np.nan)
pbl_sh2 = pbl_sh2.merge(pbl_sh1, how='left', on = ['HZ_ZA_ZAPIS_ID', 'HP_NAZWA'], indicator=True)
pbl_sh2 = pbl_sh2.loc[pbl_sh2['_merge'] == 'left_only'][['HZ_ZA_ZAPIS_ID', 'HP_NAZWA', 'KH_NAZWA_x']]
pbl_sh2 = pbl_sh2.rename(columns = {'KH_NAZWA_x': 'KH_NAZWA'})
pbl_subject_headings = pd.concat([pbl_sh1,pbl_sh2]).drop_duplicates(keep=False)

pbl_literary_awards = pd.merge(pbl_literary_awards, pbl_persons_index,  how='left', left_on = 'ZA_ZAPIS_ID', right_on = 'ODI_ZA_ZAPIS_ID') 
pbl_literary_awards = pd.merge(pbl_literary_awards, pbl_subject_headings,  how='left', left_on = 'ZA_ZAPIS_ID', right_on = 'HZ_ZA_ZAPIS_ID') 

pbl_literary_awards.to_excel('pbl_literary_awards.xlsx', index=False)

#%% PBL processing

pbl_literary_awards = gsheet_to_df('1suuyU2Xg59SeVwSDdRgNrX9jEHsKm2JKz1M620pJxpI', 'Sheet1')
