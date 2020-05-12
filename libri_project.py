import cx_Oracle
import pandas as pd
from my_functions import gsheet_to_df
import pymarc
import numpy as np
import copy
import math
from datetime import datetime
from langdetect import detect_langs
from langdetect import detect
import re
from my_functions import cSplit

# read google sheets
pbl_viaf_names_dates = gsheet_to_df('1S_KsVppXzisd4zlsZ70rJE3d40SF9XRUO75no360dnY', 'pbl_viaf')
pbl_marc_roles = gsheet_to_df('1iQ4JGo1hLWZbQX4u2kB8LwqSWMLwicFfBsVFc9MNUUk', 'pbl-marc')
pbl_marc_roles = pbl_marc_roles[pbl_marc_roles['MARC_ABBREVIATION'] != '#N/A'][['PBL_NAZWA', 'MARC_ABBREVIATION']]
pbl_marc_roles['MARC_ABBREVIATION'] = pbl_marc_roles.groupby('PBL_NAZWA')['MARC_ABBREVIATION'].transform(lambda x: '|'.join(x))
pbl_marc_roles = pbl_marc_roles.drop_duplicates().reset_index(drop=True)

# read excel files
pbl_subject_headings_info = pd.read_csv("pbl_subject_headings.csv", sep=';', encoding='cp1250')
language_codes = pd.read_excel('language_codes_marc.xlsx', sheet_name = 'language')

# SQL connection

dsn_tns = cx_Oracle.makedsn('pbl.ibl.poznan.pl', '1521', service_name='xe')
connection = cx_Oracle.connect(user='IBL_SELECT', password='CR333444', dsn=dsn_tns, encoding='windows-1250')

# PBL queries

pbl_books_query = """select z.za_zapis_id "rekord_id", z.za_type "typ", rz.rz_rodzaj_id "rodzaj_zapisu_id", rz.rz_nazwa "rodzaj_zapisu", dz.dz_dzial_id "dzial_id", dz.dz_nazwa "dzial", to_char(tw.tw_tworca_id) "tworca_id", tw.tw_nazwisko "tworca_nazwisko", tw.tw_imie "tworca_imie", to_char(a.am_autor_id) "autor_id", a.am_nazwisko "autor_nazwisko", a.am_imie "autor_imie", z.za_tytul "tytul", z.za_opis_wspoltworcow "wspoltworcy", fo.fo_nazwa "funkcja_osoby", to_char(os.os_osoba_id) "wspoltworca_id", os.os_nazwisko "wspoltworca_nazwisko", os.os_imie "wspoltworca_imie", z.za_adnotacje "adnotacja", w.wy_nazwa "wydawnictwo", w.wy_miasto "miejscowosc", z.za_rok_wydania "rok_wydania", z.za_opis_fizyczny_ksiazki "opis_fizyczny", z.za_uzytk_wpisal, z.za_ro_rok, z.za_tytul_oryginalu, z.za_wydanie, z.za_instytucja, z.za_seria_wydawnicza, z.za_te_teatr_id,z.ZA_UZYTK_WPIS_DATA,z.ZA_UZYTK_MOD_DATA
                    from pbl_zapisy z
                    full outer join IBL_OWNER.pbl_zapisy_tworcy zt on zt.zatw_za_zapis_id=z.za_zapis_id
                    full outer join IBL_OWNER.pbl_tworcy tw on zt.zatw_tw_tworca_id=tw.tw_tworca_id
                    full outer join IBL_OWNER.pbl_zapisy_autorzy za on za.zaam_za_zapis_id=z.za_zapis_id
                    full outer join IBL_OWNER.pbl_autorzy a on za.zaam_am_autor_id=a.am_autor_id
                    full outer join IBL_OWNER.pbl_zapisy_wydawnictwa zw on zw.zawy_za_zapis_id=z.za_zapis_id 
                    full outer join IBL_OWNER.pbl_wydawnictwa w on zw.zawy_wy_wydawnictwo_id=w.wy_wydawnictwo_id
                    full outer join IBL_OWNER.pbl_dzialy dz on dz.dz_dzial_id=z.za_dz_dzial1_id
                    full outer join IBL_OWNER.pbl_rodzaje_zapisow rz on rz.rz_rodzaj_id=z.za_rz_rodzaj1_id
                    full outer join IBL_OWNER.pbl_udzialy_osob uo on uo.uo_za_zapis_id = z.za_zapis_id
                    full outer join IBL_OWNER.pbl_osoby os on uo.uo_os_osoba_id=os.os_osoba_id
                    full outer join IBL_OWNER.pbl_funkcje_osob fo on fo.fo_symbol=uo.uo_fo_symbol
                    where (z.za_status_imp is null OR z.za_status_imp like 'IOK')
                    and z.za_type like 'KS'"""
pbl_reviews_books_query = """select z.za_zapis_id "rec_id", a.am_nazwisko "rec_a_naz", a.am_imie "rec_a_im", z.za_tytul "rec_tyt", zrr.zrr_tytul "rec_czas_tyt", zrr.zrr_miejsce_wydania "rec_czas_miejsc", z.za_zrodlo_rok "rec_rok", z.za_zrodlo_nr "rec_nr", z.za_zrodlo_str "rec_str", z2.za_zapis_id "ks_id", a2.am_nazwisko "ks_a_naz", a2.am_imie "ks_a_im", z2.za_tytul "ks_tyt", w.wy_nazwa "ks_wyd_nazw", w.wy_miasto "ks_wyd_miejsc", z2.za_ro_rok
                    from pbl_zapisy z
                    join pbl_zapisy z2 on z.za_za_zapis_id=z2.za_zapis_id
                    full outer join IBL_OWNER.pbl_zapisy_autorzy za on za.zaam_za_zapis_id=z.za_zapis_id
                    full outer join IBL_OWNER.pbl_autorzy a on za.zaam_am_autor_id=a.am_autor_id
                    full outer join IBL_OWNER.pbl_zrodla zr on zr.zr_zrodlo_id=z.za_zr_zrodlo_id
                    full outer join IBL_OWNER.pbl_zrodla_roczniki zrr on zrr.zrr_zr_zrodlo_id=zr.zr_zrodlo_id and zrr.zrr_rocznik=z.za_zrodlo_rok 
                    full outer join IBL_OWNER.pbl_zapisy_autorzy za2 on za2.zaam_za_zapis_id=z2.za_zapis_id
                    full outer join IBL_OWNER.pbl_autorzy a2 on za2.zaam_am_autor_id=a2.am_autor_id
                    full outer join IBL_OWNER.pbl_zapisy_wydawnictwa zw on zw.zawy_za_zapis_id=z2.za_zapis_id 
                    full outer join IBL_OWNER.pbl_wydawnictwa w on zw.zawy_wy_wydawnictwo_id=w.wy_wydawnictwo_id
                    where z.za_rz_rodzaj1_id = 18
                    and z2.za_type like 'KS'
                    order by "ks_id" asc, "rec_id" asc""" 
pbl_articles_query = """select z.za_zapis_id "rekord_id", z.za_type "typ", rz.rz_rodzaj_id "rodzaj_zapisu_id", rz.rz_nazwa "rodzaj_zapisu", dz.dz_dzial_id "dzial_id", dz.dz_nazwa "dzial", to_char(tw.tw_tworca_id) "tworca_id", tw.tw_nazwisko "tworca_nazwisko", tw.tw_imie "tworca_imie", to_char(a.am_autor_id) "autor_id", a.am_nazwisko "autor_nazwisko", a.am_imie "autor_imie", z.za_tytul "tytul", z.za_opis_wspoltworcow "wspoltworcy", fo.fo_nazwa "funkcja_osoby", to_char(os.os_osoba_id) "wspoltworca_id", os.os_nazwisko "wspoltworca_nazwisko", os.os_imie "wspoltworca_imie", z.za_adnotacje "adnotacja", to_char(zr.zr_zrodlo_id) "czasopismo_id", zr.zr_tytul "czasopismo", z.za_zrodlo_rok "rok", z.za_zrodlo_nr "numer", z.za_zrodlo_str "strony",z.za_tytul_oryginalu,z.za_te_teatr_id,z.ZA_UZYTK_WPIS_DATA,z.ZA_UZYTK_MOD_DATA,z.ZA_TYPE
                    from pbl_zapisy z
                    full outer join IBL_OWNER.pbl_zapisy_tworcy zt on zt.zatw_za_zapis_id=z.za_zapis_id
                    full outer join IBL_OWNER.pbl_tworcy tw on zt.zatw_tw_tworca_id=tw.tw_tworca_id
                    full outer join IBL_OWNER.pbl_zapisy_autorzy za on za.zaam_za_zapis_id=z.za_zapis_id
                    full outer join IBL_OWNER.pbl_autorzy a on za.zaam_am_autor_id=a.am_autor_id
                    full outer join IBL_OWNER.pbl_zrodla zr on zr.zr_zrodlo_id=z.za_zr_zrodlo_id
                    full outer join IBL_OWNER.pbl_dzialy dz on dz.dz_dzial_id=z.za_dz_dzial1_id
                    full outer join IBL_OWNER.pbl_rodzaje_zapisow rz on rz.rz_rodzaj_id=z.za_rz_rodzaj1_id
                    full outer join IBL_OWNER.pbl_udzialy_osob uo on uo.uo_za_zapis_id = z.za_zapis_id
                    full outer join IBL_OWNER.pbl_osoby os on uo.uo_os_osoba_id=os.os_osoba_id
                    full outer join IBL_OWNER.pbl_funkcje_osob fo on fo.fo_symbol=uo.uo_fo_symbol
                    where z.za_type in ('IZA','PU')"""                  
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

pbl_books = pd.read_sql(pbl_books_query, con=connection).fillna(value = np.nan)
pbl_articles = pd.read_sql(pbl_articles_query, con=connection).fillna(value = np.nan)
pbl_persons_index = pd.read_sql(pbl_persons_index_query, con=connection).fillna(value = np.nan)
pbl_sh1 = pd.read_sql(pbl_sh_query1, con=connection).fillna(value = np.nan)
pbl_sh2 = pd.read_sql(pbl_sh_query2, con=connection).fillna(value = np.nan)
pbl_subject_headings = pd.concat([pbl_sh1,pbl_sh2]).drop_duplicates(keep=False)
del [pbl_sh1, pbl_sh2]

pbl_reviews_books = pd.read_sql(pbl_reviews_books_query, con=connection).fillna(value = np.nan)


pbl_reviews_books.loc[pbl_reviews_books['rec_a_naz'].isna(), 'rec_a_naz'] = '[no author]'
pbl_reviews_books.loc[pbl_reviews_books['rec_tyt'].isna(), 'rec_tyt'] = '[no title]'
pbl_reviews_books.loc[pbl_reviews_books['ks_a_naz'].isna(), 'ks_a_naz'] = '[no author]'
pbl_reviews_books['rec_autor'] = pbl_reviews_books[pbl_reviews_books.columns[1:3]].apply(lambda x: ' '.join(x.dropna().astype(str)), axis=1)
pbl_reviews_books = copy.copy(pbl_reviews_books)
pbl_reviews_books['rec_rok'] = pbl_reviews_books['rec_rok'].apply(lambda x: np.nan if math.isnan(x) else '{:4.0f}'.format(x))
pbl_reviews_books['rec_czas_miejsc'] = pbl_reviews_books[pbl_reviews_books.columns[2:5:7]].apply(lambda x: ' '.join(x.dropna().astype(str)), axis=1)
pbl_reviews_books['rec'] = pbl_reviews_books.iloc[:, [-1,3,4,5,7,8]].apply(lambda x: ', '.join(x.dropna().astype(str)), axis=1)
pbl_reviews_books['ks_autor'] = pbl_reviews_books[['ks_a_naz', 'ks_a_im']].apply(lambda x: ' '.join(x.dropna().astype(str)), axis=1)
pbl_reviews_books['ks_wyd_miejsc'] = pbl_reviews_books[['ks_wyd_miejsc', 'ZA_RO_ROK']].apply(lambda x: ' '.join(x.dropna().astype(str)), axis=1)
pbl_reviews_books['ks'] = pbl_reviews_books[['ks_autor', 'ks_tyt', 'ks_wyd_nazw', 'ks_wyd_miejsc']].apply(lambda x: ' '.join(x.dropna().astype(str)), axis=1)
pbl_reviews_books = pbl_reviews_books[['rec_id', 'rec', 'ks_id', 'ks']]

# PBL books

pbl_books = pd.merge(pbl_books, pbl_persons_index,  how='left', left_on = 'rekord_id', right_on = 'ODI_ZA_ZAPIS_ID') 
pbl_books = pd.merge(pbl_books, pbl_subject_headings,  how='left', left_on = 'rekord_id', right_on = 'HZ_ZA_ZAPIS_ID') 
pbl_books = pd.merge(pbl_books, pbl_viaf_names_dates,  how='left', left_on = 'tworca_id', right_on = 'pbl_id') 
pbl_books = pd.merge(pbl_books, pbl_marc_roles,  how='left', left_on = 'funkcja_osoby', right_on = 'PBL_NAZWA').sort_values('rekord_id').reset_index(drop=True)

# PBL books fields

#najpierw stworzenie tabeli, a potem iterowanie po wierszach, żeby tworzyć rekordy MARC

LDR = '-----nam---------4u-----'
X001 = pbl_books['rekord_id'].apply(lambda x: 'pl' + '{:09d}'.format(x)).to_frame().drop_duplicates()
X001.columns = ['001']
X005 = pbl_books[['rekord_id', 'ZA_UZYTK_MOD_DATA', 'ZA_UZYTK_WPIS_DATA']].drop_duplicates()
X005.columns = ['rekord_id', '005', 'ZA_UZYTK_WPIS_DATA']

def date_to_string(x):
    try:
        val = x.date().strftime('%Y%m%d')
    except ValueError:
        val = np.nan
    return val
X005['005'] = X005['005'].apply(lambda x: date_to_string(x))
X005['ZA_UZYTK_WPIS_DATA'] = X005['ZA_UZYTK_WPIS_DATA'].apply(lambda x: date_to_string(x))

def proper_date(row):
    if pd.isnull(row['ZA_UZYTK_WPIS_DATA']) and pd.isnull(row['005']):
        val = '19980218'
    elif pd.isnull(row['005']):
        val = row['ZA_UZYTK_WPIS_DATA']
    else:
        val = row['005']
    return val
X005['005'] = X005.apply(lambda x: proper_date(x), axis=1)
X005 = X005[['rekord_id', '005']]
X008 = pbl_books[['rekord_id', 'ZA_UZYTK_WPIS_DATA','ZA_RO_ROK', 'tytul']].drop_duplicates()
def date_to_string2(x):
    try:
        val = x.date().strftime('%y%m%d')
    except ValueError:
        val = '19980218'
    return val
X008['ZA_UZYTK_WPIS_DATA'] = X008['ZA_UZYTK_WPIS_DATA'].apply(lambda x: date_to_string2(x))
#czy to rozwiązanie jest okej?
def clear_tytul(row):
    if pd.isnull(row['tytul']):
        val = '[no title]'
    elif row['tytul'][-1] == ']':
        val = re.sub('(^.*?)([\.!?] {0,1})\[.*?$', r'\1\2', row['tytul']).strip()
    elif row['tytul'][0] == '[':
        val = re.sub('(^.*?\])(.*?$)', r'\2', row['tytul']).strip()
    else:
        val = row['tytul']
    return val
X008['tytul'] = X008.apply(lambda x: clear_tytul(x), axis=1)

def title_lang(x):
    try:
        if pd.isnull(x):
            val = ''
        elif x == '[no title]':
            val = ''
        else:
            val = detect(re.sub('[^a-zA-ZÀ-ž\s]', ' ', x.lower()))
    except:
        val = ''
    return val
        
X008['language'] = X008['tytul'].apply(lambda x: title_lang(x))
X008 = pd.merge(X008, language_codes[['country code', 'language code']],  how='left', left_on = 'language', right_on = 'country code')
X008 = X008.iloc[:, [0, 1, 2, 3, 6]]
X008['language code'] = X008['language code'].apply(lambda x: '  -d' if pd.isnull(x) else x)
X008['008'] = X008.apply(lambda x: f"{x['ZA_UZYTK_WPIS_DATA']}s{x['ZA_RO_ROK']}----                    {x['language code']}", axis = 1)
X008 = X008[['rekord_id', '008']]
X040 = '\\\\$aIBL$bpol'
X100 = pbl_books[['rekord_id', 'rodzaj_zapisu', 'tworca_nazwisko', 'tworca_imie', 'autor_nazwisko', 'autor_imie', '$a', '$d', '$0']].drop_duplicates()

def tworca_nazwisko_100(row):
    if row['rodzaj_zapisu'] == 'książka twórcy (podmiotowa)' and pd.notnull(row['tworca_nazwisko']) and ',' in row['tworca_nazwisko'] and row['tworca_imie'] == '*':
        val = row['autor_nazwisko']
    else:
        val = row['tworca_nazwisko']
    return val
X100['tworca_nazwisko'] = X100.apply(lambda x: tworca_nazwisko_100(x), axis=1)

def tworca_imie_100(row):
    if row['rodzaj_zapisu'] == 'książka twórcy (podmiotowa)' and pd.notnull(row['tworca_nazwisko']) and row['tworca_nazwisko'] == row['autor_nazwisko'] and row['tworca_imie'] == '*':
        val = row['autor_imie']
    else:
        val = row['tworca_imie']
    return val
X100['tworca_imie'] = X100.apply(lambda x: tworca_imie_100(x), axis=1)

def x100(row):
    if pd.isnull(row['tworca_nazwisko']) and row['rodzaj_zapisu'] == 'książka twórcy (podmiotowa)':
        val = np.nan
    else:
        if row['rodzaj_zapisu'] == 'książka twórcy (podmiotowa)' and len(f"str({row['tworca_nazwisko']}), str({row['tworca_imie']})") == len(str(row['$a'])) and ''.join(re.findall('[A-ZÀ-Ž]', f"{row['tworca_nazwisko']}, {row['tworca_imie']}")) == ''.join(re.findall('[A-ZÀ-Ž]', row['$a'])):
            val = f"1\\$a{row['$a']}$d{row['$d']}$4aut$0{row['$0']}"
        else:
            if row['rodzaj_zapisu'] == 'książka twórcy (podmiotowa)':
                val = f"1\\$a{row['tworca_nazwisko']}, {row['tworca_imie']}$d{row['$d']}$4aut$0{row['$0']}"
            else:
                if pd.notnull(row['autor_nazwisko']):
                    val = f"1 $a{row['autor_nazwisko']}, {row['autor_imie']}$4aut"
                else:
                    val = np.nan
    return val
X100['100'] = X100.apply(lambda x: x100(x), axis=1)     
X100 = X100[['rekord_id', '100']].drop_duplicates()
X100['100'] = X100['100'].str.replace('..nan', '')
X100['100'] = X100.groupby('rekord_id')['100'].transform(lambda x: '|'.join(x.dropna().str.strip()))
X100 = X100.drop_duplicates().reset_index(drop=True)
X100 = pd.DataFrame(X100['100'].str.split('|', 1).tolist(), index=X100['rekord_id'])
X100['rekord_id'] = X100.index
X100 = X100.reset_index(drop=True).fillna(value=np.nan).replace(r'^\s*$', np.nan, regex=True)
X100.columns = ['100', '700', 'rekord_id']
X240 = pbl_books[['rekord_id', 'ZA_TYTUL_ORYGINALU']].drop_duplicates()
X240.columns = ['rekord_id', '240']
X240['240'] = '\\\\$a' + X240['240'].str.replace('^(.*?\])(.*$)',r'\1', regex=True).str.replace('^\[|\]$', '', regex=True)
X245 = pbl_books[['rekord_id', 'autor_nazwisko', 'autor_imie', 'tytul', 'wspoltworcy', 'ZA_INSTYTUCJA']].drop_duplicates()    
X245['tytul_ok'] = X245.apply(lambda x: '10$a' + clear_tytul(x), axis=1)  
def autor_245(row):
    if pd.notnull(row['autor_nazwisko']) and pd.notnull(row['autor_imie']):
        val = f"$c{row['autor_imie']} {row['autor_nazwisko']}"
    elif pd.notnull(row['autor_nazwisko']) and pd.isnull(row['autor_imie']):
        val = f"$c{row['autor_nazwisko']}"
    else:
        val = '[no author]'
    return val

X245['autor'] = X245.apply(lambda x: autor_245(x), axis=1)
X245 = X245[['rekord_id', 'tytul_ok', 'autor', 'wspoltworcy', 'ZA_INSTYTUCJA']]
X245['tytul_ok'] = X245.groupby('rekord_id')['tytul_ok'].transform(lambda x: '|'.join(x.drop_duplicates().dropna().str.strip()))
X245['autor'] = X245.groupby('rekord_id')['autor'].transform(lambda x: '|'.join(x.drop_duplicates().dropna().str.strip()))
X245['autor'] = X245['autor'].str.replace('\|\$c', ', ', regex=True)
X245['wspoltworcy'] = X245.groupby('rekord_id')['wspoltworcy'].transform(lambda x: '|'.join(x.drop_duplicates().dropna().str.strip()))
X245['ZA_INSTYTUCJA'] = X245.groupby('rekord_id')['ZA_INSTYTUCJA'].transform(lambda x: '|'.join(x.drop_duplicates().dropna().str.strip()))
X245['245'] = X245.apply(lambda x: f"{x['tytul_ok']} /{x['autor']} ; {x['wspoltworcy']} ; {x['ZA_INSTYTUCJA']}.", axis=1)
X245['245'] = X245['245'].apply(lambda x: re.sub('( ;  ; )(\.)|( ;   ; )(\.)|( ; )(\.)|', r'\2', x)).apply(lambda x: re.sub('^\.$', '', x))
X245 = X245[['rekord_id', '245']].drop_duplicates()
X250 = pbl_books[['rekord_id', 'ZA_WYDANIE']].drop_duplicates()
X250.columns = ['rekord_id', '250']
X250['250'] = X250['250'].apply(lambda x: '\\\\$a' + x if pd.notnull(x) else np.nan)
X264 = pbl_books[['rekord_id', 'wydawnictwo', 'miejscowosc', 'ZA_RO_ROK']].drop_duplicates()
def wydawnictwo(row):
    if pd.isnull(row['wydawnictwo']) and pd.isnull(row['miejscowosc']):
        val = '1\\$aS.l. : $bs.n.'
    else:
        val = f"1\\$a{row['miejscowosc']} : $b{row['wydawnictwo']}"
    return val
X264['264'] = X264.apply(lambda x: wydawnictwo(x), axis=1)
X264 = X264[['rekord_id', '264', 'ZA_RO_ROK']]
X264['264'] = X264.groupby('rekord_id')['264'].transform(lambda x: '; '.join(x.dropna().str.strip()))
X264['ZA_RO_ROK'] = X264.groupby('rekord_id')['ZA_RO_ROK'].transform(lambda x: '|'.join(x.drop_duplicates().dropna().astype(str).str.strip()))
X264 = X264.drop_duplicates()
X264['264'] = X264['264'] + ',$c' + X264['ZA_RO_ROK']
X264 = X264[['rekord_id', '264']]
X300 = pbl_books[['rekord_id', 'opis_fizyczny']].drop_duplicates()
X300.columns = ['rekord_id', '300']
def opis_fiz(x):
    if pd.isnull(x):
        val = np.nan
    elif re.findall('^\[\d{4}.{0,}\],|^\[\d{4}.{0,}\]\.', x):
        val = '\\\\$a' + re.sub('(\[\d{4}.{0,}\](,|\.))(.*$)', r'\3', x).strip()
    else:
        val = '\\\\$a' + x
    return val
X300['300'] = X300['300'].apply(lambda x: opis_fiz(x))
X490 = pbl_books[['rekord_id', 'ZA_SERIA_WYDAWNICZA']].drop_duplicates()
X490.columns = ['rekord_id', '490']
X490['490'] = X490['490'].str.replace('^ {0,}\(', '', regex=True).str.replace('\)\.{0,1}$', '', regex=True).str.replace('\);{0,1} {0,1}\(', '#', regex=True).str.replace('(\d+)(\. )', r'\1\2#', regex=True)
X490['490'] = X490['490'].apply(lambda x: re.sub('(\) )([a-zA-ZÀ-ž])', r'\1(\2', x) if pd.notnull(x) and re.findall('\) [a-zA-ZÀ-ž]', x) and not re.findall('\(', x) else x).str.replace('\) \(', '#', regex=True)
X490 = cSplit(X490, 'rekord_id', '490', '#')
def seria(x):
    if pd.isnull(x):
        val = np.nan
    elif re.findall('\d', x):
        if '; ' in x:
            val = re.sub('(; {0,})(\d+)', r'|\2', x)
        else:
            val = re.sub('(, {0,})(\d+)', r'|\2', x)
    else:
        val = x
    return val
X490['490'] = X490['490'].apply(lambda x: seria(x))
X490 = cSplit(X490, 'rekord_id', '490', '|', 'wide', 1)
X490[1] = X490[1].str.replace(r'|', '; ')
def seria2(row):
    if pd.isnull(row[1]) and pd.notnull(row[0]):
        val = '0\\$a' + row[0].strip()
    elif pd.notnull(row[0]) and pd.notnull(row[1]):
        val = f"0\\$a{row[0]} ; $v{row[1]}".strip()
    else:
        val = np.nan
    return val
X490['490'] = X490.apply(lambda x: seria2(x), axis = 1)
X490 = X490[['rekord_id', '490']]
X490['490'] = X490.groupby('rekord_id')['490'].transform(lambda x: '|'.join(x.dropna().str.strip()))
X490 = X490.drop_duplicates()


# tutaj
















