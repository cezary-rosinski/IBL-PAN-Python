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
from functools import reduce
from my_functions import df_to_mrc
from my_functions import cosine_sim_2_elem

# def
def date_to_string(x):
    try:
        val = x.date().strftime('%Y%m%d')
    except ValueError:
        val = np.nan
    return val

def proper_date(row):
    if pd.isnull(row['ZA_UZYTK_WPIS_DATA']) and pd.isnull(row['005']):
        val = '19980218'
    elif pd.isnull(row['005']):
        val = row['ZA_UZYTK_WPIS_DATA']
    else:
        val = row['005']
    return val

def date_to_string2(x):
    try:
        val = x.date().strftime('%y%m%d')
    except ValueError:
        val = '19980218'
    return val

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

def clear_title(x):
    if pd.isnull(x):
        val = '[no title]'
    elif x[-1] == ']':
        val = re.sub('(?<!\.\.)(\.$)', '', re.sub('(^.*?)([\.!?] {0,1})\[.*?$', r'\1\2', x).strip())
    elif x[0] == '[':
        val = re.sub('(?<!\.\.)(\.$)', '', re.sub('(^.*?\])(.*?$)', r'\2', x).strip())
    else:
        val = x
    return val

def tworca_nazwisko_100(row):
    if row['rodzaj_zapisu'] == 'książka twórcy (podmiotowa)' and pd.notnull(row['tworca_nazwisko']) and ',' in row['tworca_nazwisko'] and row['tworca_imie'] == '*':
        val = row['autor_nazwisko']
    else:
        val = row['tworca_nazwisko']
    return val

def tworca_imie_100(row):
    if row['rodzaj_zapisu'] == 'książka twórcy (podmiotowa)' and pd.notnull(row['tworca_nazwisko']) and row['tworca_nazwisko'] == row['autor_nazwisko'] and row['tworca_imie'] == '*':
        val = row['autor_imie']
    else:
        val = row['tworca_imie']
    return val

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

def autor_245(row):
    if pd.notnull(row['autor_nazwisko']) and pd.notnull(row['autor_imie']):
        val = f"$c{row['autor_imie']} {row['autor_nazwisko']}"
    elif pd.notnull(row['autor_nazwisko']) and pd.isnull(row['autor_imie']):
        val = f"$c{row['autor_nazwisko']}"
    else:
        val = '[no author]'
    return val

def wydawnictwo(row):
    if pd.isnull(row['wydawnictwo']) and pd.isnull(row['miejscowosc']):
        val = '1\\$aS.l. : $bs.n.'
    else:
        val = f"1\\$a{row['miejscowosc']} : $b{row['wydawnictwo']}"
    return val

def opis_fiz(x):
    if pd.isnull(x):
        val = np.nan
    elif re.findall('^\[\d{4}.{0,}\],|^\[\d{4}.{0,}\]\.', x):
        val = '\\\\$a' + re.sub('(\[\d{4}.{0,}\](,|\.))(.*$)', r'\3', x).strip()
    else:
        val = '\\\\$a' + x
    return val

def seria(x):
    if pd.isnull(x):
        val = np.nan
    elif re.findall('\d', x):
        if '; ' in x:
            val = re.sub('(; {0,})(\d+)', r'❦\2', x)
        else:
            val = re.sub('(, {0,})(\d+)', r'❦\2', x)
    else:
        val = x
    return val

def seria2(row):
    if pd.isnull(row[1]) and pd.notnull(row[0]):
        val = '0\\$a' + row[0].strip()
    elif pd.notnull(row[0]) and pd.notnull(row[1]):
        val = f"0\\$a{row[0]} ; $v{row[1]}".strip()
    else:
        val = np.nan
    return val

def x600(row):
    if len(f"str({row['tworca_nazwisko']}), str({row['tworca_imie']})") == len(str(row['$a'])) and ''.join(re.findall('[A-ZÀ-Ž]', f"{row['tworca_nazwisko']}, {row['tworca_imie']}")) == ''.join(re.findall('[A-ZÀ-Ž]', row['$a'])):
        val = f"14$a{row['$a']}$d{row['$d']}$0{row['$0']}"
    elif row['tworca_imie'] != '':
        val = f"14$a{row['tworca_nazwisko']}, {row['tworca_imie']}$d{row['$d']}$0{row['$0']}"
    else:
        val = f"14$a{row['tworca_nazwisko']}"
    return val

def x650(x):
    if x['MARC_FIELD'] == '4,65':
        if pd.notnull(x['KH_NAZWA']):
            val = f"04$a{x['HP_NAZWA']}, {x['KH_NAZWA']}".strip()
        else:
            val = f"04$a{x['HP_NAZWA']}".strip()
    elif x['MARC_FIELD'] == '24,65':
        if pd.notnull(x['KH_NAZWA']):
            val = f"24$a{x['HP_NAZWA']}, {x['KH_NAZWA']}".strip()
        else:
            val = f"24$a{x['HP_NAZWA']}".strip()
    else:
        val = np.nan
    return val

def x700(x):
    if pd.isnull(x['wspoltworca_nazwisko']):
        val = np.nan
    elif pd.notnull(x['MARC_ABBREVIATION']):
        val = f"1\\$a{x['wspoltworca_nazwisko']}, {x['wspoltworca_imie']}$e{x['funkcja_osoby']}{x['MARC_ABBREVIATION']}"
    else:
        val = f"1\\$a{x['wspoltworca_nazwisko']}, {x['wspoltworca_imie']}$e{x['funkcja_osoby']}"
    return val

def book_collection(x):
    try:
        if int(x) <= 2003:
            val = '\\\\$aPBL 1989-2003: książki i czasopisma'
        else:
            val = '\\\\$aPBL 2004-2012: książki'
    except ValueError:
        val = '\\\\$aPBL 1989-2003: książki i czasopisma'
    return val

def X008_art(x):
    if pd.isnull(x):
        val = '   '
    elif len(x) == 2:
        val = x + ' '
    elif len(x) == 1:
        val = x + '  '
    else:
        val = x
    return val

def tworca_nazwisko_100_art(row):
    if row['rodzaj_zapisu'] in ["adaptacja utworów", "antologia w czasopiśmie", "listy", "proza", "tekst paraliteracki twórcy", "twórczość pozaliteracka", "wiersz"] and pd.notnull(row['tworca_nazwisko']) and ',' in row['tworca_nazwisko'] and row['tworca_imie'] == '*':
        val = row['autor_nazwisko']
    else:
        val = row['tworca_nazwisko']
    return val

def tworca_imie_100_art(row):
    if row['rodzaj_zapisu'] in ["adaptacja utworów", "antologia w czasopiśmie", "listy", "proza", "tekst paraliteracki twórcy", "twórczość pozaliteracka", "wiersz"] and pd.notnull(row['tworca_nazwisko']) and row['tworca_nazwisko'] == row['autor_nazwisko'] and row['tworca_imie'] == '*':
        val = row['autor_imie']
    else:
        val = row['tworca_imie']
    return val

def x100_art(row):
    if pd.isnull(row['tworca_nazwisko']) and row['rodzaj_zapisu'] in ["adaptacja utworów", "antologia w czasopiśmie", "listy", "proza", "tekst paraliteracki twórcy", "twórczość pozaliteracka", "wiersz"]:
        val = np.nan
    else:
        if row['rodzaj_zapisu'] in ["adaptacja utworów", "antologia w czasopiśmie", "listy", "proza", "tekst paraliteracki twórcy", "twórczość pozaliteracka", "wiersz"] and len(f"str({row['tworca_nazwisko']}), str({row['tworca_imie']})") == len(str(row['$a'])) and ''.join(re.findall('[A-ZÀ-Ž]', f"{row['tworca_nazwisko']}, {row['tworca_imie']}")) == ''.join(re.findall('[A-ZÀ-Ž]', row['$a'])):
            val = f"1\\$a{row['$a']}$d{row['$d']}$4aut$0{row['$0']}"
        else:
            if row['rodzaj_zapisu'] in ["adaptacja utworów", "antologia w czasopiśmie", "listy", "proza", "tekst paraliteracki twórcy", "twórczość pozaliteracka", "wiersz"]:
                val = f"1\\$a{row['tworca_nazwisko']}, {row['tworca_imie']}$d{row['$d']}$4aut$0{row['$0']}"
            else:
                if pd.notnull(row['autor_nazwisko']):
                    val = f"1 $a{row['autor_nazwisko']}, {row['autor_imie']}$4aut"
                else:
                    val = np.nan
    return val

def X773_art(x):
    if pd.isnull(x['rok']) and pd.isnull(x['numer']) and pd.isnull(x['strony']):
        val = f"0\\$t{x['czasopismo']}"
    elif pd.isnull(x['numer']) and pd.isnull(x['strony']):
        val = f"0\\$t{x['czasopismo']}$gR. {x['rok']}$9{x['rok']}"
    elif pd.isnull(x['strony']):
        val = f"0\\$t{x['czasopismo']}$gR. {x['rok']}, {x['numer']}$9{x['rok']}"
    elif pd.isnull(x['numer']):
        val = f"0\\$t{x['czasopismo']}$gR. {x['rok']}, {x['strony']}$9{x['rok']}"
    else:
        val = f"0\\$t{x['czasopismo']}$gR. {x['rok']}, {x['numer']}, {x['strony']}$9{x['rok']}"
    return val

# read google sheets
language_magazine = gsheet_to_df('1EKKdPL8II1l7vYivDVJCYePuswlimt1tQHgvgsScJjs', 'Export Worksheet')
full_text = gsheet_to_df('11_EA1S-4pcIoT3SxjcxYso1o842fV8_NuDmT9cXfsVY', 'Arkusz1')
pbl_viaf_names_dates = gsheet_to_df('1S_KsVppXzisd4zlsZ70rJE3d40SF9XRUO75no360dnY', 'pbl_viaf')
pbl_marc_roles = gsheet_to_df('1iQ4JGo1hLWZbQX4u2kB8LwqSWMLwicFfBsVFc9MNUUk', 'pbl-marc')
pbl_marc_roles = pbl_marc_roles[pbl_marc_roles['MARC_ABBREVIATION'] != '#N/A'][['PBL_NAZWA', 'MARC_ABBREVIATION']]
pbl_marc_roles['MARC_ABBREVIATION'] = pbl_marc_roles.groupby('PBL_NAZWA')['MARC_ABBREVIATION'].transform(lambda x: '❦'.join(x))
pbl_marc_roles = pbl_marc_roles.drop_duplicates().reset_index(drop=True)

# read excel files
pbl_subject_headings_info = pd.read_csv("pbl_subject_headings.csv", sep=';', encoding='cp1250')
language_codes = pd.read_excel('language_codes_marc.xlsx', sheet_name = 'language')

# SQL connection

dsn_tns = cx_Oracle.makedsn('pbl.ibl.poznan.pl', '1521', service_name='xe')
connection = cx_Oracle.connect(user='IBL_SELECT', password='CR333444', dsn=dsn_tns, encoding='windows-1250')

# PBL queries

pbl_books_query = """select z.za_zapis_id "rekord_id", z.za_type "typ", rz.rz_rodzaj_id "rodzaj_zapisu_id", rz.rz_nazwa "rodzaj_zapisu", dz.dz_dzial_id "dzial_id", dz.dz_nazwa "dzial", to_char(tw.tw_tworca_id) "tworca_id", tw.tw_nazwisko "tworca_nazwisko", tw.tw_imie "tworca_imie", to_char(a.am_autor_id) "autor_id", a.am_nazwisko "autor_nazwisko", a.am_imie "autor_imie", z.za_tytul "tytul", z.za_opis_wspoltworcow "wspoltworcy", fo.fo_nazwa "funkcja_osoby", to_char(os.os_osoba_id) "wspoltworca_id", os.os_nazwisko "wspoltworca_nazwisko", os.os_imie "wspoltworca_imie", z.za_adnotacje "adnotacja", z.za_adnotacje2 "adnotacja2", z.za_adnotacje3 "adnotacja3", w.wy_nazwa "wydawnictwo", w.wy_miasto "miejscowosc", z.za_rok_wydania "rok_wydania", z.za_opis_fizyczny_ksiazki "opis_fizyczny", z.za_uzytk_wpisal, z.za_ro_rok, z.za_tytul_oryginalu, z.za_wydanie, z.za_instytucja, z.za_seria_wydawnicza, z.za_te_teatr_id,z.ZA_UZYTK_WPIS_DATA,z.ZA_UZYTK_MOD_DATA
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
pbl_books_reviews_query = """select z.za_zapis_id "rec_id", a.am_nazwisko "rec_a_naz", a.am_imie "rec_a_im", z.za_tytul "rec_tyt", zrr.zrr_tytul "rec_czas_tyt", zrr.zrr_miejsce_wydania "rec_czas_miejsc", z.za_zrodlo_rok "rec_rok", z.za_zrodlo_nr "rec_nr", z.za_zrodlo_str "rec_str", z2.za_zapis_id "ks_id", a2.am_nazwisko "ks_a_naz", a2.am_imie "ks_a_im", z2.za_tytul "ks_tyt", w.wy_nazwa "ks_wyd_nazw", w.wy_miasto "ks_wyd_miejsc", z2.za_ro_rok
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
pbl_articles_query = """select z.za_zapis_id "rekord_id", z.za_type "typ", rz.rz_rodzaj_id "rodzaj_zapisu_id", rz.rz_nazwa "rodzaj_zapisu", dz.dz_dzial_id "dzial_id", dz.dz_nazwa "dzial", to_char(tw.tw_tworca_id) "tworca_id", tw.tw_nazwisko "tworca_nazwisko", tw.tw_imie "tworca_imie", to_char(a.am_autor_id) "autor_id", a.am_nazwisko "autor_nazwisko", a.am_imie "autor_imie", z.za_tytul "tytul", z.za_opis_wspoltworcow "wspoltworcy", fo.fo_nazwa "funkcja_osoby", to_char(os.os_osoba_id) "wspoltworca_id", os.os_nazwisko "wspoltworca_nazwisko", os.os_imie "wspoltworca_imie", z.za_adnotacje "adnotacja", z.za_adnotacje2 "adnotacja2", z.za_adnotacje3 "adnotacja3", to_char(zr.zr_zrodlo_id) "czasopismo_id", zr.zr_tytul "czasopismo", z.za_zrodlo_rok "rok", z.za_zrodlo_nr "numer", z.za_zrodlo_str "strony",z.za_tytul_oryginalu,z.za_te_teatr_id,z.ZA_UZYTK_WPIS_DATA,z.ZA_UZYTK_MOD_DATA,z.ZA_TYPE
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
pbl_sh2 = pbl_sh2.merge(pbl_sh1, how='left', on = ['HZ_ZA_ZAPIS_ID', 'HP_NAZWA'], indicator=True)
pbl_sh2 = pbl_sh2.loc[pbl_sh2['_merge'] == 'left_only'][['HZ_ZA_ZAPIS_ID', 'HP_NAZWA', 'KH_NAZWA_x']]
pbl_sh2 = pbl_sh2.rename(columns = {'KH_NAZWA_x': 'KH_NAZWA'})
pbl_subject_headings = pd.concat([pbl_sh1,pbl_sh2]).drop_duplicates(keep=False)
del [pbl_sh1, pbl_sh2]

pbl_books_reviews = pd.read_sql(pbl_books_reviews_query, con=connection).fillna(value = np.nan)
pbl_books_reviews['ks_tyt'] = pbl_books_reviews['ks_tyt'].apply(lambda x: clear_title(x))
pbl_books_reviews.loc[pbl_books_reviews['rec_a_naz'].isna(), 'rec_a_naz'] = '[no author]'
pbl_books_reviews.loc[pbl_books_reviews['rec_tyt'].isna(), 'rec_tyt'] = '[no title]'
pbl_books_reviews.loc[pbl_books_reviews['ks_a_naz'].isna(), 'ks_a_naz'] = '[no author]'
pbl_books_reviews['rec_autor'] = pbl_books_reviews[pbl_books_reviews.columns[1:3]].apply(lambda x: ' '.join(x.dropna().astype(str)), axis=1)
pbl_books_reviews = copy.copy(pbl_books_reviews)
pbl_books_reviews['rec_rok'] = pbl_books_reviews['rec_rok'].apply(lambda x: np.nan if math.isnan(x) else '{:4.0f}'.format(x))
pbl_books_reviews['rec_czas_miejsc'] = pbl_books_reviews[pbl_books_reviews.columns[5:7]].apply(lambda x: ' '.join(x.dropna().astype(str)), axis=1)
pbl_books_reviews['rec'] = pbl_books_reviews.iloc[:, [-1,3,4,5,7,8]].apply(lambda x: ', '.join(x.dropna().astype(str)), axis=1)
pbl_books_reviews['ks_autor'] = pbl_books_reviews[['ks_a_naz', 'ks_a_im']].apply(lambda x: ' '.join(x.dropna().astype(str)), axis=1)
pbl_books_reviews['ks_wyd_miejsc'] = pbl_books_reviews[['ks_wyd_miejsc', 'ZA_RO_ROK']].apply(lambda x: ' '.join(x.dropna().astype(str)), axis=1)
pbl_books_reviews['ks'] = pbl_books_reviews[['ks_autor', 'ks_tyt', 'ks_wyd_nazw', 'ks_wyd_miejsc']].apply(lambda x: ', '.join(x.dropna().astype(str)), axis=1)
pbl_books_reviews = pbl_books_reviews[['rec_id', 'rec', 'ks_id', 'ks']]

# PBL books

pbl_books = pd.merge(pbl_books, pbl_persons_index,  how='left', left_on = 'rekord_id', right_on = 'ODI_ZA_ZAPIS_ID') 
pbl_books = pd.merge(pbl_books, pbl_subject_headings,  how='left', left_on = 'rekord_id', right_on = 'HZ_ZA_ZAPIS_ID') 
pbl_books = pd.merge(pbl_books, pbl_viaf_names_dates,  how='left', left_on = 'tworca_id', right_on = 'pbl_id') 
pbl_books = pd.merge(pbl_books, pbl_marc_roles,  how='left', left_on = 'funkcja_osoby', right_on = 'PBL_NAZWA').sort_values('rekord_id').reset_index(drop=True)

# PBL books fields

#najpierw stworzenie tabeli, a potem iterowanie po wierszach, żeby tworzyć rekordy MARC

LDR = '-----nam---------4u-----'
X001 = pbl_books[['rekord_id']].drop_duplicates()
X001['001'] = pbl_books['rekord_id'].apply(lambda x: 'pl' + '{:09d}'.format(x))
X005 = pbl_books[['rekord_id', 'ZA_UZYTK_MOD_DATA', 'ZA_UZYTK_WPIS_DATA']].drop_duplicates()
X005.columns = ['rekord_id', '005', 'ZA_UZYTK_WPIS_DATA']
X005['005'] = X005['005'].apply(lambda x: date_to_string(x))
X005['ZA_UZYTK_WPIS_DATA'] = X005['ZA_UZYTK_WPIS_DATA'].apply(lambda x: date_to_string(x))
X005['005'] = X005.apply(lambda x: proper_date(x), axis=1)
X005 = X005[['rekord_id', '005']]
X008 = pbl_books[['rekord_id', 'ZA_UZYTK_WPIS_DATA','ZA_RO_ROK', 'tytul']].drop_duplicates()
X008['ZA_UZYTK_WPIS_DATA'] = X008['ZA_UZYTK_WPIS_DATA'].apply(lambda x: date_to_string2(x))
#czy to rozwiązanie jest okej?
X008['tytul'] = X008.apply(lambda x: clear_tytul(x), axis=1)    
X008['language'] = X008['tytul'].apply(lambda x: title_lang(x))
X008 = pd.merge(X008, language_codes[['country code', 'language code']],  how='left', left_on = 'language', right_on = 'country code')
X008 = X008.iloc[:, [0, 1, 2, 3, 6]]
X008['language code'] = X008['language code'].apply(lambda x: '  -d' if pd.isnull(x) else x)
X008['008'] = X008.apply(lambda x: f"{x['ZA_UZYTK_WPIS_DATA']}s{x['ZA_RO_ROK']}----                    {x['language code']}", axis = 1)
X008 = X008[['rekord_id', '008']]
X040 = '\\\\$aIBL$bpol'
X100 = pbl_books[['rekord_id', 'rodzaj_zapisu', 'tworca_nazwisko', 'tworca_imie', 'autor_nazwisko', 'autor_imie', '$a', '$d', '$0']].drop_duplicates()
X100['tworca_nazwisko'] = X100.apply(lambda x: tworca_nazwisko_100(x), axis=1)
X100['tworca_imie'] = X100.apply(lambda x: tworca_imie_100(x), axis=1)
X100['100'] = X100.apply(lambda x: x100(x), axis=1)     
X100 = X100[['rekord_id', '100']].drop_duplicates()
X100['100'] = X100['100'].str.replace('..nan', '')
X100['100'] = X100.groupby('rekord_id')['100'].transform(lambda x: '❦'.join(x.dropna().str.strip()))
X100 = X100.drop_duplicates().reset_index(drop=True)
X100 = pd.DataFrame(X100['100'].str.split('❦', 1).tolist(), index=X100['rekord_id'])
X100['rekord_id'] = X100.index
X100 = X100.reset_index(drop=True).fillna(value=np.nan).replace(r'^\s*$', np.nan, regex=True)
X100.columns = ['100', '700', 'rekord_id']
X240 = pbl_books[['rekord_id', 'ZA_TYTUL_ORYGINALU']].drop_duplicates()
X240.columns = ['rekord_id', '240']
X240['240'] = '\\\\$a' + X240['240'].str.replace('^(.*?\])(.*$)',r'\1', regex=True).str.replace('^\[|\]$', '', regex=True)
X245 = pbl_books[['rekord_id', 'autor_nazwisko', 'autor_imie', 'tytul', 'wspoltworcy', 'ZA_INSTYTUCJA']].drop_duplicates()    
X245['tytul_ok'] = X245.apply(lambda x: '10$a' + clear_tytul(x), axis=1)  
X245['autor'] = X245.apply(lambda x: autor_245(x), axis=1)
X245 = X245[['rekord_id', 'tytul_ok', 'autor', 'wspoltworcy', 'ZA_INSTYTUCJA']]
X245['tytul_ok'] = X245.groupby('rekord_id')['tytul_ok'].transform(lambda x: '❦'.join(x.drop_duplicates().dropna().str.strip()))
X245['autor'] = X245.groupby('rekord_id')['autor'].transform(lambda x: '❦'.join(x.drop_duplicates().dropna().str.strip()))
X245['autor'] = X245['autor'].str.replace('\❦\$c', ', ', regex=True)
X245['wspoltworcy'] = X245.groupby('rekord_id')['wspoltworcy'].transform(lambda x: '❦'.join(x.drop_duplicates().dropna().str.strip()))
X245['ZA_INSTYTUCJA'] = X245.groupby('rekord_id')['ZA_INSTYTUCJA'].transform(lambda x: '❦'.join(x.drop_duplicates().dropna().str.strip()))
X245['245'] = X245.apply(lambda x: f"{x['tytul_ok']} /{x['autor']} ; {x['wspoltworcy']} ; {x['ZA_INSTYTUCJA']}.", axis=1)
X245['245'] = X245['245'].apply(lambda x: re.sub('( ;  ; )(\.)|( ;   ; )(\.)|( ; )(\.)|', r'\2', x)).apply(lambda x: re.sub('^\.$', '', x))
X245 = X245[['rekord_id', '245']].drop_duplicates()
X250 = pbl_books[['rekord_id', 'ZA_WYDANIE']].drop_duplicates()
X250.columns = ['rekord_id', '250']
X250['250'] = X250['250'].apply(lambda x: '\\\\$a' + x if pd.notnull(x) else np.nan)
X264 = pbl_books[['rekord_id', 'wydawnictwo', 'miejscowosc', 'ZA_RO_ROK']].drop_duplicates()
X264['264'] = X264.apply(lambda x: wydawnictwo(x), axis=1)
X264 = X264[['rekord_id', '264', 'ZA_RO_ROK']]
X264['264'] = X264.groupby('rekord_id')['264'].transform(lambda x: '; '.join(x.dropna().str.strip()))
X264['ZA_RO_ROK'] = X264.groupby('rekord_id')['ZA_RO_ROK'].transform(lambda x: '❦'.join(x.drop_duplicates().dropna().astype(str).str.strip()))
X264 = X264.drop_duplicates()
X264['264'] = X264['264'] + ',$c' + X264['ZA_RO_ROK']
X264 = X264[['rekord_id', '264']]
X300 = pbl_books[['rekord_id', 'opis_fizyczny']].drop_duplicates()
X300.columns = ['rekord_id', '300']
X300['300'] = X300['300'].apply(lambda x: opis_fiz(x))
X490 = pbl_books[['rekord_id', 'ZA_SERIA_WYDAWNICZA']].drop_duplicates()
X490.columns = ['rekord_id', '490']
X490['490'] = X490['490'].str.replace('^ {0,}\(', '', regex=True).str.replace('\)\.{0,1}$', '', regex=True).str.replace('\);{0,1} {0,1}\(', '#', regex=True).str.replace('(\d+)(\. )', r'\1\2#', regex=True)
X490['490'] = X490['490'].apply(lambda x: re.sub('(\) )([a-zA-ZÀ-ž])', r'\1(\2', x) if pd.notnull(x) and re.findall('\) [a-zA-ZÀ-ž]', x) and not re.findall('\(', x) else x).str.replace('\) \(', '#', regex=True)
X490 = cSplit(X490, 'rekord_id', '490', '#')
X490['490'] = X490['490'].apply(lambda x: seria(x))
X490 = cSplit(X490, 'rekord_id', '490', '❦', 'wide', 1)
X490[1] = X490[1].str.replace(r'❦', '; ')
X490['490'] = X490.apply(lambda x: seria2(x), axis = 1)
X490 = X490[['rekord_id', '490']]
X490['490'] = X490.groupby('rekord_id')['490'].transform(lambda x: '❦'.join(x.dropna().str.strip()))
X490 = X490.drop_duplicates()
X520 = pbl_books[['rekord_id', 'adnotacja', 'adnotacja2', 'adnotacja3']].drop_duplicates()
X520['adnotacja'] = X520[X520.columns[1:]].apply(lambda x: ' '.join(x.dropna().astype(str).str.strip()), axis=1).str.replace(' +', ' ')
X520 = X520[['rekord_id', 'adnotacja']]
X520['adnotacja'] = X520['adnotacja'].apply(lambda x: '2\\$a' + re.sub('\n', '', re.sub(' +', ' ', x)) if x != '' else np.nan)
X520.columns = ['rekord_id', '520']
X590 = pbl_books[['rekord_id', 'rodzaj_zapisu']].drop_duplicates()
X590.columns = ['rekord_id', '590']
X590['590'] = X590['590'].apply(lambda x: f"\\\\$a{x}")
X600 = pbl_books[['rekord_id', 'rodzaj_zapisu', 'tworca_nazwisko', 'tworca_imie', '$a', '$d', '$0']].drop_duplicates()
X600 = X600.loc[(X600['rodzaj_zapisu'] == 'książka o twórcy (przedmiotowa)') &
                (X600['tworca_nazwisko'].notnull())]
X600['tworca_nazwisko'] = X600.apply(lambda x: x['tworca_nazwisko'].replace(',', '❦') if pd.notnull(x['tworca_nazwisko']) and ',' in x['tworca_nazwisko'] and x['tworca_imie'] == '*' else x['tworca_nazwisko'], axis=1)
X600['tworca_imie'] = X600.apply(lambda x: '' if pd.notnull(x['tworca_nazwisko']) and x['tworca_imie'] == '*' else x['tworca_imie'], axis=1)
X600 = cSplit(X600, 'rekord_id', 'tworca_nazwisko', '❦')
for column in X600:
    X600[column] = X600[column].apply(lambda x: x.strip() if isinstance(x, str) else x)
X600['600'] = X600.apply(lambda x: x600(x), axis=1)
X600 = X600[['rekord_id', '600']].drop_duplicates()
X600['600'] = X600.groupby('rekord_id')['600'].transform(lambda x: '❦'.join(x))
X600 = X600.drop_duplicates()
X600 = pd.merge(X600, pbl_books[['rekord_id']].drop_duplicates(),  how='outer', left_on = 'rekord_id', right_on = 'rekord_id').sort_values('rekord_id').reset_index(drop=True)
X610 = pbl_books[['rekord_id', 'HP_NAZWA', 'KH_NAZWA']].drop_duplicates()
pbl_sh_test = pbl_subject_headings_info.loc[pbl_subject_headings_info['MARC_FIELD'] == '24,61']
X610 = pd.merge(X610, pbl_sh_test,  how='inner', on = ['HP_NAZWA', 'KH_NAZWA']).reset_index(drop=True)
X610['610'] = X610.apply(lambda x: f"24$a{x['HP_NAZWA']}, {x['KH_NAZWA']}" if pd.notnull(x['KH_NAZWA']) else f"24$a{x['HP_NAZWA']}", axis=1)
X610 = X610[['rekord_id', '610']]
X610['610'] = X610.groupby('rekord_id')['610'].transform(lambda x: '❦'.join(x))
X610 = X610.drop_duplicates()
X610 = pd.merge(X610, pbl_books[['rekord_id']].drop_duplicates(),  how='outer', on = 'rekord_id').sort_values('rekord_id').reset_index(drop=True)
X611 = pbl_books[['rekord_id', 'HP_NAZWA', 'KH_NAZWA']].drop_duplicates()
pbl_sh_test = pbl_subject_headings_info.loc[pbl_subject_headings_info['MARC_FIELD'] == '24,611']
X611 = pd.merge(X611, pbl_sh_test,  how='inner', on = ['HP_NAZWA', 'KH_NAZWA']).reset_index(drop=True)
X611['611'] = X611.apply(lambda x: f"24$a{x['HP_NAZWA']}, {x['KH_NAZWA']}" if pd.notnull(x['KH_NAZWA']) else f"24$a{x['HP_NAZWA']}", axis=1)
X611 = X611[['rekord_id', '611']]
X611['611'] = X611.groupby('rekord_id')['611'].transform(lambda x: '❦'.join(x))
X611 = X611.drop_duplicates()
X611 = pd.merge(X611, pbl_books[['rekord_id']].drop_duplicates(),  how='outer', on = 'rekord_id').sort_values('rekord_id').reset_index(drop=True)
X630a = pbl_books[['rekord_id', 'HP_NAZWA', 'KH_NAZWA']].drop_duplicates()
pbl_sh_test = pbl_subject_headings_info.loc[pbl_subject_headings_info['MARC_FIELD'] == '4,63']
X630a = pd.merge(X630a, pbl_sh_test,  how='inner', on = ['HP_NAZWA', 'KH_NAZWA']).reset_index(drop=True)
X630a['630'] = X630a.apply(lambda x: f"\\4$a{x['HP_NAZWA']}, {x['KH_NAZWA']}" if pd.notnull(x['KH_NAZWA']) else f"\\4$a{x['HP_NAZWA']}", axis=1)
X630a = X630a[['rekord_id', '630']]
X630a['630'] = X630a.groupby('rekord_id')['630'].transform(lambda x: '❦'.join(x))
X630a = X630a.drop_duplicates()
X630b = pbl_books[['rekord_id', 'dzial']].drop_duplicates()
X630b['dzial'] = X630b['dzial'].str.replace('(.*?)( - [A-ZÀ-Ž]$)', r'\1', regex=True)
pbl_sh_test = pbl_subject_headings_info.loc[pbl_subject_headings_info['MARC_FIELD'] == '4,63'][['HP_NAZWA', 'MARC_FIELD']]
X630b = pd.merge(X630b, pbl_sh_test,  how='inner', left_on = 'dzial', right_on = 'HP_NAZWA').reset_index(drop=True)
X630b['630'] = '\\4$a' + X630b['dzial']
X630b = X630b[['rekord_id', '630']]
X630 = pd.concat([X630a, X630b])
del [X630a, X630b]
X630['630'] = X630.groupby('rekord_id')['630'].transform(lambda x: '❦'.join(x.drop_duplicates()))
X630 = X630.drop_duplicates()
X630 = pd.merge(X630, pbl_books[['rekord_id']].drop_duplicates(),  how='outer', on = 'rekord_id').sort_values('rekord_id').reset_index(drop=True)
X650a = pbl_books[['rekord_id', 'HP_NAZWA', 'KH_NAZWA']].drop_duplicates()
pbl_sh_test = pbl_subject_headings_info.loc[pbl_subject_headings_info['MARC_FIELD'].isin(['4,65', '24,65'])]
X650a = pd.merge(X650a, pbl_sh_test,  how='inner', on = ['HP_NAZWA', 'KH_NAZWA']).reset_index(drop=True)
X650a['650'] = X650a.apply(lambda x: x650(x), axis=1)
X650a = X650a[['rekord_id', '650']]
X650a['650'] = X650a.groupby('rekord_id')['650'].transform(lambda x: '❦'.join(x))
X650a = X650a.drop_duplicates()
X650b = pbl_books[['rekord_id', 'dzial']].drop_duplicates()
X650b['dzial'] = X650b['dzial'].str.replace('(.*?)( - [A-ZA-Ž]$)', r'\1', regex=True)
pbl_sh_test = pbl_subject_headings_info.loc[pbl_subject_headings_info['MARC_FIELD'] == '4,65'][['HP_NAZWA', 'MARC_FIELD']]
X650b = pd.merge(X650b, pbl_sh_test,  how='inner', left_on = 'dzial', right_on = 'HP_NAZWA').reset_index(drop=True)
X650b['650'] = '04$a' + X650b['dzial']
X650b = X650b[['rekord_id', '650']]
X650b['650'] = X650b.groupby('rekord_id')['650'].transform(lambda x: '❦'.join(x))
X650b = X650b.drop_duplicates()
X650 = pd.concat([X650a, X650b])
del [X650a, X650b]
X650['650'] = X650.groupby('rekord_id')['650'].transform(lambda x: '❦'.join(x.drop_duplicates()))
X650 = X650.drop_duplicates()
X650 = pd.merge(X650, pbl_books[['rekord_id']].drop_duplicates(),  how='outer', on = 'rekord_id').sort_values('rekord_id').reset_index(drop=True)
X651 = pbl_books[['rekord_id', 'HP_NAZWA', 'KH_NAZWA']].drop_duplicates()
pbl_sh_test = pbl_subject_headings_info.loc[pbl_subject_headings_info['MARC_FIELD'] == '4,651']
X651 = pd.merge(X651, pbl_sh_test,  how='inner', on = ['HP_NAZWA', 'KH_NAZWA']).reset_index(drop=True)
X651['651'] = X651.apply(lambda x: f"\\4$a{x['HP_NAZWA']}, {x['KH_NAZWA']}" if pd.notnull(x['KH_NAZWA']) else f"\\4$a{x['HP_NAZWA']}", axis=1)
X651 = X651[['rekord_id', '651']]
X651['651'] = X651.groupby('rekord_id')['651'].transform(lambda x: '❦'.join(x))
X651 = X651.drop_duplicates()
X651 = pd.merge(X651, pbl_books[['rekord_id']].drop_duplicates(),  how='outer', on = 'rekord_id').sort_values('rekord_id').reset_index(drop=True)
X655 = pbl_books[['rekord_id', 'tytul']].drop_duplicates()
X655['655'] = X655['tytul'].apply(lambda x: '❦'.join(re.findall('(?<!^.)(?<=\[)(.*?)(?=\])', x)).strip() if pd.notnull(x) else np.nan)
X655 = X655[['rekord_id', '655']]
X655 = cSplit(X655, 'rekord_id', '655', '❦')
X655['655'] = X655['655'].apply(lambda x: f"\\4$a{x}" if pd.notnull(x) else np.nan)
X655['655'] = X655.groupby('rekord_id')['655'].transform(lambda x: '❦'.join(x.dropna()))
X655 = X655.drop_duplicates().reset_index(drop=True).replace(r'^\s*$', np.nan, regex=True)
X700 = pbl_books[['rekord_id', 'funkcja_osoby', 'wspoltworca_nazwisko', 'wspoltworca_imie', 'MARC_ABBREVIATION']].drop_duplicates()
X700['MARC_ABBREVIATION'] = X700['MARC_ABBREVIATION'].apply(lambda x: re.sub('(^|(?<=\❦))', r'$4', x) if pd.notnull(x) else np. nan).str.replace('❦', '')
X700['700'] = X700.apply(lambda x: x700(x), axis=1)
X700 = X700[['rekord_id', '700']]
from100 = X100.loc[X100['700'].notnull()][['rekord_id', '700']]
from100 = cSplit(from100, 'rekord_id', '700', '❦')
X700 = X700.append(from100, ignore_index=True).sort_values('rekord_id').reset_index(drop=True)
X700['700'] = X700.groupby('rekord_id')['700'].transform(lambda x: '❦'.join(x.dropna().str.strip()))
X700 = X700.drop_duplicates().reset_index(drop=True).replace(r'^\s*$', np.nan, regex=True)
X100 = X100[['rekord_id', '100']]
X710 = pbl_books[['rekord_id', 'ZA_INSTYTUCJA']].drop_duplicates().reset_index(drop=True)
X710.columns = ['rekord_id', '710']
X710['710'] = X710['710'].apply(lambda x: f"24$a{x}$4isp" if pd.notnull(x) else np.nan)
X787 = pbl_books[['rekord_id', 'HP_NAZWA', 'KH_NAZWA']].drop_duplicates()
pbl_sh_test = pbl_subject_headings_info.loc[pbl_subject_headings_info['MARC_FIELD'] == '787']
X787 = pd.merge(X787, pbl_sh_test,  how='inner', on = ['HP_NAZWA', 'KH_NAZWA']).reset_index(drop=True)
X787['787'] = X787.apply(lambda x: f"{x['HP_NAZWA']}, {x['KH_NAZWA']}" if pd.notnull(x['KH_NAZWA']) else f"{x['HP_NAZWA']}", axis=1)
X787 = X787[['rekord_id', '787']]
X787['787'] = X787['787'].apply(lambda x: f"\\\\$a{x}")
X787['787'] = X787.groupby('rekord_id')['787'].transform(lambda x: '❦'.join(x))
X787 = X787.drop_duplicates()
X787 = pd.merge(X787, pbl_books[['rekord_id']].drop_duplicates(),  how='outer', on = 'rekord_id').sort_values('rekord_id').reset_index(drop=True)
X856 = pbl_books_reviews[['ks_id', 'rec_id', 'rec']]
X856.columns = ['rekord_id', 'rec_id', 'rec']
X856['856'] = X856.apply(lambda x: f"42$uhttp://libri.ucl.cas.cz/Record/pl{'{:09d}'.format(x['rec_id'])}$yreview: {x['rec']}$4N", axis=1)
X856 = X856[['rekord_id', '856']]
X856['856'] = X856.groupby('rekord_id')['856'].transform(lambda x: '❦'.join(x))
X856 = X856.drop_duplicates()
X856 = pd.merge(X856, pbl_books[['rekord_id']].drop_duplicates(),  how='right', on = 'rekord_id').sort_values('rekord_id').reset_index(drop=True)
X995 = pbl_books[['rekord_id', 'ZA_RO_ROK']].drop_duplicates().reset_index(drop=True)
X995['995'] = X995['ZA_RO_ROK'].apply(lambda x: book_collection(x))
X995 = X995[['rekord_id', '995']]
dfs = [X001, X005, X008, X100, X240, X245, X250, X264, X300, X490, X520, X590, X600, X610, X611, X630, X650, X651, X655, X700, X787, X856, X995]
pbl_marc_books = reduce(lambda left,right: pd.merge(left,right,on='rekord_id'), dfs)
pbl_marc_books['LDR'] = LDR
pbl_marc_books['040'] = X040
del pbl_marc_books['rekord_id']
columns_list = pbl_marc_books.columns.tolist()
columns_list.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
pbl_marc_books = pbl_marc_books.reindex(columns=columns_list)

pbl_marc_books.to_excel('pbl_marc_books.xlsx', index=False)
# pbl_marc_books = pd.read_excel('pbl_marc_books.xlsx')

df_to_mrc(pbl_marc_books, '❦', 'pbl_marc_books.mrc')

# PBL articles

pbl_articles = pd.merge(pbl_articles, pbl_persons_index,  how='left', left_on = 'rekord_id', right_on = 'ODI_ZA_ZAPIS_ID') 
pbl_articles = pd.merge(pbl_articles, pbl_subject_headings,  how='left', left_on = 'rekord_id', right_on = 'HZ_ZA_ZAPIS_ID') 
pbl_articles = pd.merge(pbl_articles, pbl_viaf_names_dates,  how='left', left_on = 'tworca_id', right_on = 'pbl_id') 
pbl_articles = pd.merge(pbl_articles, pbl_marc_roles,  how='left', left_on = 'funkcja_osoby', right_on = 'PBL_NAZWA').sort_values('rekord_id').reset_index(drop=True)

# PBL articles fields

LDR = '-----nab---------4u-----'
X001 = pbl_articles[['rekord_id']].drop_duplicates()
X001['001'] = pbl_articles['rekord_id'].apply(lambda x: 'pl' + '{:09d}'.format(x))
X005 = pbl_articles[['rekord_id', 'ZA_UZYTK_MOD_DATA', 'ZA_UZYTK_WPIS_DATA']].drop_duplicates()
X005.columns = ['rekord_id', '005', 'ZA_UZYTK_WPIS_DATA']
X005['005'] = X005['005'].apply(lambda x: date_to_string(x))
X005['ZA_UZYTK_WPIS_DATA'] = X005['ZA_UZYTK_WPIS_DATA'].apply(lambda x: date_to_string(x))
X005['005'] = X005.apply(lambda x: proper_date(x), axis=1)
X005 = X005[['rekord_id', '005']]
X008 = pbl_articles[['rekord_id', 'ZA_UZYTK_WPIS_DATA', 'czasopismo_id', 'rok']].drop_duplicates()
X008['rok'] = X008['rok'].apply(lambda x: '{:4.0f}'.format(x))
X008['ZA_UZYTK_WPIS_DATA'] = X008['ZA_UZYTK_WPIS_DATA'].apply(lambda x: date_to_string2(x))
X008 = pd.merge(X008, language_magazine[['ZR_ZRODLO_ID', 'country_code', 'language_code']], how='left', left_on='czasopismo_id', right_on='ZR_ZRODLO_ID')
X008['country_code'] = X008['country_code'].apply(lambda x: X008_art(x))
X008['language_code'] = X008['language_code'].apply(lambda x: X008_art(x))
X008['008'] = X008.apply(lambda x: f"{x['ZA_UZYTK_WPIS_DATA']}s{x['rok']}----{x['country_code']}                 {x['language_code']}  ", axis = 1)
X008 = X008[['rekord_id', '008']].drop_duplicates().reset_index(drop=True)
X040 = '\\\\$aIBL$bpol'
X100 = pbl_articles[['rekord_id', 'rodzaj_zapisu', 'tworca_nazwisko', 'tworca_imie', 'autor_nazwisko', 'autor_imie', '$a', '$d', '$0']].drop_duplicates() 
X100['tworca_nazwisko'] = X100.apply(lambda x: tworca_nazwisko_100_art(x), axis=1)
X100['tworca_imie'] = X100.apply(lambda x: tworca_imie_100_art(x), axis=1)
X100['100'] = X100.apply(lambda x: x100_art(x), axis=1)    
X100 = X100[['rekord_id', '100']].drop_duplicates()
X100['100'] = X100['100'].str.replace('..nan', '')
X100['100'] = X100.groupby('rekord_id')['100'].transform(lambda x: '❦'.join(x.dropna().str.strip()))
X100 = X100.drop_duplicates().reset_index(drop=True)
X100 = pd.DataFrame(X100['100'].str.split('❦', 1).tolist(), index=X100['rekord_id'])
X100['rekord_id'] = X100.index
X100 = X100.reset_index(drop=True).fillna(value=np.nan).replace(r'^\s*$', np.nan, regex=True)
X100.columns = ['100', '700', 'rekord_id']
X240 = pbl_articles[['rekord_id', 'ZA_TYTUL_ORYGINALU']].drop_duplicates()
X240.columns = ['rekord_id', '240']
X240['240'] = '\\\\$a' + X240['240'].str.replace('^(.*?\])(.*$)',r'\1', regex=True).str.replace('^\[|\]$', '', regex=True)
X245 = pbl_articles[['rekord_id', 'autor_nazwisko', 'autor_imie', 'tytul', 'wspoltworcy']].drop_duplicates()    
X245['tytul_ok'] = X245.apply(lambda x: '10$a' + clear_tytul(x), axis=1)  
X245['autor'] = X245.apply(lambda x: autor_245(x), axis=1)
X245 = X245[['rekord_id', 'tytul_ok', 'autor', 'wspoltworcy']]
X245['tytul_ok'] = X245.groupby('rekord_id')['tytul_ok'].transform(lambda x: '❦'.join(x.drop_duplicates().dropna().str.strip()))
X245['autor'] = X245.groupby('rekord_id')['autor'].transform(lambda x: '❦'.join(x.drop_duplicates().dropna().str.strip()))
X245['autor'] = X245['autor'].str.replace('\❦\$c', ', ', regex=True)
X245['wspoltworcy'] = X245.groupby('rekord_id')['wspoltworcy'].transform(lambda x: '❦'.join(x.drop_duplicates().dropna().str.strip()))
X245['245'] = X245.apply(lambda x: f"{x['tytul_ok']} /{x['autor']} ; {x['wspoltworcy']}.", axis=1)
X245['245'] = X245['245'].apply(lambda x: re.sub('( ;  ; )(\.)|( ;   ; )(\.)|( ; )(\.)|', r'\2', x)).apply(lambda x: re.sub('^\.$', '', x))
X245 = X245[['rekord_id', '245']].drop_duplicates()
X520 = pbl_articles[['rekord_id', 'adnotacja', 'adnotacja2', 'adnotacja3']].drop_duplicates()
X520['adnotacja'] = X520[X520.columns[1:]].apply(lambda x: ' '.join(x.dropna().astype(str).str.strip()), axis=1).str.replace(' +', ' ')
X520 = X520[['rekord_id', 'adnotacja']]
X520['adnotacja'] = X520['adnotacja'].apply(lambda x: '2\\$a' + re.sub('\n', '', re.sub(' +', ' ', x)) if x != '' else np.nan)
X520.columns = ['rekord_id', '520']
X600 = pbl_articles[['rekord_id', 'rodzaj_zapisu', 'tworca_nazwisko', 'tworca_imie', '$a', '$d', '$0']].drop_duplicates()
X600 = X600.loc[(~X600['rodzaj_zapisu'].isin(["adaptacja utworów", "antologia w czasopiśmie", "listy", "proza", "tekst paraliteracki twórcy", "twórczość pozaliteracka", "wiersz"])) &
                (X600['tworca_nazwisko'].notnull())]
X600['tworca_nazwisko'] = X600.apply(lambda x: x['tworca_nazwisko'].replace(',', '❦') if pd.notnull(x['tworca_nazwisko']) and ',' in x['tworca_nazwisko'] and x['tworca_imie'] == '*' else x['tworca_nazwisko'], axis=1)
X600['tworca_imie'] = X600.apply(lambda x: '' if pd.notnull(x['tworca_nazwisko']) and x['tworca_imie'] == '*' else x['tworca_imie'], axis=1)
X600 = cSplit(X600, 'rekord_id', 'tworca_nazwisko', '❦')
for column in X600:
    X600[column] = X600[column].apply(lambda x: x.strip() if isinstance(x, str) else x)
X600['600'] = X600.apply(lambda x: x600(x), axis=1)
X600 = X600[['rekord_id', '600']].drop_duplicates()
X600['600'] = X600.groupby('rekord_id')['600'].transform(lambda x: '❦'.join(x))
X600 = X600.drop_duplicates()
X600 = pd.merge(X600, pbl_articles[['rekord_id']].drop_duplicates(),  how='outer', left_on = 'rekord_id', right_on = 'rekord_id').sort_values('rekord_id').reset_index(drop=True)
X610 = pbl_articles[['rekord_id', 'HP_NAZWA', 'KH_NAZWA']].drop_duplicates()
pbl_sh_test = pbl_subject_headings_info.loc[pbl_subject_headings_info['MARC_FIELD'] == '24,61']
X610 = pd.merge(X610, pbl_sh_test,  how='inner', on = ['HP_NAZWA', 'KH_NAZWA']).reset_index(drop=True)
X610['610'] = X610.apply(lambda x: f"24$a{x['HP_NAZWA']}, {x['KH_NAZWA']}" if pd.notnull(x['KH_NAZWA']) else f"24$a{x['HP_NAZWA']}", axis=1)
X610 = X610[['rekord_id', '610']]
X610['610'] = X610.groupby('rekord_id')['610'].transform(lambda x: '❦'.join(x))
X610 = X610.drop_duplicates()
X610 = pd.merge(X610, pbl_articles[['rekord_id']].drop_duplicates(),  how='outer', on = 'rekord_id').sort_values('rekord_id').reset_index(drop=True)
X611 = pbl_articles[['rekord_id', 'HP_NAZWA', 'KH_NAZWA']].drop_duplicates()
pbl_sh_test = pbl_subject_headings_info.loc[pbl_subject_headings_info['MARC_FIELD'] == '24,611']
X611 = pd.merge(X611, pbl_sh_test,  how='inner', on = ['HP_NAZWA', 'KH_NAZWA']).reset_index(drop=True)
X611['611'] = X611.apply(lambda x: f"24$a{x['HP_NAZWA']}, {x['KH_NAZWA']}" if pd.notnull(x['KH_NAZWA']) else f"24$a{x['HP_NAZWA']}", axis=1)
X611 = X611[['rekord_id', '611']]
X611['611'] = X611.groupby('rekord_id')['611'].transform(lambda x: '❦'.join(x))
X611 = X611.drop_duplicates()
X611 = pd.merge(X611, pbl_articles[['rekord_id']].drop_duplicates(),  how='outer', on = 'rekord_id').sort_values('rekord_id').reset_index(drop=True)
X630a = pbl_articles[['rekord_id', 'HP_NAZWA', 'KH_NAZWA']].drop_duplicates()
pbl_sh_test = pbl_subject_headings_info.loc[pbl_subject_headings_info['MARC_FIELD'] == '4,63']
X630a = pd.merge(X630a, pbl_sh_test,  how='inner', on = ['HP_NAZWA', 'KH_NAZWA']).reset_index(drop=True)
X630a['630'] = X630a.apply(lambda x: f"\\4$a{x['HP_NAZWA']}, {x['KH_NAZWA']}" if pd.notnull(x['KH_NAZWA']) else f"\\4$a{x['HP_NAZWA']}", axis=1)
X630a = X630a[['rekord_id', '630']]
X630a['630'] = X630a.groupby('rekord_id')['630'].transform(lambda x: '❦'.join(x))
X630a = X630a.drop_duplicates()
X630b = pbl_articles[['rekord_id', 'dzial']].drop_duplicates()
X630b['dzial'] = X630b['dzial'].str.replace('(.*?)( - [A-ZÀ-Ž]$)', r'\1', regex=True)
pbl_sh_test = pbl_subject_headings_info.loc[pbl_subject_headings_info['MARC_FIELD'] == '4,63'][['HP_NAZWA', 'MARC_FIELD']]
X630b = pd.merge(X630b, pbl_sh_test,  how='inner', left_on = 'dzial', right_on = 'HP_NAZWA').reset_index(drop=True)
X630b['630'] = '\\4$a' + X630b['dzial']
X630b = X630b[['rekord_id', '630']]
X630 = pd.concat([X630a, X630b])
del [X630a, X630b]
X630['630'] = X630.groupby('rekord_id')['630'].transform(lambda x: '❦'.join(x.drop_duplicates()))
X630 = X630.drop_duplicates()
X630 = pd.merge(X630, pbl_articles[['rekord_id']].drop_duplicates(),  how='outer', on = 'rekord_id').sort_values('rekord_id').reset_index(drop=True)
X650a = pbl_articles[['rekord_id', 'HP_NAZWA', 'KH_NAZWA']].drop_duplicates()
pbl_sh_test = pbl_subject_headings_info.loc[pbl_subject_headings_info['MARC_FIELD'].isin(['4,65', '24,65'])]
X650a = pd.merge(X650a, pbl_sh_test,  how='inner', on = ['HP_NAZWA', 'KH_NAZWA']).reset_index(drop=True)
X650a['650'] = X650a.apply(lambda x: x650(x), axis=1)
X650a = X650a[['rekord_id', '650']]
X650a['650'] = X650a.groupby('rekord_id')['650'].transform(lambda x: '❦'.join(x))
X650a = X650a.drop_duplicates()
X650b = pbl_articles[['rekord_id', 'dzial']].drop_duplicates()
X650b['dzial'] = X650b['dzial'].str.replace('(.*?)( - [A-ZA-Ž]$)', r'\1', regex=True)
pbl_sh_test = pbl_subject_headings_info.loc[pbl_subject_headings_info['MARC_FIELD'] == '4,65'][['HP_NAZWA', 'MARC_FIELD']]
X650b = pd.merge(X650b, pbl_sh_test,  how='inner', left_on = 'dzial', right_on = 'HP_NAZWA').reset_index(drop=True)
X650b['650'] = '04$a' + X650b['dzial']
X650b = X650b[['rekord_id', '650']]
X650b['650'] = X650b.groupby('rekord_id')['650'].transform(lambda x: '❦'.join(x))
X650b = X650b.drop_duplicates()
X650 = pd.concat([X650a, X650b])
del [X650a, X650b]
X650['650'] = X650.groupby('rekord_id')['650'].transform(lambda x: '❦'.join(x.drop_duplicates()))
X650 = X650.drop_duplicates()
X650 = pd.merge(X650, pbl_articles[['rekord_id']].drop_duplicates(),  how='outer', on = 'rekord_id').sort_values('rekord_id').reset_index(drop=True)
X651 = pbl_articles[['rekord_id', 'HP_NAZWA', 'KH_NAZWA']].drop_duplicates()
pbl_sh_test = pbl_subject_headings_info.loc[pbl_subject_headings_info['MARC_FIELD'] == '4,651']
X651 = pd.merge(X651, pbl_sh_test,  how='inner', on = ['HP_NAZWA', 'KH_NAZWA']).reset_index(drop=True)
X651['651'] = X651.apply(lambda x: f"\\4$a{x['HP_NAZWA']}, {x['KH_NAZWA']}" if pd.notnull(x['KH_NAZWA']) else f"\\4$a{x['HP_NAZWA']}", axis=1)
X651 = X651[['rekord_id', '651']]
X651['651'] = X651.groupby('rekord_id')['651'].transform(lambda x: '❦'.join(x))
X651 = X651.drop_duplicates()
X651 = pd.merge(X651, pbl_articles[['rekord_id']].drop_duplicates(),  how='outer', on = 'rekord_id').sort_values('rekord_id').reset_index(drop=True)
X655 = pbl_articles[['rekord_id', 'rodzaj_zapisu']].drop_duplicates()
X655.columns = ['rekord_id', '655']
X655['655'] = X655['655'].apply(lambda x: f"\\4$a{x.capitalize()}")
X700 = pbl_articles[['rekord_id', 'funkcja_osoby', 'wspoltworca_nazwisko', 'wspoltworca_imie', 'MARC_ABBREVIATION']].drop_duplicates()
X700['MARC_ABBREVIATION'] = X700['MARC_ABBREVIATION'].apply(lambda x: re.sub('(^|(?<=\❦))', r'$4', x) if pd.notnull(x) else np. nan).str.replace('❦', '')
X700['700'] = X700.apply(lambda x: x700(x), axis=1)
X700 = X700[['rekord_id', '700']]
from100 = X100.loc[X100['700'].notnull()][['rekord_id', '700']]
from100 = cSplit(from100, 'rekord_id', '700', '❦')
X700 = X700.append(from100, ignore_index=True).sort_values('rekord_id').reset_index(drop=True)
X700['700'] = X700.groupby('rekord_id')['700'].transform(lambda x: '❦'.join(x.dropna().str.strip()))
X700 = X700.drop_duplicates().reset_index(drop=True).replace(r'^\s*$', np.nan, regex=True)
X100 = X100[['rekord_id', '100']]
X773 = pbl_articles[['rekord_id', 'czasopismo', 'rok', 'numer', 'strony']].drop_duplicates()
X773['rok'] = X773['rok'].apply(lambda x: '{:4.0f}'.format(x))  
X773['773'] = X773.apply(lambda x: X773_art(x), axis=1)
X773 = X773[['rekord_id', '773']]
X787 = pbl_articles[['rekord_id', 'HP_NAZWA', 'KH_NAZWA']].drop_duplicates()
pbl_sh_test = pbl_subject_headings_info.loc[pbl_subject_headings_info['MARC_FIELD'] == '787']
X787 = pd.merge(X787, pbl_sh_test,  how='inner', on = ['HP_NAZWA', 'KH_NAZWA']).reset_index(drop=True)
X787['787'] = X787.apply(lambda x: f"{x['HP_NAZWA']}, {x['KH_NAZWA']}" if pd.notnull(x['KH_NAZWA']) else f"{x['HP_NAZWA']}", axis=1)
X787 = X787[['rekord_id', '787']]
X787['787'] = X787['787'].apply(lambda x: f"\\\\$a{x}")
X787['787'] = X787.groupby('rekord_id')['787'].transform(lambda x: '❦'.join(x))
X787 = X787.drop_duplicates()
X787 = pd.merge(X787, pbl_articles[['rekord_id']].drop_duplicates(),  how='outer', on = 'rekord_id').sort_values('rekord_id').reset_index(drop=True)
X856 = pbl_books_reviews[['rec_id', 'ks_id', 'ks']]
X856.columns = ['rekord_id', 'ks_id', 'ks']
X856['856'] = X856.apply(lambda x: f"42$uhttp://libri.ucl.cas.cz/Record/pl{'{:09d}'.format(x['ks_id'])}$yreview of: {x['ks']}$4N", axis=1)
X856 = X856[['rekord_id', '856']]
X856['856'] = X856.groupby('rekord_id')['856'].transform(lambda x: '❦'.join(x))
X856 = X856.drop_duplicates()
X856 = pd.merge(X856, pbl_articles[['rekord_id']].drop_duplicates(),  how='right', on = 'rekord_id').sort_values('rekord_id').reset_index(drop=True)
X995 = pbl_articles[['rekord_id']].drop_duplicates().reset_index(drop=True)
X995['995'] = '\\\\$aPBL 1989-2003: książki i czasopisma'
dfs = [X001, X005, X008, X100, X240, X245, X520, X600, X610, X611, X630, X650, X651, X655, X700, X773, X787, X856, X995]
pbl_marc_articles = reduce(lambda left,right: pd.merge(left,right,on='rekord_id'), dfs)
pbl_marc_articles['LDR'] = LDR
pbl_marc_articles['040'] = X040
del pbl_marc_articles['rekord_id']
columns_list = pbl_marc_articles.columns.tolist()
columns_list.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
pbl_marc_articles = pbl_marc_articles.reindex(columns=columns_list)

pbl_marc_articles.to_excel('pbl_marc_articles.xlsx', index=False)

df_to_mrc(pbl_marc_articles, '❦', 'pbl_marc_articles.mrc')



# połączyć wszystko w jedną tabelę, zapisać na dysku, usunąć zmienne, wczytać tylko jeden plik i dalej go przetwarzać do marca
# może dać jakiś input, że chcę ks lub cz, żeby nie powtarzać całego schematu pobierania z sql
# if z inputem?


# bazhum enrichment
# =============================================================================
# pbl_magazine_year = pbl_articles[['czasopismo_id', 'czasopismo', 'rok']].drop_duplicates().sort_values(['czasopismo', 'rok'])
# pbl_magazine_year['rok'] = pbl_magazine_year['rok'].apply(lambda x: '{:4.0f}'.format(x))
# pbl_magazine_year['join'] = pbl_magazine_year.apply(lambda x: '❦'.join(x.astype(str)), axis=1)
# bazhum_pbl_mapping = gsheet_to_df('16OkWDxvJML-SG_7WBF98XE9OtkPSvOUTjaNahVM4a3I', 'Arkusz2')
# bazhum_pbl_mapping.fillna(value=pd.np.nan, inplace=True)
# bazhum_pbl_mapping = bazhum_pbl_mapping[bazhum_pbl_mapping['PBL_czasopismo'].notnull()]
# bazhum_links = gsheet_to_df('1abTBBgmVrAt6JltecOKPWgrRny32r3MaA_JY9QGQRXc', 'Sheet1')    
# for column in bazhum_links:
#     bazhum_links[column] = bazhum_links[column].str.strip().str.replace('^"', '', regex=True).str.replace('",$|,$', '', regex=True)
# bazhum_links = pd.merge(bazhum_links, bazhum_pbl_mapping, how = 'left', left_on = 'tytul_czasopisma', right_on = 'BazHum_czasopismo').drop_duplicates()
# bazhum_magazine_year = bazhum_links[['PBL_id', 'PBL_czasopismo', 'year']].drop_duplicates().sort_values(['PBL_czasopismo', 'year'])
# bazhum_magazine_year['join'] = bazhum_magazine_year.apply(lambda x: '❦'.join(x.astype(str)), axis=1)
# bazhum_magazine_year = bazhum_magazine_year[bazhum_magazine_year['join'].isin(pbl_magazine_year['join'])]
# 
# test = pbl_articles.copy()[['rekord_id', 'autor_nazwisko', 'autor_imie', 'tytul', 'czasopismo_id', 'czasopismo', 'rok', 'numer', 'strony']].drop_duplicates()
# test = test[test['czasopismo_id'].isin(bazhum_pbl_mapping['PBL_id'])]
# test['autor_nazwisko'] = test.groupby('rekord_id')['autor_nazwisko'].transform(lambda x: '❦'.join(x.dropna().astype(str)))
# test['autor_imie'] = test.groupby('rekord_id')['autor_imie'].transform(lambda x: '❦'.join(x.dropna().astype(str)))
# test = test.drop_duplicates()
# 
# def find_number(x):
#     try:
#         val = re.findall(r'\d+', str(x))[0]
#     except:
#         val = np.nan
#     return val
#         
# test['number'] = test['numer'].apply(lambda x: find_number(x))
# test['rok'] = test['rok'].astype(object)
# test['rok'] = test['rok'].apply(lambda x: '{:4.0f}'.format(x))
# test['join'] = test[['czasopismo_id', 'czasopismo', 'rok']].apply(lambda x: '❦'.join(x.astype(str)), axis=1)
# test = test[test['join'].isin(bazhum_magazine_year['join'])]
# 
# bazhum_links['join'] = bazhum_links[['PBL_id', 'PBL_czasopismo', 'year']].apply(lambda x: '❦'.join(x.astype(str)), axis=1)
# bazhum_links = bazhum_links[bazhum_links['join'].isin(bazhum_magazine_year['join'])]
# 
# test['simple_title'] = test['tytul'].apply(lambda x: re.sub('\W', '', str(x).lower()))
# bazhum_links['simple_title'] = bazhum_links['title'].apply(lambda x: re.sub('\W', '', str(x).lower()))
# 
# test['join2'] = test.apply(lambda x: str(x['czasopismo_id']) + str(x['rok']) + str(x['number']) + str(x['strony']), axis=1)
# bazhum_links['join2'] = bazhum_links.apply(lambda x: str(x['PBL_id']) + str(x['year']) + str(x['volume']) + str(x['pages']), axis=1)
# 
# # 1 option
# test2 = pd.merge(test, bazhum_links,  how='inner', left_on = ['czasopismo_id', 'rok', 'number', 'strony'], right_on = ['PBL_id', 'year', 'volume', 'pages'])
# len(test2[test2['id'].notnull()])
# # 1791 vs. 1620
# # 2 option
# test3 = pd.merge(test, bazhum_links,  how='inner', left_on = ['czasopismo_id', 'simple_title'], right_on = ['PBL_id', 'simple_title'])
# len(test3[test3['id'].notnull()])
# len(test3[test3['id'].isnull()])
# # 1588 vs 1796
# # 3 option
# test4 = pd.merge(test, bazhum_links,  how='inner', left_on = ['czasopismo_id', 'strony'], right_on = ['PBL_id', 'pages'])
# 
# testy = pd.concat([test2, test3, test4]).drop_duplicates()
# testy.drop(['number_x', 'join_x', 'join2_x', 'simple_title_x', 'join_y', 'join2_y', 'simple_title_y', 'simple_title'], axis = 1, inplace=True)
# testy = testy.drop_duplicates()
# test = test[~test['rekord_id'].isin(testy['rekord_id'])]
# testy = pd.concat([testy, test])
# testy.drop(['number', 'join', 'join2', 'simple_title'], axis = 1, inplace=True)
# testy = testy.sort_values(['czasopismo', 'rok', 'rekord_id'])
# 
# writer = pd.ExcelWriter('pbl_linki_bazhum.xlsx', engine='xlsxwriter')
# testy.to_excel(writer, sheet_name='pbl_mapping', index=False)
# bazhum_links.to_excel(writer, sheet_name='bazhum', index=False)
# writer.save()
# =============================================================================























            

