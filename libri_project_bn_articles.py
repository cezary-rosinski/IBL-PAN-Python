import pandas as pd
import numpy as np
from my_functions import marc_parser_1_field
import re
import pandasql
from my_functions import cSplit
import json
import requests
from my_functions import df_to_mrc
import io
from my_functions import gsheet_to_df
import cx_Oracle
import regex
from functools import reduce
import glob
from my_functions import f

# =============================================================================
# bn_magazines = gsheet_to_df('10-QUomq_H8v06H-yUhjO60hm45Wbo9DanJTemTgSUrA', 'Sheet1')[['ZRODLA_BN']]
# bn_magazines['index'] = bn_magazines.index + 1
# bn_magazines = cSplit(bn_magazines, 'index', 'ZRODLA_BN', ' \| ')['ZRODLA_BN'].tolist()
# 
# # path = 'F:/Cezary/Documents/IBL/Migracja z BN/bn_all/'
# path = 'C:/Users/User/Documents/bn_all/'
# files = [f for f in glob.glob(path + '*.mrk8', recursive=True)]
# 
# encoding = 'utf-8'
# marc_df = pd.DataFrame()
# for i, file_path in enumerate(files):
#     print(str(i) + '/' + str(len(files)))
#     marc_list = io.open(file_path, 'rt', encoding = encoding).read().splitlines()
#     marc_list = list(filter(None, marc_list))  
#     df = pd.DataFrame(marc_list, columns = ['test'])
#     df['field'] = df['test'].replace(r'(^.)(...)(.+?$)', r'\2', regex = True)
#     df['content'] = df['test'].replace(r'(^.)(.....)(.+?$)', r'\3', regex = True)
#     df['help'] = df.apply(lambda x: f(x, 'LDR'), axis=1)
#     df['help'] = df['help'].ffill()
#     df['magazine'] = df.apply(lambda x: f(x, '773'), axis=1)
#     df['magazine'] = df.groupby('help')['magazine'].ffill().bfill()
#     df = df[df['magazine'].notnull()]
#     try:
#         df['index'] = df.index + 1
#         df_field = marc_parser_1_field(df, 'index', 'magazine', '\$')[['index', '$t']]
#         df_field['$t'] = df_field['$t'].str.replace('\.$', '')
#         df_field.columns = ['index', 'title']
#         df = pd.merge(df, df_field, how='left', on='index')
#         df = df[df['title'].isin(bn_magazines)].drop(columns=['magazine', 'index', 'title'])
#     except AttributeError:
#         pass
#     if len(df) > 0:
#         df['id'] = df.apply(lambda x: f(x, '009'), axis = 1)
#         df['id'] = df.groupby('help')['id'].ffill().bfill()
#         df = df[['id', 'field', 'content']]
#         df['content'] = df.groupby(['id', 'field'])['content'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
#         df = df.drop_duplicates().reset_index(drop=True)
#         df_wide = df.pivot(index = 'id', columns = 'field', values = 'content')
#         marc_df = marc_df.append(df_wide)
#  
# fields = marc_df.columns.tolist()
# fields = [i for i in fields if 'LDR' in i or re.compile('\d{3}').findall(i)]
# marc_df = marc_df.loc[:, marc_df.columns.isin(fields)]
# 
# fields.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
# marc_df = marc_df.reindex(columns=fields)       
# marc_df.to_excel('bn_articles.xlsx', index=False)
# =============================================================================
        
# SQL connection

dsn_tns = cx_Oracle.makedsn('pbl.ibl.poznan.pl', '1521', service_name='xe')
connection = cx_Oracle.connect(user='IBL_SELECT', password='CR333444', dsn=dsn_tns, encoding='windows-1250')

entire_magazines = gsheet_to_df('10-QUomq_H8v06H-yUhjO60hm45Wbo9DanJTemTgSUrA', 'Sheet1')[['ZRODLA_BN', 'typ2']]
entire_magazines = entire_magazines[(entire_magazines['typ2'].str.contains('fil|lit')) &
                                    (~entire_magazines['typ2'].str.contains('filozof|polit'))].reset_index(drop=True)
entire_magazines['index'] = entire_magazines.index + 1
entire_magazines = cSplit(entire_magazines, 'index', 'ZRODLA_BN', ' \| ')['ZRODLA_BN'].tolist()

bn_articles = pd.read_excel('bn_articles.xlsx')   
bn_articles = bn_articles[bn_articles['773'].notnull()].reset_index(drop=True) 
bn_articles['id'] = bn_articles['009']

pbl_viaf_links = ['1cEz73dGN2r2-TTc702yne9tKfH9PQ6UyAJ2zBSV6Jb0', '1_Bhwzo0xu4yTn8tF0ZNAZq9iIAqIxfcrjeLVCm_mggM', '1L-7Zv9EyLr5FeCIY_s90rT5Hz6DjAScCx6NxfuHvoEQ']
pbl_viaf = pd.DataFrame()
for elem in pbl_viaf_links:
    df = gsheet_to_df(elem, 'pbl_bn').drop_duplicates()
    df = df[df['czy_ten_sam'] != 'nie'][['pbl_id', 'BN_id', 'BN_name']]
    df['BN_name'] = df['BN_name'].str.replace('\|\(', ' (').str.replace('\;\|', '; ').str.replace('\|$', '')
    df['index'] = df.index + 1
    df = cSplit(df, 'index', 'BN_name', '\|').drop(columns='index')
    pbl_viaf = pbl_viaf.append(df)
pbl_viaf = pbl_viaf.drop_duplicates()

tworca_i_dzial = """select tw.tw_tworca_id "pbl_id", dz.dz_dzial_id||'|'||dz.dz_nazwa "osoba_pbl_dzial_id_name"
                    from pbl_tworcy tw
                    full join pbl_dzialy dz on dz.dz_dzial_id=tw.tw_dz_dzial_id"""
tworca_i_dzial = pd.read_sql(tworca_i_dzial, con=connection).fillna(value = np.nan)
tworca_i_dzial['pbl_id'] = tworca_i_dzial['pbl_id'].apply(lambda x: '{:4.0f}'.format(x))

X100 = marc_parser_1_field(bn_articles, 'id', '100', '\$')[['id', '$a', '$c', '$d']].replace(r'^\s*$', np.NaN, regex=True)
X100['name'] = X100[['$a', '$d', '$c']].apply(lambda x: ' '.join(x.dropna().astype(str)), axis=1)
X100 = X100[['id', 'name']]
X100['name'] = X100['name'].str.replace("(\))(\.$)", r"\1").apply(lambda x: regex.sub('(\p{Ll})(\.$)', r'\1', x))
X100 = pd.merge(X100, pbl_viaf, how='inner', left_on='name', right_on='BN_name')[['id', 'name', 'pbl_id']]
X100 = pd.merge(X100, tworca_i_dzial, how='left', on='pbl_id')[['id', 'osoba_pbl_dzial_id_name']]
X100['osoba_bn_autor'] = X100.groupby('id')['osoba_pbl_dzial_id_name'].transform(lambda x: '❦'.join(x.astype(str)))
X100 = X100.drop(columns='osoba_pbl_dzial_id_name').drop_duplicates()

X600 = marc_parser_1_field(bn_articles, 'id', '600', '\$')[['id', '$a', '$c', '$d']].replace(r'^\s*$', np.NaN, regex=True)
X600['name'] = X600[['$a', '$d', '$c']].apply(lambda x: ' '.join(x.dropna().astype(str)), axis=1)
X600 = X600[['id', 'name']]
X600['name'] = X600['name'].str.replace("(\))(\.$)", r"\1").apply(lambda x: regex.sub('(\p{Ll})(\.$)', r'\1', x))
X600 = pd.merge(X600, pbl_viaf, how='inner', left_on='name', right_on='BN_name')[['id', 'name', 'pbl_id']]
X600 = pd.merge(X600, tworca_i_dzial, how='left', on='pbl_id')[['id', 'osoba_pbl_dzial_id_name']]
X600['osoba_bn_temat'] = X600.groupby('id')['osoba_pbl_dzial_id_name'].transform(lambda x: '❦'.join(x.astype(str)))
X600 = X600.drop(columns='osoba_pbl_dzial_id_name').drop_duplicates()

bn_articles = [bn_articles, X100, X600]
bn_articles = reduce(lambda left,right: pd.merge(left,right,on='id', how = 'outer'), bn_articles)

def dziedzina_PBL(x):
    try:
        if bool(regex.search('(?<=\$a|:|\[|\+|\()(82)', x)):
            val = 'ukd_lit'
        elif bool(regex.search('(?<=\$a|:|\[|\+)(791)', x)) or bool(regex.search('(?<=\$a|:)(792)', x)) or bool(regex.search('\$a7\.09', x)):
            val = 'ukd_tfrtv'
        elif bool(regex.search('(?<=\$a01)(\(|\/|2|4|5|9)', x)) or bool(regex.search('(?<=\$a|\[])(050)', x)) or bool(regex.search('(?<=\$a|:|\[|\+|\()(811\.162)', x)):
            val = 'ukd_biblio'
        elif bool(regex.search('\$a002', x)) or bool(regex.search('(?<=\$a|:)(305)', x)) or bool(regex.search('(?<=\$a39|:39)(\(438\)|8\.2)', x)) or bool(regex.search('(?<=\$a|:)(929[^\.]051)', x)) or bool(regex.search('(?<=\$a|:)(929[^\.]052)', x)):
            val = 'ukd_pogranicze'
        else:
            val = 'bez_ukd_PBL'
    except TypeError:
        val = 'bez_ukd_PBL'
    return val

bn_articles['dziedzina_PBL'] = bn_articles['080'].apply(lambda x: dziedzina_PBL(x))

literary_word_keys = bn_articles.copy()[['id', '080', '245', '600', '610', '630', '648', '650', '651', '655', '658', 'osoba_bn_autor', 'osoba_bn_temat', 'dziedzina_PBL']]

literary_words = 'literat|literac|pisar|bajk|dramat|epigramat|esej|felieton|film|komedi|nowel|opowiadani|pamiętnik|poemiks|poezj|powieść|proza|reportaż|satyr|wspomnieni|Scenariusze zajęć|Podręczniki dla gimnazjów|teatr|Nagrod|aforyzm|baśń|baśnie|polonijn|dialogi|fantastyka naukowa|legend|pieśń|poemat|przypowieś|honoris causa|filologi|kino polskie|pieśni|interpretacj|poet|liryk'
literary_word_keys['literary_word_keys'] = literary_word_keys['245'].str.contains(literary_words, flags=re.IGNORECASE) | literary_word_keys['600'].str.contains(literary_words, flags=re.IGNORECASE) | literary_word_keys['610'].str.contains(literary_words, flags=re.IGNORECASE) | literary_word_keys['630'].str.contains(literary_words, flags=re.IGNORECASE) | literary_word_keys['648'].str.contains(literary_words, flags=re.IGNORECASE) | literary_word_keys['650'].str.contains(literary_words, flags=re.IGNORECASE) | literary_word_keys['651'].str.contains(literary_words, flags=re.IGNORECASE) | literary_word_keys['655'].str.contains(literary_words, flags=re.IGNORECASE) | literary_word_keys['658'].str.contains(literary_words, flags=re.IGNORECASE)
literary_word_keys = literary_word_keys[literary_word_keys['literary_word_keys'] == True][['id', 'literary_word_keys']]
bn_articles = pd.merge(bn_articles, literary_word_keys, how='left', on='id')

memories_words = 'pamiętniki i wspomnienia|literatura podróżnicza|pamiętniki|reportaż|relacja z podróży'
memories = bn_articles.copy()[['id', '245', '600', '610', '630', '648', '650', '651', '655', '658']]
memories['wspomnienia'] = memories['245'].str.contains(memories_words, flags=re.IGNORECASE) | memories['600'].str.contains(memories_words, flags=re.IGNORECASE) | memories['600'].str.contains(memories_words, flags=re.IGNORECASE) | memories['610'].str.contains(memories_words, flags=re.IGNORECASE) | memories['630'].str.contains(memories_words, flags=re.IGNORECASE) | memories['648'].str.contains(memories_words, flags=re.IGNORECASE) | memories['650'].str.contains(memories_words, flags=re.IGNORECASE) | memories['655'].str.contains(memories_words, flags=re.IGNORECASE) | memories['658'].str.contains(memories_words, flags=re.IGNORECASE)
memories = memories[memories['wspomnienia'] == True][['id', 'wspomnienia']]
bn_articles = pd.merge(bn_articles, memories, how='left', on='id')

bible_words = "biblia|analiza i interpretacja|edycja krytyczna|materiały konferencyjne"
bible = bn_articles.copy()[['id', '245', '650', '655']]
bible['biblia'] = bible['245'].str.contains(bible_words, flags=re.IGNORECASE) | bible['650'].str.contains(bible_words, flags=re.IGNORECASE) | bible['650'].str.contains(bible_words, flags=re.IGNORECASE)
bible = bible[bible['biblia'] == True][['id', 'biblia']]
bn_articles = pd.merge(bn_articles, bible, how='left', on='id')

zle = bn_articles.copy()
zle = zle[(zle['osoba_bn_autor'].isnull()) & 
        (zle['osoba_bn_temat'].isnull()) & 
        (zle['dziedzina_PBL'] == 'bez_ukd_PBL') & 
        (zle['literary_word_keys'].isnull()) & 
        (zle['wspomnienia'].isnull()) & 
        (zle['biblia'].isnull())]
zle = zle[['id', '080', '773', '245', '600', '610', '630', '648', '650', '651', '655', '658', 'osoba_bn_autor', 'osoba_bn_temat', 'dziedzina_PBL', 'literary_word_keys', 'wspomnienia', 'biblia']]

dobre = bn_articles.copy()
dobre = dobre[~dobre['id'].isin(zle['id'])]
dobre = dobre[['id', '080', '773', '245', '600', '610', '630', '648', '650', '651', '655', '658', 'osoba_bn_autor', 'osoba_bn_temat', 'dziedzina_PBL', 'literary_word_keys', 'wspomnienia', 'biblia']]



# Nikodem

# =============================================================================
# ids = [17858, 5827, 13557, 14877, 2346, 829, 10098, 18157, 40, 992, 1091, 15059, 371, 6782, 7677, 1422, 14857, 1161, 2907, 6764, 9758, 9497, 9357, 12177, 16618, 757, 7057, 17637, 11899, 382, 163, 6756, 1025, 7197, 5867, 16037, 9877, 8207, 457, 16337, 10817, 3267, 12877, 16417, 9377, 2312, 18900, 299, 6783, 660, 355, 16840, 574, 11898, 16537, 5927, 10758, 12958, 12939, 269, 17697, 18899, 796, 7758, 2221, 15026, 10598, 6925, 1362, 5887, 14617, 15018, 16318, 12357, 1903, 6091, 6776, 5447, 1581, 11857, 566, 7937, 1385, 3388, 858, 16617, 234, 61, 1582, 1411, 10637, 435, 6738, 8137, 10217, 477, 9777, 8205, 16477, 902, 11000, 13677, 11017, 1401, 5387, 1321, 3932, 4487, 70, 5067, 11577, 15158, 8259, 274, 192, 45, 17803, 15078, 3947, 3547, 814, 1546, 11719, 18098, 6767, 611, 3467, 15017, 851, 3289, 3187, 4107, 5293, 17740, 10777, 1245, 12977, 18878, 388, 18800, 17837, 96, 10999, 6291, 12938, 3927, 18917, 18818, 17957, 6889, 6825, 14881, 16658, 276, 2947, 14880, 2328, 850, 14980, 5847, 13538, 8437, 14882, 6167, 602, 14777, 290, 10537, 81, 16538, 2304, 146, 11997, 17797, 3875, 766, 17417, 2687, 6411, 845, 229, 10657, 2326, 17839, 14779, 4787, 1909, 16997, 69, 1608, 15178, 10898, 8559, 806, 15317, 17938, 5767, 1921, 12658, 1062, 231, 1614, 1163, 347, 12117, 9097, 5048, 67, 354, 437, 15117, 2348, 3987, 14817, 6905, 16177, 16117, 4647, 306, 1341, 1550, 353, 4687, 953, 17698, 2121, 10137, 3068, 13637, 1625, 6047, 6827, 14237, 4888, 861, 15177, 15939, 398, 13177, 758, 15057, 6018, 2655, 15157, 345, 4967, 14057, 5967, 6994, 10677, 2329, 1323, 16717, 14017, 16078, 10097, 4207, 101, 8199, 439, 4147, 6989, 11139, 4327, 8224, 186, 16678, 182, 15357, 12617, 17819, 1902, 2767, 8397, 1545, 536, 114, 5292, 761, 16817, 6962, 2334, 2353, 17804, 10717, 785, 1281, 12957, 8200, 16317, 6961, 2658, 12197, 14757, 2887, 1566, 16838, 3387, 6734, 1906, 4788, 1164, 1366, 2142, 12657, 351, 289, 561, 63, 15098, 366, 2307, 14918, 9757, 3934, 7597, 818, 7297, 3872, 433, 220, 57, 16437, 17377, 1910, 800, 15337, 113, 5288, 6008, 14058, 8698, 1605, 52, 3147, 6342, 209, 2666, 5187, 6651, 65, 6067, 12479, 10978, 6927, 856, 210, 657, 570, 330, 4247, 365, 14897, 735, 11117, 286, 12577, 16319, 786, 4007, 462, 6591, 1626, 12157, 11098, 4927, 4447, 1381, 3948, 436, 1406, 16397, 1194, 469, 14497, 1384, 1027, 14117, 576, 1541, 5909, 6633, 18957, 3874, 489, 162, 1108, 638, 10597, 636, 17744, 10980, 298, 1065, 15579, 6828, 13357, 16599, 18717, 328, 425, 11077, 9698, 1223, 15037, 15623, 442, 668, 5527, 7017, 1240, 514, 15019, 3327, 6571, 97, 1116, 117, 11217, 10020, 792, 111, 5888, 17838, 8457, 10737, 10979, 680, 315, 1226, 15977, 801, 743, 1564, 10437, 17801, 6976, 12638, 17759, 6779, 11601, 12837, 753, 375, 13037, 6020, 723, 5207, 14297, 17657, 3907, 8201, 18477, 15677, 14277, 3347, 4789, 8213, 6778, 5227, 12297, 1039, 4747, 11097, 721, 1121, 859, 15621, 6692, 1237, 1607, 282, 1147, 7157, 14778, 10897, 6897, 15897, 74, 16418, 66, 103, 1606, 508, 195, 10917, 683, 10997, 18017, 428, 10837, 1244, 17798, 434, 13877, 185, 15498, 8206, 1565, 1383, 11040, 3894, 15217, 1081, 285, 1409, 8217, 12018, 421, 1109]
# 
# query = """select z.za_zapis_id, zr.zr_zrodlo_id, z.za_ro_rok
# from pbl_zapisy z
# join IBL_OWNER.pbl_zrodla zr on zr.zr_zrodlo_id=z.za_zr_zrodlo_id"""
# 
# pbl_query = pd.read_sql(query, connection)
# 
# pbl_query = pbl_query[pbl_query['ZR_ZRODLO_ID'].isin(ids)]
# 
# czasopisma = pbl_query['ZR_ZRODLO_ID'].value_counts().to_frame()
# czasopisma2 = pbl_query.copy()[['ZR_ZRODLO_ID', "ZA_RO_ROK"]].drop_duplicates()
# czasopisma2['ZA_RO_ROK'] = czasopisma2.groupby('ZR_ZRODLO_ID')['ZA_RO_ROK'].transform(lambda x: ', '.join(x.dropna().astype(str)))
# czasopisma2 = czasopisma2.drop_duplicates()
# czasopisma2['ZA_RO_ROK'] = czasopisma2['ZA_RO_ROK'].str.split(', ').apply(set).str.join(', ')
# 
# test_mag = marc_parser_1_field(bn_articles, 'id', '773', '\$')[['id', '$t']]
# test = bn_articles[['id', '008']]
# test['008'] = test['008'].apply(lambda x: x[7:11])
# test_mag['$t'] = test_mag['$t'].str.replace('\.$', '')
# test_mag.columns = ['id', 'magazine']
# test_mag = pd.merge(test_mag, test, how='left', on='id')
# 
# lata_bn = test_mag.copy()[['magazine', '008']]
# lata_bn['008'] = lata_bn.groupby('magazine')['008'].transform(lambda x: ', '.join(x))
# lata_bn = lata_bn.drop_duplicates()
# lata_bn['008'] = lata_bn['008'].str.split(', ').apply(set).str.join(', ')
# 
# 
# test_mag = test_mag['magazine'].value_counts().to_frame()
# test_mag['title'] = test_mag.index
# test_mag = test_mag.reset_index(drop=True)
# 
# 
# ok_mag = dobre.copy()[['id', '773']]
# ok_mag = marc_parser_1_field(ok_mag, 'id', '773', '\$')[['$t']]
# ok_mag['$t'] = ok_mag['$t'].str.replace('\.$', '')
# ok_mag.columns = ['magazine']
# ok_mag = ok_mag['magazine'].value_counts().to_frame()
# ok_mag['title'] = ok_mag.index
# ok_mag = ok_mag.reset_index(drop=True)
# 
# test = pd.merge(test_mag, ok_mag, how='left', on='title')
# test.columns = ['all', 'magazine', 'ok']
# test = test[['magazine', 'all', 'ok']]
# test['%'] = test['ok'] / test['all'] * 100
# test = test.sort_values('%', ascending=False)
# 
# test.to_excel('test.xlsx', index=False)
# =============================================================================













type_of_record_old = bn_articles.copy()
type_of_record_old['czy_ma_ukd'] = type_of_record_old['080'].apply(lambda x: 'tak' if pd.notnull(x) else 'nie')
def position_of_dash(x):
    try:
        if bool(re.search('(\\\\\$a|:)(821\.)', x)):
            val = x.index('-')
        else: 
            val = np.nan
    except (TypeError, ValueError):
        val = np.nan
    return val
    
type_of_record_old['dash_position'] = type_of_record_old['080'].apply(lambda x: position_of_dash(x))



test = type_of_record_old[['080', 'dash_position']]


bn_ok <- chunk4
stare_rodzajowanie <- bn_ok %>%
  mutate(czy_ma_ukd = ifelse(X080=="","nie","tak"),
         position_dash = ifelse(grepl("(\\\\\\\\\\$a|:)(821\\.)",X080),str_locate(X080,"\\-")[,1], NA),
         position_dash = ifelse(is.na(position_dash),"",as.integer(position_dash)),
         position_091 = str_locate(X080,"\\(091\\)")[,1],
         position_091 = ifelse(is.na(position_091),"",as.integer(position_091)),
         rodzaj_ksiazki = ifelse(grepl("Antologi",X655),"antologia",
                                 ifelse(position_091!=""&position_dash!="",
                          ifelse(as.integer(position_091)<as.integer(position_dash), "przedmiotowa", "podmiotowa"),
                          ifelse(position_dash!="","podmiotowa","przedmiotowa"))),
         rodzaj_ksiazki = ifelse(czy_ma_ukd=="nie","",as.character(rodzaj_ksiazki)))
gatunki_podmiotowe <- stare_rodzajowanie %>%
  filter(rodzaj_ksiazki=="podmiotowa") %>%
  select(X655) %>%
  unique() %>%
  cSplit(.,"X655",sep = "|",direction = "long") %>%
  unique() %>%
  filter(str_detect(X655,"\\$y[\\d-]+ w\\."))
gatunki_podmiotowe <- str_replace_all(str_replace_all(paste(gatunki_podmiotowe$X655,collapse = "|"),"(.{2})(\\$a)","\\2"),"\\$","\\\\$")
stare_rodzajowanie$czy_podmiotowy <- grepl(gatunki_podmiotowe,stare_rodzajowanie$X655)|grepl(gatunki_podmiotowe,stare_rodzajowanie$X650)
stare_rodzajowanie <- stare_rodzajowanie %>%
  mutate(rodzaj_ksiazki = ifelse(str_count(X245, " / ")+1>2,"antologia",
                                 ifelse(str_count(X245, " / ")+1==2,"współwydanie",
                                        ifelse(rodzaj_ksiazki==""&czy_podmiotowy==TRUE&!grepl("xhistoria|xtematyka|xbiografia",X650)&!grepl("xhistoria|xtematyka|xbiografia",X655),"podmiotowa",
                                               ifelse(X100!=""&grepl("aPamiętnik|aLiteratura podróżnicza",X655)&!grepl("xhistoria|xtematyka|xbiografia",X650)&!grepl("xhistoria|xtematyka|xbiografia",X655),"podmiotowa",
                                                      ifelse(X100!=""&grepl("aReportaż",X655)&grepl("\\$y",X655)&!grepl("xhistoria|xtematyka|xbiografia",X650)&!grepl("xhistoria|xtematyka|xbiografia",X655),"podmiotowa",
                                                             ifelse(X100!=""&(X655=="\\7$aReportaż polski$2DBN"|X655=="\\7$aReportaż$2DBN")&!grepl("xhistoria|xtematyka|xbiografia",X650)&!grepl("xhistoria|xtematyka|xbiografia",X655),"podmiotowa",
                                                                    ifelse(rodzaj_ksiazki==""&czy_podmiotowy==FALSE,"przedmiotowa",as.character(rodzaj_ksiazki)))))))),
         rodzaj_ksiazki = ifelse(rodzaj_ksiazki=="","przedmiotowa",as.character(rodzaj_ksiazki)),
         rodzaj_ksiazki = ifelse(grepl("Lektury Wszech Czasów : streszczenie, analiza, interpretacja|Lektury Wszech Czasów - Literat|Biblioteczka Opracowań",X490)|grepl("Lektury Wszech Czasów : streszczenie, analiza, interpretacja|Lektury Wszech Czasów - Literat|Biblioteczka Opracowań",X830),"przedmiotowa",as.character(rodzaj_ksiazki)),
         ilu_tworcow = str_count(X100,"\\$a"),
         rodzaj_ksiazki = ifelse(ilu_tworcow>4&rodzaj_ksiazki=="podmiotowa","antologia",as.character(rodzaj_ksiazki)),
         rodzaj_ksiazki = ifelse(grepl("Legendy",X655),"antologia",as.character(rodzaj_ksiazki))) %>%
  filter(grepl("katalog wystawy",X655,ignore.case = TRUE)&rodzaj_ksiazki=="przedmiotowa") %>% 
  select(id)










listy_2011 = gsheet_to_df('1s22ClRxlrPHaAXi_n_JJH3mX6vKYzKjsKbpJpQztx_8', 'lista_ksiazek')
listy_2010 = gsheet_to_df('1Vjeg0JsYI-8v9B-x_yyujIhmw7UR7Lk7poexrHNdVzM', 'lista_ksiazek')
listy_2009 = gsheet_to_df('1Gc4gQSm9b4NDTQysiauzW9Jac6yP0oNuFbB8utO4kS4', 'lista_ksiazek')
listy_2005 = gsheet_to_df('1HkWkX61sQWktSXf0v0uPV8j2DwuTocesyCJuKTdisIU', 'lista_ksiazek')
listy_2006 = gsheet_to_df('1zeMx_Idsum8JmlM6G7Eufx9LxloHoAHv8V-My71VZf4', 'lista_ksiazek')
listy_2007 = gsheet_to_df('19iL7YoD8ug-rLnpzS6FD46aS2J1BRf4qL5VxllywCGE', 'lista_ksiazek')
listy_2008 = gsheet_to_df('1RshTeWdXBE7OzOEfoGpL9Ljb_GXDlGDePNjV1HKmuOo', 'lista_ksiazek')
listy_2004 = gsheet_to_df('1RmDia97s4B8F74sS7Wbpnv_A9zMfr4xTvcD9leukAfM', 'lista_książek')
listy_2004['typ_ksiazki'], listy_2004['link'], listy_2004['link_1'], listy_2004['status'], listy_2004['blad_w_imporcie_tytulu'] = [np.nan, np.nan, np.nan, np.nan, np.nan]
listy_2004 = listy_2004[listy_2005.columns]





test = bn_articles.copy().head(1000)







test = bn_articles[bn_articles['dziedzina_PBL'] != 'błąd'][['X080', 'dziedzina_PBL']]

for i, lista in test['X080'].iteritems():
    for element in lista:
        if isinstance(element, str) == False:
            print(i, type(element))

dir(pd.Series)


bn_cz_mapping = pd.read_excel('F:/Cezary/Documents/IBL/Pliki python/bn_cz_mapping.xlsx')
gatunki_pbl = pd.DataFrame({'gatunek': ["aforyzm", "album", "antologia", "autobiografia", "dziennik", "esej", "felieton", "inne", "kazanie", "list", "miniatura prozą", "opowiadanie", "poemat", "powieść", "proza", "proza poetycka", "reportaż", "rozmyślanie religijne", "rysunek, obraz", "scenariusz", "szkic", "tekst biblijny", "tekst dramatyczny", "dramat", "wiersze", "wspomnienia", "wypowiedź", "pamiętniki", "poezja", "literatura podróżnicza", "satyra", "piosenka"]})
gatunki_pbl['gatunek'] = gatunki_pbl['gatunek'].apply(lambda x: f"$a{x}")

test = bn_articles.copy()
test['osoba_bn_autor'] = test[['osoba_bn_autor', 'X100']].apply(lambda x: np.nan if pd.isnull(x['X100']) else x['osoba_bn_autor'], axis=1)
test['osoba_bn_temat'] = test[['osoba_bn_temat', 'X600']].apply(lambda x: np.nan if pd.isnull(x['X600']) else x['osoba_bn_temat'], axis=1)
test['dziedzina_PBL'] = test[['dziedzina_PBL', 'X080']].apply(lambda x: np.nan if pd.isnull(x['X080']) else x['dziedzina_PBL'], axis=1)

testowy = test.copy()[(test['osoba_bn_autor'].isnull()) &
                      (test['osoba_bn_temat'].isnull()) &
                      (test['dziedzina_PBL'].isnull()) &
                      (test['literary_word_keys'].isnull()) &
                      (test['slowa_literackie'].isnull())][['X080', 'X100', 'X245', 'X600', 'X610', 'X630', 'X650', 'X651', 'X655', 'X773']]


test = bn_articles[(bn_articles['X650'].isnull()) &
                   (bn_articles['X655'].isnull()) &
                   (bn_articles['X651'].isnull()) &
                   (bn_articles['X600'].isnull()) &
                   (bn_articles['X610'].isnull()) &
                   (bn_articles['slowa_literackie'].isnull())]

test2 = testowy[testowy['X245'].str.contains('Zaliczka albo dodatek')==True]

test = bn_articles.head(10000)
bn_articles = bn_articles[bn_articles['X001'] == 'b0000003184267']


test = pbl_enrichment.copy()[pbl_enrichment.index == '991027974299705066']

n = 10000
list_df = [bn_articles[i:i+n] for i in range(0, bn_articles.shape[0],n)]

pbl_enrichment_full = pd.DataFrame()
for i, group in enumerate(list_df):
    print(str(i) + '/' + str(len(list_df)))
    pbl_enrichment = group.copy()[['id', 'dziedzina_PBL', 'rodzaj_ksiazki', 'DZ_NAZWA', 'X650', 'X655']]
    pbl_enrichment['DZ_NAZWA'] = pbl_enrichment['DZ_NAZWA'].str.replace(' - .*?$', '', regex=True)
    pbl_enrichment = cSplit(pbl_enrichment, 'id', 'X655', '|')
    pbl_enrichment['jest x'] = pbl_enrichment['X655'].str.contains('\$x')
    pbl_enrichment['nowe650'] = pbl_enrichment.apply(lambda x: x['X655'] if x['jest x'] == True else np.nan, axis=1)
    pbl_enrichment['X655'] = pbl_enrichment.apply(lambda x: x['X655'] if x['jest x'] == False else np.nan, axis=1)
    pbl_enrichment['X650'] = pbl_enrichment[['X650', 'nowe650']].apply(lambda x: '❦'.join(x.dropna().astype(str)), axis=1)
    pbl_enrichment = pbl_enrichment.drop(['jest x', 'nowe650'], axis=1)
    
    query = "select * from pbl_enrichment a join gatunki_pbl b on lower(a.X655) like '%'||b.gatunek||'%'"
    gatunki1 = pandasql.sqldf(query)
    query = "select * from pbl_enrichment a join gatunki_pbl b on lower(a.X650) like '%'||b.gatunek||'%'"
    gatunki2 = pandasql.sqldf(query)
    gatunki = pd.concat([gatunki1, gatunki2]).drop_duplicates()
    gatunki['gatunek'] = gatunki['gatunek'].apply(lambda x: ''.join([x[:2], x[2].upper(), x[2 + 1:]]).strip())
    try:
        X655_field = marc_parser_1_field(gatunki, 'id', 'X655', '\$')[['id', '$y']].drop_duplicates()
        X655_field = X655_field[X655_field['$y'] != '']   
        gatunki = pd.merge(gatunki, X655_field, how='left', on='id')
    except KeyError:
        pass
    gatunki['gatunek'] = gatunki['gatunek'].apply(lambda x: f"\\7{x.strip()}")
    try:
        gatunki['gatunek+data'] = gatunki.apply(lambda x: f"{x['gatunek']}$y{x['$y']}" if pd.notnull(x['$y']) else np.nan, axis=1)
        gatunki['nowe655'] = gatunki[['X655', 'gatunek', 'gatunek+data']].apply(lambda x: '❦'.join(x.dropna().astype(str)), axis=1)
    except KeyError:
        pass
    gatunki['nowe655'] = gatunki[['X655', 'gatunek']].apply(lambda x: '❦'.join(x.dropna().astype(str)), axis=1)
    gatunki['nowe655'] = gatunki.groupby('id')['nowe655'].transform(lambda x: '❦'.join(x))
    gatunki = gatunki[['id', 'nowe655']].drop_duplicates()
    gatunki['nowe655'] = gatunki['nowe655'].str.split('❦').apply(set).str.join('❦')
    
    pbl_enrichment = pd.merge(pbl_enrichment, gatunki, how ='left', on='id')
    pbl_enrichment['nowe650'] = pbl_enrichment.apply(lambda x: x['X655'] if pd.isnull(x['nowe655']) else np.nan, axis=1)
    pbl_enrichment['DZ_NAZWA'] = pbl_enrichment['DZ_NAZWA'].apply(lambda x: f"\\7$a{x}" if 'do ustalenia' not in x else np.nan)
    pbl_enrichment['X650'] = pbl_enrichment['X650'].replace(r'^\s*$', np.nan, regex=True)
    pbl_enrichment['650'] = pbl_enrichment[['X650', 'nowe650', 'DZ_NAZWA']].apply(lambda x: '❦'.join(x.dropna().astype(str)), axis=1)
    pbl_enrichment['655'] = pbl_enrichment['nowe655'].replace(np.nan, '', regex=True)
    pbl_enrichment['655'] = pbl_enrichment.apply(lambda x: f"{x['655']}❦\\7$aOpracowanie" if x['rodzaj_ksiazki'] == 'przedmiotowa' else f"{x['655']}❦\\7$aDzieło literackie", axis=1)
    pbl_enrichment = pbl_enrichment[['id', '650', '655']].replace(r'^\❦', '', regex=True)
    pbl_enrichment['650'] = pbl_enrichment.groupby('id')['650'].transform(lambda x: '❦'.join(x.dropna().astype(str)))
    pbl_enrichment['655'] = pbl_enrichment.groupby('id')['655'].transform(lambda x: '❦'.join(x.dropna().astype(str)))
    pbl_enrichment = pbl_enrichment.drop_duplicates().reset_index(drop=True)
    pbl_enrichment['650'] = pbl_enrichment['650'].str.split('❦').apply(set).str.join('❦')
    pbl_enrichment['655'] = pbl_enrichment['655'].str.split('❦').apply(set).str.join('❦')
    pbl_enrichment_full = pbl_enrichment_full.append(pbl_enrichment)



















position_of_LDR = bn_articles.columns.get_loc("LDR")
bn_articles_marc = bn_articles.iloc[:,position_of_LDR:]

bn_articles_marc = bn_articles_marc.set_index('X009', drop=False)
pbl_enrichment = pbl_enrichment.set_index('id').rename(columns={'650':'X650', '655':'X655'})
bn_articles_marc = pbl_enrichment.combine_first(bn_articles_marc)

fields_to_remove = bn_cz_mapping[bn_cz_mapping['cz'] == 'del']['bn'].to_list()
bn_articles_marc = bn_articles_marc.loc[:, ~bn_articles_marc.columns.isin(fields_to_remove)]

merge_500s = [col for col in bn_articles_marc.columns if 'X5' in col]

bn_articles_marc['500'] = bn_articles_marc[merge_500s].apply(lambda x: '❦'.join(x.dropna().astype(str)), axis = 1)
bn_articles_marc = bn_articles_marc.loc[:, ~bn_articles_marc.columns.isin(merge_500s)]
bn_articles_marc.rename(columns={'X260':'X264'}, inplace=True)
bn_articles_marc.drop(['X852', 'X856'], axis = 1, inplace=True) 
bn_new_column_names = bn_articles_marc.columns.to_list()
bn_new_column_names = [column.replace('X', '') for column in bn_new_column_names]
bn_articles_marc.columns = bn_new_column_names
bn_articles_marc['240'] = bn_articles_marc['246'].apply(lambda x: x if pd.notnull(x) and 'Tyt. oryg.:' in x else np.nan)
bn_articles_marc['246'] = bn_articles_marc['246'].apply(lambda x: x if pd.notnull(x) and 'Tyt. oryg.:' not in x else np.nan)
bn_articles_marc['995'] = '\\\\$aPBL 2004-2019: czasopisma'