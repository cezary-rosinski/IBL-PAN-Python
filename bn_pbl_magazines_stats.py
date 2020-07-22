import pandas as pd
import numpy as np
from my_functions import marc_parser_1_field
import pandasql
from my_functions import cSplit
from my_functions import gsheet_to_df
import cx_Oracle

dsn_tns = cx_Oracle.makedsn('pbl.ibl.poznan.pl', '1521', service_name='xe')
connection = cx_Oracle.connect(user='IBL_SELECT', password='CR333444', dsn=dsn_tns, encoding='windows-1250')

pbl_bn_magazines = gsheet_to_df('1V6BreA4_cEb3FRvv53E5ri2Yl1u3Z2x-yQxxBbAzCeo', 'Sheet1')
pbl_bn_magazines = pbl_bn_magazines[pbl_bn_magazines['decyzja'] == 'tak']

bn_magazines = pd.read_excel('bn_magazines_to_statistics.xlsx')[['id', '008', '773', 'decision']]
bn_magazine_title = marc_parser_1_field(bn_magazines, 'id', '773', '\$')[['id', '$t']]
bn_magazine_title.columns = ['id', 'bn_magazine']
bn_magazine_title = pd.merge(bn_magazine_title, pbl_bn_magazines[['pbl_magazine', 'bn_magazine']], 'left', 'bn_magazine')
pbl_query = 'select zr.zr_zrodlo_id, zr.zr_tytul from pbl_zrodla zr'
pbl_magazines = pd.read_sql(pbl_query, connection).rename(columns={'ZR_TYTUL':'pbl_magazine', 'ZR_ZRODLO_ID':'pbl_id'})
bn_magazine_title = pd.merge(bn_magazine_title, pbl_magazines, 'left', 'pbl_magazine').drop(columns=['bn_magazine'])
bn_magazines = pd.merge(bn_magazines, bn_magazine_title, how='left', on='id')
bn_magazines['year'] = bn_magazines['008'].apply(lambda x: x[7:11])
bn_magazines = bn_magazines[['id', 'pbl_id', 'pbl_magazine', 'year', 'decision']]

bn_query = """select q1.pbl_id, q1.pbl_magazine, q1.year, q1."liczba BN", q2."liczba BN ok"
            from
            (select a.pbl_id, a.pbl_magazine, a.year, count (*) "liczba BN"
            from bn_magazines a
            group by a.pbl_id, a.pbl_magazine, a.year) q1
            left join
            (select a.pbl_id, a.pbl_magazine, a.year, count (*) "liczba BN ok"
            from bn_magazines a
            where a.decision like 'OK'
            group by a.pbl_id, a.pbl_magazine, a.year) q2
            on q1.pbl_id||'|'||q1.year = q2.pbl_id||'|'||q2.year
            order by q1.pbl_magazine, q1.year
            """
bn_stats = pandasql.sqldf(bn_query)
bn_stats['procent literacki BN'] = bn_stats['liczba BN ok'] / bn_stats['liczba BN'] * 100
bn_stats = bn_stats[bn_stats['pbl_magazine'].notnull()]

# bn_stats.loc[bn_stats['pbl_magazine'] == 'Zeszyty Naukowe Państwowej Wyższej Szkoły Zawodowej w Koninie. Zeszyt Naukowy Instytutu  Neofilologii', 'pbl_id'] = 16599

pbl_ids = bn_stats.copy()['pbl_id'].drop_duplicates().astype(int).tolist()

pbl_query = """select z.za_zapis_id "record_id", zr.zr_zrodlo_id "pbl_id", z.za_ro_rok "year"
            from pbl_zapisy z
            join IBL_OWNER.pbl_zrodla zr on zr.zr_zrodlo_id=z.za_zr_zrodlo_id"""

pbl_stats = pd.read_sql(pbl_query, connection)
pbl_stats = pbl_stats[pbl_stats['pbl_id'].isin(pbl_ids)]
pbl_stats = pbl_stats.groupby(['pbl_id', 'year']).count()
pbl_stats = pbl_stats.reset_index(level=['pbl_id', 'year']).rename(columns={'record_id':'liczba PBL'})

bn_stats['pbl_id'] = bn_stats['pbl_id'].astype(np.int64)
bn_stats['help'] = bn_stats['pbl_id'].astype(str) + '|' + bn_stats['year']
pbl_stats['help'] = pbl_stats['pbl_id'].astype(str) + '|' + pbl_stats['year'].astype(str)

stats = pd.merge(bn_stats, pbl_stats, 'left', 'help')
stats = stats[['help', 'pbl_magazine', 'liczba BN', 'liczba BN ok', 'procent literacki BN', 'liczba PBL']]
stats['index'] = stats.index + 1
stats = cSplit(stats, 'index', 'help', '|', 'wide')
stats = stats.rename(columns={'help_0':'pbl_id', 'help_1':'year'}).drop(columns=['index']).sort_values(['pbl_magazine', 'year'])

stats.to_excel('statystyki_czasopism_bn_pbl.xlsx', index=False)
