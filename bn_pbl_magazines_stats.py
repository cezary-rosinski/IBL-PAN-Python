import pandas as pd
import numpy as np
from my_functions import marc_parser_1_field
import pandasql
from my_functions import cSplit
from my_functions import gsheet_to_df
import cx_Oracle


pbl_bn_magazines = bn_magazines = gsheet_to_df('10-QUomq_H8v06H-yUhjO60hm45Wbo9DanJTemTgSUrA', 'Sheet1')[['ZR_ZRODLO_ID', 'ZRODLA_PBL', 'ZRODLA_BN']]
pbl_bn_magazines = cSplit(pbl_bn_magazines, 'ZR_ZRODLO_ID', 'ZRODLA_BN', ' \| ')

bn_magazines = pd.read_excel('bn_magazines_to_statistics.xlsx')[['id', '008', '773', 'decision']]

bn_magazine_title = marc_parser_1_field(bn_magazines, 'id', '773', '\$')[['id', '$t']]
bn_magazine_title.columns = ['id', 'magazine_title']
bn_magazine_title['magazine_title'] = bn_magazine_title['magazine_title'].str.replace('\.$', '')
bn_magazines = pd.merge(bn_magazines, bn_magazine_title, how='left', on='id')
bn_magazines['year'] = bn_magazines['008'].apply(lambda x: x[7:11])
bn_magazines = pd.merge(bn_magazines, pbl_bn_magazines, 'left', left_on='magazine_title', right_on='ZRODLA_BN')
bn_magazines = bn_magazines[['id', 'ZR_ZRODLO_ID', 'ZRODLA_PBL', 'year', 'decision']]
bn_magazines = bn_magazines.rename(columns={'ZRODLA_PBL': 'magazine_title'})

bn_query = """
        select q1.ZR_ZRODLO_ID, q1.magazine_title, q1.year, q1."liczba BN", q2."liczba BN ok"
        from
        (select a.ZR_ZRODLO_ID, a.magazine_title, a.year, count (*) "liczba BN"
        from bn_magazines a
        group by a.ZR_ZRODLO_ID, a.magazine_title, a.year) q1
        left join
        (select a.ZR_ZRODLO_ID, a.magazine_title, a.year, count (*) "liczba BN ok"
        from bn_magazines a
        where a.decision like 'OK'
        group by a.ZR_ZRODLO_ID, a.magazine_title, a.year) q2
        on q1.ZR_ZRODLO_ID||'|'||q1.year = q2.ZR_ZRODLO_ID||'|'||q2.year
        order by q1.magazine_title, q1.year
        """

bn_stats = pandasql.sqldf(bn_query)
bn_stats['procent literacki'] = bn_stats['liczba BN ok'] / bn_stats['liczba BN'] * 100

pbl_ids = bn_stats.copy()['ZR_ZRODLO_ID'].drop_duplicates().astype(int).tolist()

dsn_tns = cx_Oracle.makedsn('pbl.ibl.poznan.pl', '1521', service_name='xe')
connection = cx_Oracle.connect(user='IBL_SELECT', password='CR333444', dsn=dsn_tns, encoding='windows-1250')

pbl_query = """select z.za_zapis_id, zr.zr_zrodlo_id, z.za_ro_rok
            from pbl_zapisy z
            join IBL_OWNER.pbl_zrodla zr on zr.zr_zrodlo_id=z.za_zr_zrodlo_id"""

pbl_stats = pd.read_sql(pbl_query, connection)
pbl_stats = pbl_stats[pbl_stats['ZR_ZRODLO_ID'].isin(pbl_ids)]
pbl_stats['ZA_RO_ROK'] = pbl_stats['ZA_RO_ROK'].astype(object)
pbl_bn_magazines['ZR_ZRODLO_ID'] = pbl_bn_magazines['ZR_ZRODLO_ID'].astype(np.int64)
pbl_stats = pd.merge(pbl_stats, pbl_bn_magazines[['ZR_ZRODLO_ID', 'ZRODLA_PBL']].drop_duplicates(), how='left', on='ZR_ZRODLO_ID')

pbl_stats = pbl_stats.groupby(['ZR_ZRODLO_ID', 'ZRODLA_PBL', 'ZA_RO_ROK']).count()
pbl_stats = pbl_stats.reset_index(level=['ZR_ZRODLO_ID', 'ZRODLA_PBL', 'ZA_RO_ROK'])
pbl_stats.columns = ['ZR_ZRODLO_ID', 'magazine_title', 'year', 'liczba PBL']

bn_stats['ZR_ZRODLO_ID'] = bn_stats['ZR_ZRODLO_ID'].astype(np.int64)
pbl_stats['year'] = pbl_stats['year'].astype(object)

bn_stats['help'] = bn_stats['ZR_ZRODLO_ID'].astype(str) + '|' + bn_stats['magazine_title'] + '|' + bn_stats['year']
pbl_stats['help'] = pbl_stats['ZR_ZRODLO_ID'].astype(str) + '|' + pbl_stats['magazine_title'] + '|' + pbl_stats['year'].astype(str)

stats = pd.merge(bn_stats, pbl_stats, 'left', 'help')
stats = stats[['help', 'liczba BN', 'liczba BN ok', 'liczba PBL']]
stats['index'] = stats.index + 1
stats = cSplit(stats, 'index', 'help', '|', 'wide')
stats = stats.rename(columns={'help_0':'ZR_ZRODLO_ID', 'help_1':'magazine_title', 'help_2':'year'}).drop(columns=['index'])

stats.to_excel('statystyki_czasopism_bn_pbl.xlsx', index=False)
