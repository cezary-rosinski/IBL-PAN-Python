import pandas as pd
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import gspread as gs
import datetime
import regex as re
from collections import OrderedDict
from gspread_dataframe import set_with_dataframe, get_as_dataframe
import numpy as np
from my_functions import cosine_sim_2_elem, marc_parser_1_field, gsheet_to_df, xml_to_mrk, cSplit, f, df_to_mrc, mrc_to_mrk, mrk_to_df, xml_to_mrk, mrk_to_mrc, get_cosine_result, df_to_gsheet, cluster_strings, cluster_records, simplify_string
import unidecode
import pandasql
import time
from google_drive_research_folders import cr_projects
from functools import reduce
import sys
import csv
from tqdm import tqdm
import json

now = datetime.datetime.now()
year = now.year
month = now.month
day = now.day

#autoryzacja do tworzenia i edycji plików
gc = gs.oauth()
#autoryzacja do penetrowania dysku
gauth = GoogleAuth()
gauth.LocalWebserverAuth()
drive = GoogleDrive(gauth)
#%% google drive
file_list = drive.ListFile({'q': f"'{cr_projects}' in parents and trashed=false"}).GetList() 
#[print(e['title'], e['id']) for e in file_list]
translation_folder = [file['id'] for file in file_list if file['title'] == 'Vimr Project'][0]
file_list = drive.ListFile({'q': f"'{translation_folder}' in parents and trashed=false"}).GetList() 
#[print(e['title'], e['id']) for e in file_list]
cz_authority_spreadsheet = [file['id'] for file in file_list if file['title'] == 'cz_authority'][0]
cz_authority_spreadsheet = gc.open_by_key(cz_authority_spreadsheet)

cz_authority_spreadsheet.worksheets()

#%% load data
list_of_records = []
with open('F:/Cezary/Documents/IBL/Translations/OCLC/Czech origin_trans/oclc_lang.csv', 'r', encoding="utf8", errors="surrogateescape") as csv_file:
#with open('C:/Users/User/Desktop/oclc_lang.csv', 'r', encoding="utf8", errors="surrogateescape") as csv_file:
    reader = csv.reader(csv_file, delimiter=',')
    headers = next(reader)
    position_008 = headers.index('008')
    for row in reader:
        if row[position_008][35:38] != 'cze':
            list_of_records.append(row)
            
oclc_lang = pd.DataFrame(list_of_records, columns=headers)
oclc_lang['language'] = oclc_lang['008'].apply(lambda x: x[35:38])
oclc_viaf = pd.read_excel('F:/Cezary/Documents/IBL/Translations/OCLC/Czech viaf/oclc_viaf.xlsx')
#oclc_viaf = pd.read_excel('C:/Users/User/Desktop/oclc_viaf.xlsx')
oclc_viaf['language'] = oclc_viaf['008'].apply(lambda x: x[35:38])
oclc_viaf = oclc_viaf[oclc_viaf['language'] != 'cze']

oclc_other_languages = pd.concat([oclc_lang, oclc_viaf])

oclc_other_languages['nature_of_contents'] = oclc_other_languages['008'].apply(lambda x: x[24:27])
oclc_other_languages = oclc_other_languages[oclc_other_languages['nature_of_contents'].isin(['\\\\\\', '6\\\\', '\\6\\', '\\\\6'])]
oclc_other_languages['type of record + bibliographic level'] = oclc_other_languages['LDR'].apply(lambda x: x[6:8])
oclc_other_languages['fiction_type'] = oclc_other_languages['008'].apply(lambda x: x[33])

cz_authority_df = get_as_dataframe(cz_authority_spreadsheet.worksheet('Sheet1'), evaluate_formulas=True).dropna(how='all').dropna(how='all', axis=1)

viaf_positives = cz_authority_df['viaf_positive'].drop_duplicates().dropna().to_list()
viaf_positives = [f"http://viaf.org/viaf/{l}" for l in viaf_positives if l]

positive_viafs_names = cz_authority_df[cz_authority_df['viaf_positive'].notnull()][['viaf_positive', 'all_names']]
positive_viafs_names = cSplit(positive_viafs_names, 'viaf_positive', 'all_names', '❦')
positive_viafs_names['all_names'] = positive_viafs_names['all_names'].apply(lambda x: re.sub('(.*?)(\$a.*?)(\$0.*$)', r'\2', x) if pd.notnull(x) else np.nan)
positive_viafs_names = positive_viafs_names[positive_viafs_names['all_names'].notnull()].drop_duplicates()

positive_viafs_diacritics = cz_authority_df[cz_authority_df['viaf_positive'].notnull()][['viaf_positive', 'cz_name']]
positive_viafs_diacritics['cz_name'] = positive_viafs_diacritics['cz_name'].apply(lambda x: unidecode.unidecode(x))

viaf_positives_dict = {}
for element in viaf_positives:
    viaf_positives_dict[re.findall('\d+', element)[0]] = {'viaf id':element}
for i, row in positive_viafs_names.iterrows():
    if 'form of name' in viaf_positives_dict[row['viaf_positive']]:
        viaf_positives_dict[row['viaf_positive']]['form of name'].append(row['all_names'])
    else:
        viaf_positives_dict[row['viaf_positive']].update({'form of name':[row['all_names']]})
for i, row in positive_viafs_diacritics.iterrows():
    if 'unidecode name' in viaf_positives_dict[row['viaf_positive']]:
        viaf_positives_dict[row['viaf_positive']]['unidecode name'].append(row['cz_name'])
    else:
        viaf_positives_dict[row['viaf_positive']].update({'unidecode name':[row['cz_name']]})
        
viaf_positives_dict = dict(sorted(viaf_positives_dict.items(), key = lambda item : len(item[1]['unidecode name']), reverse=True))

with open("viaf_positives_dict.json", 'w', encoding='utf-8') as file: 
    json.dump(viaf_positives_dict, file, ensure_ascii=False, indent=4)

# oclc_other_languages['language'].drop_duplicates().sort_values().to_list()

#%% de-duplication
fiction_types = ['1', 'd', 'f', 'h', 'j', 'p']
languages = ['pol', 'swe', 'ita', 'spa']
#languages = ['ita']

now = datetime.datetime.now()

for language in languages:
    print(language)
    df = oclc_other_languages[(oclc_other_languages['language'] == language)]
    df_language_materials_monographs = df[df['type of record + bibliographic level'] == 'am']
    negative = df_language_materials_monographs.copy()
    df_other_types = df[~df['001'].isin(df_language_materials_monographs['001'])]
    #df_other_types.to_excel(f"oclc_{language}_other_types_(first_negative).xlsx", index=False)
    df_first_positive = df_language_materials_monographs[(df_language_materials_monographs['041'].str.contains('\$hcz')) &
                                                         (df_language_materials_monographs['fiction_type'].isin(fiction_types))]
    negative = negative[~negative['001'].isin(df_first_positive['001'])]
    #df_first_positive.to_excel(f"oclc_{language}_positive.xlsx", index=False)
    df_second_positive = marc_parser_1_field(negative, '001', '100', '\$')[['001', '$1']]
    df_second_positive = df_second_positive[df_second_positive['$1'].isin(viaf_positives)]
    df_second_positive = negative[negative['001'].isin(df_second_positive['001'])]
    #df_second_positive.to_excel(f"oclc_{language}_second_positive.xlsx", index=False)
    negative = negative[~negative['001'].isin(df_second_positive['001'])].reset_index(drop=True)
    
    df_third_positive = "select * from negative a join positive_viafs_names b on a.'100' like '%'||b.all_names||'%'"
    df_third_positive = pandasql.sqldf(df_third_positive)
    
    negative = negative[~negative['001'].isin(df_third_positive['001'])].reset_index(drop=True)

    df_fourth_positive = "select * from negative a join positive_viafs_diacritics b on a.'100' like '%'||b.cz_name||'%'"
    df_fourth_positive = pandasql.sqldf(df_fourth_positive)

    negative = negative[~negative['001'].isin(df_fourth_positive['001'])].reset_index(drop=True)
    #negative.to_excel(f"oclc_{language}_negative.xlsx", index=False)
    
    df_all_positive = pd.concat([df_first_positive, df_second_positive, df_third_positive, df_fourth_positive])
    #df_all_positive.to_excel(f"oclc_{language}_df_all_positive.xlsx", index=False)
    
    df_all_positive['260'] = df_all_positive[['260', '264']].apply(lambda x: x['260'] if pd.notnull(x['260']) else x['264'], axis=1)
    df_all_positive['240'] = df_all_positive[['240', '246']].apply(lambda x: x['240'] if pd.notnull(x['240']) else x['246'], axis=1)
    df_all_positive['100_unidecode'] = df_all_positive['100'].apply(lambda x: unidecode.unidecode(x).lower() if pd.notnull(x) else x)
    
    authors = ['Hasek', 
               'Hrabal', 
               'Capek']
    
    year = now.year
    month = now.month
    day = now.day

    
    for i, author in enumerate(authors):
        sheet = gc.create(f'{author}_{language}_{year}-{month}-{day}', translation_folder)
        s = gp.Spread(sheet.id, creds=credentials)
        authors[i] = [authors[i]]
        authors[i] += [sheet.id, s]
    
    for index, (author, g_id, g_sheet) in enumerate(authors):
        # author = authors[0][0]
        # g_id = authors[0][1]
        # g_sheet = authors[0][2]
        print(f"{index+1}/{len(authors)}")
        
        #all
        
        df_oclc = df_all_positive[(df_all_positive['100_unidecode'].notnull()) &       
                                  (df_all_positive['100_unidecode'].str.contains(author.lower()))].reset_index(drop=True)
        df_oclc['001'] = df_oclc['001'].astype(int)
        sh = gc.open_by_key(g_id)
        wsh = sh.get_worksheet(0)
        wsh.update_title('all')
        g_sheet.df_to_sheet(df_oclc, sheet='all', index=0)
        
        #de-duplication 1: duplicates
        try:
            title = marc_parser_1_field(df_oclc, '001', '245', '\$')[['001', '$a', '$b', '$n', '$p']].replace(r'^\s*$', np.nan, regex=True)
        except KeyError:
            title = marc_parser_1_field(df_oclc, '001', '245', '\$')[['001', '$a', '$b']].replace(r'^\s*$', np.nan, regex=True)
        title['title'] = title[title.columns[1:]].apply(lambda x: simplify_string(x, with_spaces=False), axis=1)    
        title = title[['001', 'title']]
        df_oclc = pd.merge(df_oclc, title, how='left', on='001')
        
        place = marc_parser_1_field(df_oclc, '001', '260', '\$')[['001', '$a']].rename(columns={'$a':'place'})
        place = place[place['place'] != '']
        place['place'] = place['place'].apply(lambda x: simplify_string(x, with_spaces=False))
        df_oclc = pd.merge(df_oclc, place, how='left', on='001')
        
        publisher = marc_parser_1_field(df_oclc, '001', '260', '\$')[['001', '$b']].rename(columns={'$b':'publisher'})
        publisher = publisher.groupby('001').head(1).reset_index(drop=True)
        publisher['publisher'] = publisher['publisher'].apply(lambda x: simplify_string(x, with_spaces=False))
        df_oclc = pd.merge(df_oclc, publisher, how='left', on='001')
        
        
        year = df_oclc.copy()[['001', '008']].rename(columns={'008':'year'})
        year['year'] = year['year'].apply(lambda x: x[7:11])
        df_oclc = pd.merge(df_oclc, year, how='left', on='001')
        
        df_oclc_duplicates = pd.DataFrame()
        df_oclc_grouped = df_oclc.groupby(['title', 'place', 'year'])
        for name, group in df_oclc_grouped:
            if len(group) > 1:
                group['groupby'] = str(name)
                group_ids = '❦'.join([str(e) for e in group['001'].to_list()])
                group['group_ids'] = group_ids
                df_oclc_duplicates = df_oclc_duplicates.append(group)
        df_oclc_duplicates = df_oclc_duplicates.drop_duplicates()
        
        oclc_duplicates_list = df_oclc_duplicates['001'].drop_duplicates().tolist()
        df_oclc_duplicates_grouped = df_oclc_duplicates.groupby(['title', 'place', 'year'])
        
        df_oclc_deduplicated = pd.DataFrame()
        for name, group in df_oclc_duplicates_grouped:
            for column in group:
                if column in ['fiction_type', '490', '500', '650', '655']:
                    group[column] = '❦'.join(group[column].dropna().drop_duplicates().astype(str))
                else:
                    group[column] = group[column].dropna().astype(str).max()
            df_oclc_deduplicated = df_oclc_deduplicated.append(group)
        
        df_oclc_deduplicated = df_oclc_deduplicated.drop_duplicates().replace(r'^\s*$', np.nan, regex=True)
        df_oclc_deduplicated['001'] = df_oclc_deduplicated['001'].astype(int)
        
        df_oclc = df_oclc[~df_oclc['001'].isin(oclc_duplicates_list)]
        df_oclc = pd.concat([df_oclc, df_oclc_deduplicated]).drop(columns='title')
        df_oclc['group_ids'] = df_oclc['group_ids'].apply(lambda x: '❦'.join(set(x.split('❦'))) if pd.notnull(x) else x)
        g_sheet.df_to_sheet(df_oclc, sheet='after_removing_duplicates', index=0)
        
        #de-duplication 2: multiple volumes
        
        title = marc_parser_1_field(df_oclc, '001', '245', '\$')[['001', '$a']].replace(r'^\s*$', np.nan, regex=True)
        title['title'] = title[title.columns[1:]].apply(lambda x: simplify_string(x, with_spaces=False), axis=1)    
        title = title[['001', 'title']]
        df_oclc = pd.merge(df_oclc, title, how='left', on='001')  
        
        df_oclc_grouped = df_oclc.groupby(['title', 'place', 'year'])
            
        df_oclc_multiple_volumes = pd.DataFrame()
        for name, group in df_oclc_grouped:
            if len(group[group['245'].str.contains('\$n', regex=True)]):
                group['groupby'] = str(name)
                group_ids = '❦'.join(set([str(e) for e in group['001'].to_list() + group['group_ids'].to_list() if pd.notnull(e)]))
                group['group_ids'] = group_ids
                df_oclc_multiple_volumes = df_oclc_multiple_volumes.append(group)
                
        if df_oclc_multiple_volumes.shape[0] > 0:
            oclc_multiple_volumes_list = df_oclc_multiple_volumes['001'].drop_duplicates().tolist()
            df_oclc_multiple_volumes_grouped = df_oclc_multiple_volumes.groupby(['title', 'place', 'year'])
        
            df_oclc_multiple_volumes_deduplicated = pd.DataFrame()
            for name, group in df_oclc_multiple_volumes_grouped:
                if len(group[~group['245'].str.contains('\$n', regex=True)]) == 1:
                    for column in group:
                        if column in ['fiction_type', '490', '500', '650', '655']:
                            group[column] = '❦'.join(group[column].dropna().drop_duplicates().astype(str))  
                        elif column in ['001', '245']:
                            pass
                        else:
                            group[column] = group[column].dropna().astype(str).max()
                    df = group[~group['245'].str.contains('\$n', regex=True)]
                    df_oclc_multiple_volumes_deduplicated = df_oclc_multiple_volumes_deduplicated.append(df)
                else:
                    for column in group:
                        if column in ['fiction_type', '490', '500', '650', '655']:
                            group[column] = '❦'.join(group[column].dropna().drop_duplicates().astype(str))
                        elif column == '245':
                            field_245 = marc_parser_1_field(group, '001', '245', '\$').replace(r'^\s*$', np.nan, regex=True)
                            field_245 = field_245.iloc[:, lambda x: x.columns.isin(['001', '$a', '$b', '$c'])]
                            field_245['245'] = field_245[field_245.columns[1:]].apply(lambda x: ''.join(x.dropna().astype(str)), axis=1)
                            field_245 = field_245[['001', '245']]
                            field_245['245'] = '10' + field_245['245']
                            group = pd.merge(group.drop(columns='245'), field_245, how='left', on='001')
                            group[column] = group[column].dropna().astype(str).max()
                        else:
                            group[column] = group[column].dropna().astype(str).max()
                    group = group.drop_duplicates().reset_index(drop=True)
                    df_oclc_multiple_volumes_deduplicated = df_oclc_multiple_volumes_deduplicated.append(group)
                        
            df_oclc_multiple_volumes_deduplicated = df_oclc_multiple_volumes_deduplicated.drop_duplicates().replace(r'^\s*$', np.nan, regex=True)
            df_oclc_multiple_volumes_deduplicated['001'] = df_oclc_multiple_volumes_deduplicated['001'].astype(int)
            
            df_oclc = df_oclc[~df_oclc['001'].isin(oclc_multiple_volumes_list)]
            df_oclc = pd.concat([df_oclc, df_oclc_multiple_volumes_deduplicated]).drop_duplicates().reset_index(drop=True)
            df_oclc['group_ids'] = df_oclc['group_ids'].apply(lambda x: '❦'.join(set(x.split('❦'))) if pd.notnull(x) else x)
            g_sheet.df_to_sheet(df_oclc, sheet='after_removing_multiple_volumes', index=0)
            
        #de-duplication 3: fuzzyness
        df_oclc.drop(columns='title', inplace=True)
        field_245 = marc_parser_1_field(df_oclc, '001', '245', '\$').replace(r'^\s*$', np.nan, regex=True)
        field_245['$a'] = field_245.apply(lambda x: x['$a'] if pd.notnull(x['$a']) else x['indicator'][2:].split('.', 1)[0], axis=1)
        field_245 = field_245.iloc[:, lambda x: x.columns.isin(['001', '$a', '$b'])]
        field_245['title'] = field_245[field_245.columns[1:]].apply(lambda x: simplify_string(x), axis=1)  
        field_245 = field_245[['001', 'title']]
        df_oclc = pd.merge(df_oclc, field_245, how='left', on='001')
        
        #similarity level == 0.85 | columns == ['title', 'publisher', 'year'] | same 'year'
        df_oclc_clusters = cluster_records(df_oclc, '001', ['title', 'publisher', 'year'], 0.85)    
        df_oclc_clusters = df_oclc_clusters[df_oclc_clusters['publisher'] != '']
        df_oclc_duplicates = df_oclc_clusters.groupby(['cluster', 'year']).filter(lambda x: len(x) > 1)
        
        if df_oclc_duplicates.shape[0] > 0:
     
            if df_oclc_duplicates['001'].value_counts().max() > 1:
                sys.exit('ERROR!!!\nclustering problem!!!')
        
            oclc_duplicates_list = df_oclc_duplicates['001'].drop_duplicates().tolist()
            df_oclc_duplicates = df_oclc_duplicates.groupby('cluster')
            
            df_oclc_deduplicated = pd.DataFrame()
            for name, group in df_oclc_duplicates:
                group_ids = '❦'.join(set([str(e) for e in group['001'].to_list() + group['group_ids'].to_list() if pd.notnull(e)]))
                group['group_ids'] = group_ids
                for column in group:
                    if column in ['fiction_type', '490', '500', '650', '655']:
                        group[column] = '❦'.join(group[column].dropna().drop_duplicates().astype(str))
                    elif column == '245':
                        group[column] = group[column][group[column].str.contains('$', regex=False)]
                        group[column] = group[column].dropna().astype(str).max()
                    else:
                        group[column] = group[column].dropna().astype(str).max()
                df_oclc_deduplicated = df_oclc_deduplicated.append(group)
                
            df_oclc_deduplicated = df_oclc_deduplicated.drop_duplicates().replace(r'^\s*$', np.nan, regex=True)
            df_oclc_deduplicated['001'] = df_oclc_deduplicated['001'].astype(int)
            df_oclc = df_oclc[~df_oclc['001'].isin(oclc_duplicates_list)]
            df_oclc = pd.concat([df_oclc, df_oclc_deduplicated])
            df_oclc['group_ids'] = df_oclc['group_ids'].apply(lambda x: '❦'.join(set(x.split('❦'))) if pd.notnull(x) else x)
            g_sheet.df_to_sheet(df_oclc, sheet='after_fuzzy_duplicates_0.85_tit_pub_year', index=0)
            
    # =============================================================================
    #     #similarity level == 0.8 | columns == ['title', 'publisher'] | same 'year'
    #     df_oclc.drop(columns='cluster', inplace=True)
    #     df_oclc_clusters = cluster_records(df_oclc, '001', ['title', 'place', 'publisher', 'year'], 0.85) 
    #     df_oclc_duplicates = df_oclc_clusters.groupby(['cluster', 'year']).filter(lambda x: len(x) > 1)
    #     
    #     df_oclc_duplicates[['001', '245', 'year', '260']].to_excel('test_oclc1.xlsx', index=False)
    #     
    #     if df_oclc_duplicates.shape[0] > 0:
    #     
    #         if df_oclc_duplicates['001'].value_counts().max() > 1:
    #             sys.exit('ERROR!!!\nclustering problem!!!')
    #     
    #         oclc_duplicates_list = df_oclc_duplicates['001'].drop_duplicates().tolist()
    #         df_oclc_duplicates = df_oclc_duplicates.groupby('cluster')
    #         
    #         df_oclc_deduplicated = pd.DataFrame()
    #         for name, group in df_oclc_duplicates:
    #             group_ids = '❦'.join(set([str(e) for e in group['001'].to_list() + group['group_ids'].to_list() if pd.notnull(e)]))
    #             group['group_ids'] = group_ids
    #             for column in group:
    #                 if column in ['fiction_type', '490', '500', '650', '655']:
    #                     group[column] = '❦'.join(group[column].dropna().drop_duplicates().astype(str))
    #                 elif column == '245':
    #                     group[column] = group[column][group[column].str.contains('$', regex=False)]
    #                     group[column] = group[column].dropna().astype(str).max()
    #                 else:
    #                     group[column] = group[column].dropna().astype(str).max()
    #             df_oclc_deduplicated = df_oclc_deduplicated.append(group)
    #             
    #         df_oclc_deduplicated = df_oclc_deduplicated.drop_duplicates().replace(r'^\s*$', np.nan, regex=True)
    #         df_oclc_deduplicated['001'] = df_oclc_deduplicated['001'].astype(int)
    #         df_oclc = df_oclc[~df_oclc['001'].isin(oclc_duplicates_list)]
    #         df_oclc = pd.concat([df_oclc, df_oclc_deduplicated])
    #         df_oclc['group_ids'] = df_oclc['group_ids'].apply(lambda x: '❦'.join(set(x.split('❦'))) if pd.notnull(x) else x)
    #         g_sheet.df_to_sheet(df_oclc, sheet='after_fuzzy_duplicates_0.85_tit_pub_year', index=0)
    # =============================================================================
        
        #editions counter
        edition_clusters = cluster_strings(df_oclc['title'], 0.7)
        edition_clusters_df = pd.DataFrame()
        for k, v in edition_clusters.items():
            df = df_oclc.copy()[df_oclc['title'].str.strip().isin(v)]
            df['edition_cluster'] = k
            edition_clusters_df = edition_clusters_df.append(df)
        edition_clusters_df['edition_index'] = edition_clusters_df.groupby('edition_cluster').cumcount()+1
        df_oclc = edition_clusters_df.copy()
        g_sheet.df_to_sheet(df_oclc, sheet='final_marc21_with_editions_counters', index=0)
           
        #simplify the records
        df_oclc = df_oclc[['001', '080', '100', '245', '240', '260', '650', '655', '700', 'language', 'fiction_type', 'place', 'year', 'edition_cluster', 'edition_index']]
        df_oclc['001'] = df_oclc['001'].astype(int)
        
        identifiers = df_oclc[['001']]
        udc = marc_parser_1_field(df_oclc, '001', '080', '\$')[['001', '$a']].rename(columns={'$a':'universal decimal classification'})
        udc['universal decimal classification'] = udc.groupby('001')['universal decimal classification'].transform(lambda x: '❦'.join(x.dropna().drop_duplicates().astype(str)))
        udc = udc.drop_duplicates().reset_index(drop=True)
        marc_author = marc_parser_1_field(df_oclc, '001', '100', '\$')[['001', '$a', '$d', '$1']].rename(columns={'$a':'author name', '$d':'author birth and death', '$1':'author viaf id'})
        for column in marc_author.columns[1:]:
            marc_author[column] = marc_author.groupby('001')[column].transform(lambda x: '❦'.join(x.dropna().drop_duplicates().astype(str)))
        marc_author = marc_author.drop_duplicates().reset_index(drop=True)
        title = marc_parser_1_field(df_oclc, '001', '245', '\$').replace(r'^\s*$', np.nan, regex=True)
        title['$a'] = title.apply(lambda x: x['245'] if pd.isnull(x['$a']) else x['$a'], axis=1)
        title = title.iloc[:, lambda x: x.columns.isin(['001', '$a', '$b'])]
        title['title'] = title[title.columns[1:]].apply(lambda x: ''.join(x.dropna().astype(str)), axis=1)
        title = title[['001', 'title']]
        original_title = marc_parser_1_field(df_oclc, '001', '240', '\$').replace(r'^\s*$', np.nan, regex=True)[['001', '$a']].rename(columns={'$a':'original title'})
        place_of_publication = marc_parser_1_field(df_oclc, '001', '260', '\$').replace(r'^\s*$', np.nan, regex=True)[['001', '$a']].rename(columns={'$a':'place of publication'})
        #$e as alternative place of publication?
        try:
            contributor = marc_parser_1_field(df_oclc, '001', '700', '\$').replace(r'^\s*$', np.nan, regex=True)[['001', '$a', '$d', '$e', '$1']].rename(columns={'$a':'contributor name', '$d':'contributor birth and death', '$1':'contributor viaf id', '$e':'contributor role'})
            contributor['contributor role'] = contributor['contributor role'].apply(lambda x: x if pd.notnull(x) else 'unknown')
        except KeyError:
            contributor['contributor role'] = 'unknown'
            
        dfs = [identifiers, udc, marc_author, title, original_title, contributor, df_oclc[['001', '650', '655', 'language', 'fiction_type', 'place', 'year', 'edition_cluster', 'edition_index']]]
        df_oclc_final = reduce(lambda left,right: pd.merge(left,right,on='001', how='outer'), dfs).drop_duplicates()
        g_sheet.df_to_sheet(df_oclc_final, sheet='simplified shape', index=0)
        time.sleep(60)
        
        
#%% clusters for original titles        
fiction_types = ['1', 'd', 'f', 'h', 'j', 'p']

df = oclc_other_languages.copy()
df_language_materials_monographs = df[df['type of record + bibliographic level'] == 'am']
negative = df_language_materials_monographs.copy()
df_other_types = df[~df['001'].isin(df_language_materials_monographs['001'])]
df_first_positive = df_language_materials_monographs[(df_language_materials_monographs['041'].str.contains('\$hcz')) &
                                                     (df_language_materials_monographs['fiction_type'].isin(fiction_types))]
negative = negative[~negative['001'].isin(df_first_positive['001'])]
df_second_positive = marc_parser_1_field(negative, '001', '100', '\$')[['001', '$1']]
df_second_positive = df_second_positive[df_second_positive['$1'].isin(viaf_positives)]
df_second_positive = negative[negative['001'].isin(df_second_positive['001'])]

negative = negative[~negative['001'].isin(df_second_positive['001'])].reset_index(drop=True)
df_third_positive = "select * from negative a join positive_viafs_names b on a.'100' like '%'||b.all_names||'%'"
df_third_positive = pandasql.sqldf(df_third_positive)

negative = negative[~negative['001'].isin(df_third_positive['001'])].reset_index(drop=True)
df_fourth_positive = "select * from negative a join positive_viafs_diacritics b on a.'100' like '%'||b.cz_name||'%'"
df_fourth_positive = pandasql.sqldf(df_fourth_positive)

negative = negative[~negative['001'].isin(df_fourth_positive['001'])].reset_index(drop=True)
df_all_positive = pd.concat([df_first_positive, df_second_positive, df_third_positive, df_fourth_positive])

df_all_positive['260'] = df_all_positive[['260', '264']].apply(lambda x: x['260'] if pd.notnull(x['260']) else x['264'], axis=1)
df_all_positive['240'] = df_all_positive[['240', '246']].apply(lambda x: x['240'] if pd.notnull(x['240']) else x['246'], axis=1)
df_all_positive['100_unidecode'] = df_all_positive['100'].apply(lambda x: unidecode.unidecode(x).lower() if pd.notnull(x) else x)

df_oclc_people = marc_parser_1_field(df_all_positive, '001', '100_unidecode', '\\$')[['001', '$a', '$d', '$1']].replace(r'^\s*$', np.nan, regex=True)  
df_oclc_people['$ad'] = df_oclc_people[['$a', '$d']].apply(lambda x: '$d'.join(x.dropna().astype(str)) if pd.notnull(x['$d']) else x['$a'], axis=1)
df_oclc_people['$ad'] = '$a' + df_oclc_people['$ad']
df_oclc_people['simplify string'] = df_oclc_people['$a'].apply(lambda x: simplify_string(x))
df_oclc_people['001'] = df_oclc_people['001'].astype(int)

people_clusters = viaf_positives_dict.copy()
for key in tqdm(people_clusters, total=len(people_clusters)):
    viaf_id = people_clusters[key]['viaf id']
    records = []
    records_1 = df_oclc_people[df_oclc_people['$1'] == viaf_id]['001'].to_list()
    records += records_1
    try:
        unidecode_lower_forms_of_names = [simplify_string(e) for e in people_clusters[key]['form of name']]
        records_2 = df_oclc_people[df_oclc_people['$ad'].isin(unidecode_lower_forms_of_names)]['001'].to_list()
        records += records_2
    except KeyError:
        pass
    try:
        unidecode_name = [simplify_string(e) for e in people_clusters[key]['unidecode name']]
        records_3 = df_oclc_people[df_oclc_people['simplify string'].isin(unidecode_name)]['001'].to_list()
        records += records_3
    except KeyError:
        pass
    records = list(set(records))
    people_clusters[key].update({'list of records':records})
    
people_clusters_records = {}
for key in people_clusters:
    people_clusters_records[key] = people_clusters[key]['list of records']
    
df_people_clusters = pd.DataFrame.from_dict(people_clusters_records, orient='index').stack().reset_index(level=0).rename(columns={'level_0':'cluster_viaf', 0:'001'})
df_people_clusters['001'] = df_people_clusters['001'].astype('int64')
df_all_positive['001'] = df_all_positive['001'].astype('int64')
df_people_clusters = df_people_clusters.merge(df_all_positive, how='left', on='001').drop(columns=['all_names', 'cz_name', 'viaf_positive']).drop_duplicates().reset_index(drop=True)

multiple_clusters = df_people_clusters['001'].value_counts().reset_index()
multiple_clusters = multiple_clusters[multiple_clusters['001'] > 1]['index'].to_list()
df_multiple_clusters = df_people_clusters[df_people_clusters['001'].isin(multiple_clusters)].sort_values('001')
multiple_clusters_ids = df_multiple_clusters['001'].drop_duplicates().to_list()
df_people_clusters = df_people_clusters[~df_people_clusters['001'].isin(multiple_clusters_ids)]
df_multiple_clusters = df_multiple_clusters[df_multiple_clusters['cluster_viaf'] == '34454129']
df_people_clusters = pd.concat([df_people_clusters, df_multiple_clusters])

df_original_titles = df_people_clusters.replace(r'^\s*$', np.nan, regex=True)    
df_original_titles = df_original_titles[(df_original_titles['240'].notnull()) & (df_original_titles['100'].notnull())][['001', '100', '240', 'cluster_viaf']]
df_original_titles_100 = marc_parser_1_field(df_original_titles, '001', '100', '\\$')[['001', '$a', '$d', '$1']].rename(columns={'$a':'name', '$d':'dates', '$1':'viaf'})
counter_100 = df_original_titles_100['001'].value_counts().reset_index()
counter_100 = counter_100[counter_100['001'] == 1]['index'].to_list()
df_original_titles_100 = df_original_titles_100[df_original_titles_100['001'].isin(counter_100)]
df_original_titles_240 = marc_parser_1_field(df_original_titles, '001', '240', '\\$')
df_original_titles_240['original title'] = df_original_titles_240.apply(lambda x: x['$b'] if x['$b'] != '' else x['$a'], axis=1)
df_original_titles_240 = df_original_titles_240[['001', 'original title']]

df_original_titles_simple = pd.merge(df_original_titles_100, df_original_titles_240, how='left', on='001')
df_original_titles_simple = df_original_titles_simple.merge(df_original_titles[['001', 'cluster_viaf']]).reset_index(drop=True)
df_original_titles_simple['001'] = df_original_titles_simple['001'].astype(int)
df_original_titles_simple['index'] = df_original_titles_simple.index+1

df_original_titles_simple_grouped = df_original_titles_simple.groupby('cluster_viaf')

df_original_titles_simple = pd.DataFrame()
for name, group in tqdm(df_original_titles_simple_grouped, total=len(df_original_titles_simple_grouped)):
    df = cluster_records(group, 'index', ['original title'])
    df_original_titles_simple = df_original_titles_simple.append(df)

df_original_titles_simple = df_original_titles_simple.sort_values(['cluster_viaf', 'cluster'])    
df_original_titles_simple.to_excel('cluster_original_titles_0.8_author_clusters.xlsx', index=False)


















    
    
    
    
    
    
    
    
    
    