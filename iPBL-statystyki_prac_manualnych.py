from my_functions import gsheet_to_df
from tqdm import tqdm
from gspread.exceptions import WorksheetNotFound
from datetime import datetime
import pandas as pd
import gspread as gs
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from gspread_dataframe import set_with_dataframe
from collections import Counter

#%% connect google drive

#autoryzacja do tworzenia i edycji plików
gc = gs.oauth()
#autoryzacja do penetrowania dysku
gauth = GoogleAuth()
gauth.LocalWebserverAuth()
drive = GoogleDrive(gauth)


file_list = drive.ListFile({'q': "'1ZrLyjsA6Q-k78M8gpuK5EB2NXCk56zA0' in parents and trashed=false"}).GetList()
# [print(e['title'], e['id']) for e in file_list]

#%%

dokumentacja_df = gsheet_to_df('1jCjEaopxsezprUiauuYkwQcG1cp40NqdhvxIzG5qUu8', 'dokumentacja')

opracowywane_df = dokumentacja_df.loc[dokumentacja_df['OSOBA OPRACOWUJĄCA'].notnull()]

prace_manualne_statystyki = {}
for i, row in opracowywane_df.iterrows():
    osoba = row['OSOBA OPRACOWUJĄCA']
    prace_manualne_statystyki.setdefault(osoba, []).append(row['LINK'])

#stats 2023

# szacunki_prac_manualnych_2023 = gsheet_to_df('1fZxyEYxGPsGfaMGXUFYaCrTAgxV40Yi4-vzgIsyU9LA', '2023')

# project_start = datetime.fromisoformat('2023-05-01')
# radek_start = datetime.fromisoformat('2023-09-01')
# end_2023 = datetime.fromisoformat('2023-12-31')
# delta = end_2023 - project_start
# days_of_the_project_for_2023 = delta.days * 0.67
# radek_delta = end_2023 - radek_start
# days_of_the_project_radek_for_2023 = radek_delta.days * 0.67

# osoby_statystyki_2023 = {}
# for k,v in tqdm(prace_manualne_statystyki.items()):
#     for link in v:
#         doc_id = link.split('/')[-1]
#         if not doc_id:
#             doc_id = link.split('/')[-2]
#         test_df = gsheet_to_df(doc_id, 'Posts')
#         if k not in osoby_statystyki_2023:
#             osoby_statystyki_2023[k] = len(test_df.loc[test_df['do PBL'] == 'True']['do PBL'].to_list())
#         else:
#             osoby_statystyki_2023[k] += len(test_df.loc[test_df['do PBL'] == 'True']['do PBL'].to_list())

# total_from_2023 = sum(osoby_statystyki_2023.values())

# records_to_go = 40000-total_from_2023

# ile_powinno_byc_zrobione = {k:int(float(v)/20*days_of_the_project_radek_for_2023) if k == 'RM' else int(float(v)/20*days_of_the_project_for_2023) for k,v in dict(zip(szacunki_prac_manualnych_2023['osoby'], szacunki_prac_manualnych_2023['rekordów na miesiąc'])).items() if pd.notnull(k)}

#stats 2024
szacunki_prac_manualnych_2023 = gsheet_to_df('1fZxyEYxGPsGfaMGXUFYaCrTAgxV40Yi4-vzgIsyU9LA', '2023')
szacunki_prac_manualnych_2024 = gsheet_to_df('1fZxyEYxGPsGfaMGXUFYaCrTAgxV40Yi4-vzgIsyU9LA', '2024')

start_of_2024 = datetime.fromisoformat('2024-01-01')
project_current = datetime.now()

delta = project_current - start_of_2024
days_of_the_project_for_2024 = delta.days * 0.67

osoby_statystyki_2024 = {}
for k,v in tqdm(prace_manualne_statystyki.items()):
    for link in v:
        doc_id = link.split('/')[-1]
        if not doc_id:
            doc_id = link.split('/')[-2]
        test_df = gsheet_to_df(doc_id, 'Posts')
        if k not in osoby_statystyki_2024:
            osoby_statystyki_2024[k] = len(test_df.loc[test_df['do PBL'] == 'True']['do PBL'].to_list())
        else:
            osoby_statystyki_2024[k] += len(test_df.loc[test_df['do PBL'] == 'True']['do PBL'].to_list())
            
ile_powinno_byc_zrobione_2023 = {k:int(v) for k,v in dict(zip(szacunki_prac_manualnych_2023['osoby'], szacunki_prac_manualnych_2023['powinno być'])).items() if pd.notnull(k)}

ile_powinno_byc_zrobione_2024 = {k:int(float(v)/20*days_of_the_project_for_2024) for k,v in dict(zip(szacunki_prac_manualnych_2024['osoby'], szacunki_prac_manualnych_2024['rekordów na miesiąc'])).items() if pd.notnull(k)}

ile_powinno_byc_zrobione = dict(Counter(ile_powinno_byc_zrobione_2023) + Counter(ile_powinno_byc_zrobione_2024))

final_stats = {}
for k,v in osoby_statystyki_2024.items():
    final_stats.update({k:{'data': project_current.date(),
                           'jest rekordów': v,
                           'powinno być rekordów': ile_powinno_byc_zrobione.get(k),
                           'różnica': v-ile_powinno_byc_zrobione.get(k)}})

for k,v in tqdm(final_stats.items()):
    # v = final_stats.get(k)
    temp_df = pd.DataFrame().from_dict([v])
    sheet_id = [e['id'] for e in file_list if e['title'] == k][0]
    last_df = gsheet_to_df(sheet_id, 'statystyki')
    last_df = pd.concat([last_df, temp_df]).reset_index(drop=True)
    person_sheet = gc.open_by_key(sheet_id)
    set_with_dataframe(person_sheet.worksheet('statystyki'), last_df)
    
        
print(f"Sporządzono {sum(osoby_statystyki_2024.values())} z 40000 zapisów")
print(f"Do dnia {datetime.now().date()} zrealizowano {round((sum(osoby_statystyki_2024.values())/40000)*100,2)}% wymaganych zapisów.")



















