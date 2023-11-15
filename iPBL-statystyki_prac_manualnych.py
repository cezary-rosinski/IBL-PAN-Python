from my_functions import gsheet_to_df
from tqdm import tqdm
from gspread.exceptions import WorksheetNotFound
from datetime import datetime

#%%

dokumentacja_df = gsheet_to_df('1jCjEaopxsezprUiauuYkwQcG1cp40NqdhvxIzG5qUu8', 'dokumentacja')

opracowywane_df = dokumentacja_df.loc[dokumentacja_df['OSOBA OPRACOWUJĄCA'].notnull()]


prace_manualne_statystyki = {}
for i, row in opracowywane_df.iterrows():
    osoba = row['OSOBA OPRACOWUJĄCA']
    prace_manualne_statystyki.setdefault(osoba, []).append(row['LINK'])
    
osoby_statystyki = {}
for k,v in tqdm(prace_manualne_statystyki.items()):
    for link in v:
        doc_id = link.split('/')[-1]
        test_df = gsheet_to_df(doc_id, 'Posts')
        if k not in osoby_statystyki:
            osoby_statystyki[k] = len(test_df.loc[test_df['do PBL'] == 'True']['do PBL'].to_list())
        else:
            osoby_statystyki[k] += len(test_df.loc[test_df['do PBL'] == 'True']['do PBL'].to_list())
        # try:
        #     doc_id = link.split('/')[-1]
        #     test_df = gsheet_to_df(doc_id, 'Posts')
        #     if k not in osoby_statystyki:
        #         osoby_statystyki[k] = len(test_df.loc[test_df['do PBL'] == 'True']['do PBL'].to_list())
        #     else:
        #         osoby_statystyki[k] += len(test_df.loc[test_df['do PBL'] == 'True']['do PBL'].to_list())
        # except WorksheetNotFound:
        #     pass
        
print(f"Sporządzono {sum(osoby_statystyki.values())} z 40000 zapisów")
print(f"Do dnia {datetime.now().date()} zrealizowano {round((sum(osoby_statystyki.values())/40000)*100,2)}% wymaganych zapisów.")

#dodać statystyki dla poszczególnych osób, mówiące o tempie prac (czy za wolno lub za szybko z perspektywy przyjętych obciążeń [uwzględnić RM])

