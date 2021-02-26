import csv
import numpy as np
import pandas as pd

# Loading csv
list_of_records = []
with open("F:/Cezary/Documents/IBL/tabele z pulpitu/bn_data_ks_table.csv", 'r', encoding="utf8", errors="surrogateescape") as csv_file:
    reader = csv.reader(csv_file, delimiter=";")
    headers = next(reader, None)
    position_008 = headers.index('008')
    for row in reader:
        if row[position_008][7:11] in ['2004', '2005', '2006', '2007', '2008', '2009', '2010']:
            list_of_records.append(row)

df = pd.DataFrame(list_of_records, columns=headers)
df['rok'] = df['008'].apply(lambda x: x[7:11])

lata = ['2004', '2005', '2006', '2007', '2008', '2009', '2010']
for rok in lata:
    train = df.copy()[df['rok'] == rok].drop(columns='rok')
    for col in train.columns:
        if train[col].dtype==object:
            train[col]=train[col].apply(lambda x: np.nan if x==np.nan else str(x).encode('utf-8', 'replace').decode('utf-8'))
    train.to_csv(f"bn_{rok}.csv", encoding='utf-8', index=False)
    
    
    
    
    
    
  