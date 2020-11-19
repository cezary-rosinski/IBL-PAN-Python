import pandas as pd
import re
from my_functions import gsheet_to_df
import math

# Create your dataframe

df = gsheet_to_df('1H6xtk4CkAY9RAVwqt2JgZZtynhPUJtoDiZZpYeS62X4', 'Arkusz główny skróty 50-51')
#df['test'] = df['PBL 50-51'].str.replace('(\w+)(?=\s\d{1,}\:\d{1,})', r'-->\1<--')

df['regex'] = df['PBL 50-51'].apply(lambda x: re.findall('\w+(?=\s\d{1,}\:\d{1,})', x))

# Kickstart the xlsxwriter
writer = pd.ExcelWriter('pbl_retro_coloured_strings.xlsx', engine='xlsxwriter')
df.to_excel(writer, sheet_name='Sheet1', header=False, index=False)
workbook  = writer.book
worksheet = writer.sheets['Sheet1']

# Define the red format and a default format
cell_format_red = workbook.add_format({'font_color': 'red'})
cell_format_default = workbook.add_format({'bold': False})

# Start iterating through the rows and through all of the words in the list

for row in range(0, df.shape[0]):
    print(f"{row+1}/{len(df)}")
    word_indexes = []
    word_starts = 0
    for word in df.iloc[row,2]:
        word_starts = df.iloc[row,0].index(word, word_starts)
        word_ends = word_starts + len(word)
        word_indexes.append(word_starts)
        word_indexes.append(word_ends)
    word_indexes.insert(0, 0)
    string = df.iloc[row,0]
    parts = [string[i:j] for i,j in zip(word_indexes, word_indexes[1:]+[None])]
    
    styles = [cell_format_default, cell_format_red] * math.ceil(len(parts)/2)
    styles = styles[:len(parts)]
    
    segments = []
    for s, p in zip(styles, parts):
        segments.append(s)
        segments.append(p)
    
    worksheet.write_rich_string(row, 0, *segments)

writer.save()

















