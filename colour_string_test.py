import pandas as pd
import re
from my_functions import gsheet_to_df

# Create your dataframe

df = gsheet_to_df('1H6xtk4CkAY9RAVwqt2JgZZtynhPUJtoDiZZpYeS62X4', 'Arkusz główny skróty 50-51')
df['test'] = df['PBL 50-51'].str.replace('(\w+)(?=\s\d{1,}\:\d{1,})', r'-->\1<--')


df['regex'] = df['PBL 50-51'].apply(lambda x: re.findall('\w+(?=\s\d{1,}\:\d{1,})', x))
df = df.head(1)

# Kickstart the xlsxwriter
writer = pd.ExcelWriter('Testing rich strings.xlsx', engine='xlsxwriter')
df.to_excel(writer, sheet_name='Sheet1', header=False, index=False)
workbook  = writer.book
worksheet = writer.sheets['Sheet1']

# Define the red format and a default format
cell_format_red = workbook.add_format({'font_color': 'red'})
cell_format_default = workbook.add_format({'bold': False})

# Start iterating through the rows and through all of the words in the list

for i, row in df.iterrows():
    print(f"{i}/{len(df)}")
    for word in row['regex']:
        try:
            starting_point = row['PBL 50-51'].index(word)
            worksheet.write_rich_string(df.at[i, 'PBL 50-51'],
                                        cell_format_default, df.at[i, 'PBL 50-51'][0:starting_point],
                                        cell_format_red, word, 
                                        cell_format_default, df.at[i, 'PBL 50-51'][starting_point+len(word):])
        except (ValueError):
            continue

writer.save()




elif (df.iloc[row,0].index(word) > 0) \
            and (df.iloc[row,0].index(word) != len(df.iloc[row,0])-len(word)) \
            and ('Typo:' not in df.iloc[row,0]):
                starting_point = df.iloc[row,0].index(word)
                worksheet.write_rich_string(row, 0, cell_format_default,
                                    df.iloc[row,0][0:starting_point],
                                    cell_format_red, word, cell_format_default,
                                    df.iloc[row,0][starting_point+len(word):])


















for row in range(0,df.shape[0]):
    for word in wrong_words:
        try:
            # 1st case, wrong word is at the start and there is additional text
            if (df.iloc[row,0].index(word) == 0) \
            and (len(df.iloc[row,0]) != len(word)):
                worksheet.write_rich_string(row, 0, cell_format_red, word,
                                            cell_format_default,
                                            df.iloc[row,0][len(word):])

            # 2nd case, wrong word is at the middle of the string
            elif (df.iloc[row,0].index(word) > 0) \
            and (df.iloc[row,0].index(word) != len(df.iloc[row,0])-len(word)) \
            and ('Typo:' not in df.iloc[row,0]):
                starting_point = df.iloc[row,0].index(word)
                worksheet.write_rich_string(row, 0, cell_format_default,
                                    df.iloc[row,0][0:starting_point],
                                    cell_format_red, word, cell_format_default,
                                    df.iloc[row,0][starting_point+len(word):])

            # 3rd case, wrong word is at the end of the string
            elif (df.iloc[row,0].index(word) > 0) \
            and (df.iloc[row,0].index(word) == len(df.iloc[row,0])-len(word)):
                starting_point = df.iloc[row,0].index(word)
                worksheet.write_rich_string(row, 0, cell_format_default,
                                            df.iloc[row,0][0:starting_point],
                                            cell_format_red, word)

            # 4th case, wrong word is the only one in the string
            elif (df.iloc[row,0].index(word) == 0) \
            and (len(df.iloc[row,0]) == len(word)):
                worksheet.write(row, 0, word, cell_format_red)

        except ValueError:
            continue

writer.save()

