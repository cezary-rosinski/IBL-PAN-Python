from my_functions import gsheet_to_df

books_to_choose = gsheet_to_df('1BZzxU3rzqCX9LGSHWV4Y_QyD-I9ha7fPwEWLemRlj8I', 'PL')
books_to_choose = books_to_choose.loc[books_to_choose['Data omówienia'].isna()]

next_book = books_to_choose.sample(1)

#%%

print(f'Kolejna książka do omówienia to {next_book["Tytuł"].to_string(index=False)}.\nJej autor to: {next_book["Autor"].to_string(index=False)}.')


