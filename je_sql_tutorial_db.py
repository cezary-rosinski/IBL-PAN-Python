#Acknowledgements: https://nodegoat.net/blog.p/82.m/20/what-is-a-relational-database
import sqlite3
import gspread_pandas as gp
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from google_drive_credentials import gc, credentials
from google_drive_research_folders import cr_projects

gauth = GoogleAuth()
gauth.LocalWebserverAuth()
drive = GoogleDrive(gauth)

#%% load data
file_list = drive.ListFile({'q': f"'{cr_projects}' in parents and trashed=false"}).GetList() 
#[print(e['title']) for e in file_list]
je_folder = [file['id'] for file in file_list if file['title'] == 'Jakub Eichler - mentoring'][0]
file_list = drive.ListFile({'q': f"'{je_folder}' in parents and trashed=false"}).GetList() 
#[print(e['title']) for e in file_list]
sql_folder = [file['id'] for file in file_list if file['title'] == 'SQL'][0]
file_list = drive.ListFile({'q': f"'{sql_folder}' in parents and trashed=false"}).GetList() 
#[print(e['title']) for e in file_list]
sql_tutorial_db_sheet = [file['id'] for file in file_list if file['title'] == 'sql_tutorial_db'][0]
sql_tutorial_db_sheet = gp.Spread(sql_tutorial_db_sheet, creds=credentials)
sql_tutorial_db_sheet.sheets

sql_letters = sql_tutorial_db_sheet.sheet_to_df(sheet='letters', index=0)
sql_people = sql_tutorial_db_sheet.sheet_to_df(sheet='people', index=0)
sql_cities = sql_tutorial_db_sheet.sheet_to_df(sheet='cities', index=0)

#%% create a sql database and upload tables
connection = sqlite3.connect('je_tutorial.db')
cursor = connection.cursor()
sql_letters.to_sql('letters', connection, if_exists='replace', index=False)
sql_people.to_sql('people', connection, if_exists='replace', index=False)
sql_cities.to_sql('cities', connection, if_exists='replace', index=False)
connection.close()



