import gspread
from google.oauth2.service_account import Credentials
import gspread_pandas as gp
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

gauth = GoogleAuth()
gauth.LocalWebserverAuth()

drive = GoogleDrive(gauth)

#mój dysk

file_list = drive.ListFile({'q': "'root' in parents and trashed=false"}).GetList()
for file1 in file_list:
  print('title: %s, id: %s' % (file1['title'], file1['id']))

#dysk ibl.waw.pl

file_list = drive.ListFile({'q': "'1Cl-fX5VL0LnvYUs7AXMb6E0Lf1DX8DJc' in parents and trashed=false"}).GetList()
for file1 in file_list:
  print('title: %s, id: %s' % (file1['title'], file1['id']))
  
file_list = drive.ListFile({'q': "'1DjHGgS7rpWniyIY9FctYg60qkVtUcGpF' in parents and trashed=false"}).GetList()
for file1 in file_list:
  print('title: %s, id: %s' % (file1['title'], file1['id']))
  
#dysk PBL

file_list = drive.ListFile({'q': "'0B_49iQ9XaQ-vWHBidGxFcWRMMUU' in parents and trashed=false"}).GetList()

for file1 in file_list:
  print('title: %s, id: %s' % (file1['title'], file1['id']))
  
#google sheets
  
#po NW

# authorization - wklejasz linki api google do przestrzeni w jakich chcesz operować, jest tego w opór na stronie googla
scopes = [
    'https://www.googleapis.com/auth/spreadsheets', # odpowiada za edycję arkuszy
    'https://www.googleapis.com/auth/drive' # odpowiada za operacje na plikach i folderach na dysku
]

# credentials z google cloud manager, ta opcja dla cos tam usługi
credentials = Credentials.from_service_account_file(r"C:/Users/Cezary/Documents/IBL-PAN-Python/pythonsheets-credentials.json", scopes=scopes)

gc = gspread.authorize(credentials)

#Translation test
  
s = gp.Spread('1t_-_cxUzvRG2iYyKN6WMXmk7wxC9imnrFfowYJ8oIWo', creds=credentials)
s.sheets
df = s.sheet_to_df(sheet='multiple volumes', index=0)


# nazwa arkusza i id folderu, gdzie ma się pojawić, wczeniej trzeba udostepnić ten folder dla mejla usługi z pliku json credentials
sheet = gc.create('test', '1DjHGgS7rpWniyIY9FctYg60qkVtUcGpF')
sheet.id

s = gp.Spread(sheet.id, creds=credentials)

s
s.sheets
s.url
s.df_to_sheet(artykuly,sheet='trzeci')
df = s.sheet_to_df(sheet='trzeci', index=0)

worksheet = sheet.get_worksheet(3)
worksheet.format('A1:Z1', {'textFormat': {'bold': True}})
worksheet.format('A:Z', {"wrapStrategy": 'CLIP'})
sheet.batch_update({
    "requests": [
        {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": worksheet._properties['sheetId'],
                    "dimension": "ROWS",
                    "startIndex": 0,
                    #"endIndex": 100
                },
                "properties": {
                    "pixelSize": 20
                },
                "fields": "pixelSize"
            }
        }
    ]
})

mails = ['sylwia.pikula@wp.eu', 'mariola.wilczak@ibl.waw.pl', 'cezary.rosinski@ibl.waw.pl', 'cezary.rosinski@gmail.com', 'nwolczuk@gmail.com']
for mail in mails:
    sheet.share(mail, perm_type='user', role='writer', notify=False)
    
# gspread open from drive

sheet = gc.open_by_key('1t_-_cxUzvRG2iYyKN6WMXmk7wxC9imnrFfowYJ8oIWo')
worksheet = sheet.worksheet('multiple volumes')
#zakotwiczenie pierwszego wiersza
worksheet.freeze(rows=1)    
#zakładanie filtra
worksheet.set_basic_filter()
    
    
    
    
    
    
    
    
    
    
    
    
