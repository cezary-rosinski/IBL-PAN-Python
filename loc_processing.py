import requests
from requests import get
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import re
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time
from my_functions import mrc_to_mrk


url = 'https://www.loc.gov/cds/products/MDSConnect-books_all.html'
results = requests.get(url)
soup = BeautifulSoup(results.text, "html.parser")

schema = re.compile('cds\/downloads\/MDSConnect\/BooksAll\.2016\.part\d+\.utf8')
no_of_parts =  len(soup.findAll('a', attrs={'href': schema})) + 1

browser = webdriver.Chrome()

for i in range(1,no_of_parts):
    print(f'{i}/{no_of_parts}')
    i = '{:02d}'.format(i)
    url = f"https://www.loc.gov/cds/downloads/MDSConnect/BooksAll.2016.part{i}.utf8.gz"
    browser.get(url)
    time.sleep(80)
    
    





mrc_to_mrk('F:/Cezary/Documents/IBL/Translations/LoC/Books.All.2016.part01.utf8', 'F:/Cezary/Documents/IBL/Translations/LoC/loc_1.mrk')