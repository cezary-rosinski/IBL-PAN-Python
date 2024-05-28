import pandas as pd
import regex as re
import regex
import glob
from unidecode import unidecode
import datetime
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import fp_credentials
import time
from selenium.common.exceptions import NoSuchElementException, ElementNotInteractableException, NoAlertPresentException, SessionNotCreatedException, ElementClickInterceptedException, InvalidArgumentException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import numpy as np
import sys
import io
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import gspread as gs
from google_drive_research_folders import cr_projects
from gspread_dataframe import set_with_dataframe, get_as_dataframe
from Gortych_wiso_credentials import login, password

pd.options.display.max_colwidth = 10000
#%%
def scrape_results():
    results = browser.find_elements('xpath', "//span[@class = 'boxDescription ifTitleOnly']")
    while not results:
        time.sleep(2)
        results = browser.find_elements('xpath', "//span[@class = 'boxDescription ifTitleOnly']")

    for i, a in enumerate(results):
        # i = 9
        # a = results[0]
        
        time.sleep(2)
        browser.execute_script(f"window.scrollTo(0, {i+1}*150)")
        time.sleep(2)
        try:
            a.click()
        except ElementClickInterceptedException:
            browser.execute_script(f"window.scrollTo(0, {i+1}*150)")
        time.sleep(10)
        try:
            save = browser.find_element('xpath', "//a[@class = 'boxSave spritesIcons']")
        except (NoSuchElementException, ElementClickInterceptedException):
            save = browser.find_element('xpath', "//a[@class = 'boxSave spritesIcons']")
        save.click()
        time.sleep(2)
        save_confirm = browser.find_elements('xpath', "//span[contains(text(),'Speichern')]")[-1]
        save_confirm.click()
        time.sleep(3)
        browser.switch_to.window(browser.window_handles[1])
        browser.close()
        browser.switch_to.window(browser.window_handles[0])
        browser.back()
        time.sleep(2)
    
#%%
start = time.time()
query_text = 'postmigrantisch*'
url = 'http://erf.sbb.spk-berlin.de/han/1700563777/www.wiso-net.de/dosearch?&dbShortcut=BMP'

browser = webdriver.Firefox()    
browser.get(url) 
accept = browser.find_element('xpath', "//input[@type = 'submit']")
accept.click()

username_input = browser.find_element('xpath', "//input[@name = 'User']")
username_input.send_keys(login)

password_input= browser.find_element('xpath', "//input[@name = 'Password']")
password_input.send_keys(password)

log_in = browser.find_element('xpath', "//input[@type = 'submit']")
log_in.click()

time.sleep(10)

terms_of_use = browser.find_element('xpath', "//input[@id = 'layer_termsOfUse']")
terms_of_use.click()

terms_of_use_ok =  browser.find_elements('xpath', "//span[contains(text(),'OK')]")[-1]
terms_of_use_ok.click()

time.sleep(2)

query = browser.find_element('xpath', "//input[@id = 'field_q']")
query.send_keys(query_text)

time.sleep(2)

search_button = browser.find_element('xpath', "//input[@id = 'searchButton']")
search_button.click()

germany_press = browser.find_element('xpath', "//div[contains(text(),'Presse Deutschland')]")
germany_press.click()

scrape_results()
#ustawić na 175
for iteration in range(10):
    time.sleep(2)
    next_page = browser.find_element('xpath', "//a[@class = 'nextLink']")
    next_page.click()
    time.sleep(10)
    scrape_results()


end = time.time()
print(end - start)
    
#%%










