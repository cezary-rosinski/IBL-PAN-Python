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

pd.options.display.max_colwidth = 10000
#%%

query_text = 'postmigrantisch*'
url = 'http://erf.sbb.spk-berlin.de/han/1700563777/www.wiso-net.de/dosearch?&dbShortcut=BMP'

browser = webdriver.Firefox()    
browser.get(url) 
accept = browser.find_element('xpath', "//input[@type = 'submit']")
accept.click()

username_input = browser.find_element('xpath', "//input[@name = 'User']")
username_input.send_keys('xx')

password = browser.find_element('xpath', "//input[@name = 'Password']")
password.send_keys('xx*')

login = browser.find_element('xpath', "//input[@type = 'submit']")
login.click()

time.sleep(10)

terms_of_use = browser.find_element('xpath', "//input[@id = 'layer_termsOfUse']")
terms_of_use.click()

terms_of_use_ok =  browser.find_elements('xpath', "//span[contains(text(),'OK')]")[-1]
terms_of_use_ok.click()

query = browser.find_element('xpath', "//input[@id = 'field_q']")
query.send_keys(query_text)

search_button = browser.find_element('xpath', "//input[@id = 'searchButton']")
search_button.click()

germany_press = browser.find_element('xpath', "//div[contains(text(),'Presse Deutschland')]")
germany_press.click()

results = browser.find_elements('css selector', '.boxDescription')

for a in results:
    # a = results[0]
    a.click()
    time.sleep(10)
    save = browser.find_element('xpath', "//a[@class = 'boxSave spritesIcons']")
    save.click()
    time.sleep(2)
    save_confirm = browser.find_elements('xpath', "//span[contains(text(),'Speichern')]")[-1]
    save_confirm.click()
    time.sleep(2)
    browser.switch_to.window(browser.window_handles[1])
    browser.close()
    browser.switch_to.window(browser.window_handles[0])
    browser.back()
    
    dir(results[0])
    print(dir(a))

wstep_pl_ok = browser.find_element('xpath', "//span[contains(text(),'Ok')]").click()





sendKeys(Keys.RETURN)


username_input.send_keys(username)
password_input.send_keys(password)


browser.find_element('xpath', "//button[@class = 'button media-button button-primary button-large media-button-select']")

browser.find_element('input', {'type': 'submit'})
wybierz_pdf = browser.find_element('xpath', "//select[@id = 'metakeyselect']/option[text()='pdf-url']").click()










