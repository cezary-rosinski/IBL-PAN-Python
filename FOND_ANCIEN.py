from selenium import webdriver
import re
from selenium.common.exceptions import NoSuchElementException, ElementNotInteractableException
import pandas as pd
import unidecode
import numpy as np
import requests
from bs4 import BeautifulSoup

#%%

browser = webdriver.Firefox()
browser.get("http://193.0.122.72/cgi-bin//makwww.exe?BM=01&IZ=Nr_inw.")

pattern = re.compile(r"""
\b
\d{6}[A-Z]?                          # główny numer (6 cyfr) + opcjonalna litera
(?:\s+(?!\d{6}\b)\d{1,3}[A-Z]*)?     # opcjonalna "część": 1-3 cyfry (+ litery), ale nie kolejny 6-cyfrowy numer
\b
""", re.VERBOSE)

numery = []

html_text = browser.page_source
soup = BeautifulSoup(html_text, "html.parser")

text = soup.get_text(separator=" ")

numbers = re.findall(pattern, text)
numery.extend(numbers)

iteration = 1
while len(numbers) != 10:
    html_text = browser.page_source
    soup = BeautifulSoup(html_text, "html.parser")
    text = soup.get_text(separator=" ")
    numbers = re.findall(pattern, text)
    numery.extend(numbers)
    dalej = browser.find_element('xpath', "//input[@value = 'dalej']")
    iteration += 1
    print(iteration)
    dalej.click()

numery = set(numery)













