from selenium import webdriver
import re
from selenium.common.exceptions import NoSuchElementException, ElementNotInteractableException
import pandas as pd
import unidecode
import numpy as np

def diacritics_unidecode_to_lower(x):
    english_alphabet = 'A B C D E F G H I J K L M N O P Q R S T U V W X Y Z'.split(' ')
    new_word = ''
    for letter in x:
        if letter in english_alphabet or letter == ' ':
            new_word += letter
        else:
            new_word += unidecode.unidecode(letter.lower())
    return new_word.replace(' ', 'b')

def get_year(x):
    try: 
        val = [e for e in x if e.startswith('adres')][0]
        val = re.findall('\d{4}', val)[-1]
    except IndexError:
        val = np.nan
    return val

browser = webdriver.Firefox(executable_path='geckodriver.exe')
browser.get("http://mak.bn.org.pl/cgi-bin/makwww.exe?BM=02&IZ=autor-osoba")

list_of_items = []
last_classification = ''
next_20 = True
while next_20:
    last_url = browser.current_url
    first_elem = browser.find_element_by_css_selector('.submit4')
    first_elem.click()
    
    classification = [elem.text for elem in browser.find_elements_by_css_selector('h4') if 'Szukasz:' in elem.text][0].replace('Szukasz: ', '')
    if last_classification != classification:
        print(classification)
        classification_diacritic = diacritics_unidecode_to_lower(classification)
        last_classification = classification
        
        items = browser.find_elements_by_css_selector('.submit5')
        
        for elem in range(0,len(browser.find_elements_by_css_selector('.submit5'))):
            # elem = range(0,len(browser.find_elements_by_css_selector('.submit5')))[0]
            items = browser.find_elements_by_css_selector('.submit5')
            items[elem].click()
            
            marc = browser.find_element_by_name('MC')
            marc.click()
            
            body = browser.find_element_by_css_selector('tbody').text
            
            list_of_items.append(body)
            browser.back()
            browser.back()
        
        for element in range(2,21):
            # element = 2
            element = '{:02d}'.format(element)
            url = f'http://mak.bn.org.pl/cgi-bin/makwww.exe?BM=02&IM=01&TX=&NU={element}&WI={classification_diacritic}'
            browser.get(url)
            items = browser.find_elements_by_css_selector('.submit5')
            has_next_page = True
            value = 0
            while has_next_page:
                for elem in range(0,len(browser.find_elements_by_css_selector('.submit5'))):
                    # elem = range(0,len(browser.find_elements_by_css_selector('.submit5')))[0]
                    value += 1
                    try:
                        items = browser.find_elements_by_css_selector('.submit5')
                        items[elem].click()
                    except ElementNotInteractableException:
                        browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                        browser.find_element_by_xpath(f"//input[@value='{value}' and @class = 'submit5']").click()  
                        
                    marc = browser.find_element_by_name('MC')
                    marc.click()
                    
                    body = browser.find_element_by_css_selector('tbody').text

                    list_of_items.append(body)
                    browser.back()
                    browser.back()
                try:
                    next_page = browser.find_element_by_xpath("//input[@value='ciąg dalszy ']").click()
                    items = browser.find_elements_by_css_selector('.submit5')                
                    has_next_page = True
                except NoSuchElementException:
                    has_next_page = False
        browser.get(last_url)
        next_list = browser.find_element_by_name('PD').click()
        next_20 = True
    else:
        next_20 = False

browser.close()        
list_of_items_unique = list(set(list_of_items))

with open('samizdat_bn_mak.txt', 'wt', encoding='utf-8') as f:
    for el in list_of_items_unique:
        f.write(el + '\n\n')
    f.close()
























