from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import re
from selenium.common.exceptions import NoSuchElementException
import pandas as pd
import unidecode

def diacritics_unidecode_to_lower(x):
    english_alphabet = 'A B C D E F G H I J K L M N O P Q R S T U V W X Y Z'.split(' ')
    new_word = ''
    for letter in x:
        if letter in english_alphabet or letter == ' ':
            new_word += letter
        else:
            new_word += unidecode.unidecode(letter.lower())
    return new_word.replace(' ', 'b')

browser = webdriver.Chrome()
browser.get("http://mak.bn.org.pl/cgi-bin/makwww.exe?BM=24&IZ=Has%B3o_przedm")
list_of_journals = []
next_20 = True
while next_20:
    last_url = browser.current_url
    first_elem = browser.find_element_by_css_selector('.submit4')
    first_elem.click()
    
    subject_heading = [elem.text for elem in browser.find_elements_by_css_selector('h4') if 'Szukasz:' in elem.text][0].replace('Szukasz: ', '')
    print(subject_heading)
    subject_heading = diacritics_unidecode_to_lower(subject_heading)
    
    journals = browser.find_elements_by_css_selector('.submit5')
    for elem in range(0,len(browser.find_elements_by_css_selector('.submit5'))):
        journals = browser.find_elements_by_css_selector('.submit5')
        journals[elem].click()
        body = browser.find_element_by_css_selector('tbody').text
        sh_for_journal = [elem.text for elem in browser.find_elements_by_css_selector('h4') if 'Szukasz:' in elem.text][0].replace('Szukasz: ', '')
        pair = (sh_for_journal, body)
        list_of_journals.append(pair)
        browser.back()
    
    for element in range(2,21):
        element = '{:02d}'.format(element)
        url = f'http://mak.bn.org.pl/cgi-bin/makwww.exe?BM=24&IM=04&TX=&NU={element}&WI={subject_heading}'
        browser.get(url)
        journals = browser.find_elements_by_css_selector('.submit5')
        has_next_page = True
        while has_next_page:
            for elem in range(0,len(browser.find_elements_by_css_selector('.submit5'))):
                journals = browser.find_elements_by_css_selector('.submit5')
                journals[elem].click()
                body = browser.find_element_by_css_selector('tbody').text
                sh_for_journal = [elem.text for elem in browser.find_elements_by_css_selector('h4') if 'Szukasz:' in elem.text][0].replace('Szukasz: ', '')
                pair = (sh_for_journal, body)
                list_of_journals.append(pair)
                browser.back()
            try:
                next_page = browser.find_element_by_xpath("//input[@value='ciąg dalszy ']").click()
                journals = browser.find_elements_by_css_selector('.submit5')                
                has_next_page = True
            except NoSuchElementException:
                has_next_page = False
    try:
        browser.get(last_url)
        next_list = browser.find_element_by_name('PD').click()
        next_20 = True
    except NoSuchElementException:
        next_20 = False

df = pd.DataFrame(list_of_journals, columns = ['hasło przedmiotowe', 'body'])

df.to_excel('BN_MAK_journals.xlsx', index=False)

print('Done')








