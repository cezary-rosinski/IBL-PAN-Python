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

browser = webdriver.Chrome()
browser.get("http://mak.bn.org.pl/cgi-bin/makwww.exe?BM=53&IZ=Klasyfikacja")

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
            items = browser.find_elements_by_css_selector('.submit5')
            items[elem].click()
            body = browser.find_element_by_css_selector('tbody').text
            classification_for_journal = [elem.text for elem in browser.find_elements_by_css_selector('h4') if 'Szukasz:' in elem.text][0].replace('Szukasz: ', '')
            pair = (classification_for_journal, body)
            list_of_items.append(pair)
            browser.back()
        
        for element in range(2,21):
            element = '{:02d}'.format(element)
            url = f'http://mak.bn.org.pl/cgi-bin/makwww.exe?BM=53&IM=08&TX=&NU={element}&WI={classification_diacritic}'
            browser.get(url)
            items = browser.find_elements_by_css_selector('.submit5')
            has_next_page = True
            value = 0
            while has_next_page:
                for elem in range(0,len(browser.find_elements_by_css_selector('.submit5'))):
                    value += 1
                    try:
                        items = browser.find_elements_by_css_selector('.submit5')
                        items[elem].click()
                    except ElementNotInteractableException:
                        browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                        browser.find_element_by_xpath(f"//input[@value='{value}' and @class = 'submit5']").click()         
                    body = browser.find_element_by_css_selector('tbody').text
                    classification_for_journal = [elem.text for elem in browser.find_elements_by_css_selector('h4') if 'Szukasz:' in elem.text][0].replace('Szukasz: ', '')
                    pair = (classification_for_journal, body)
                    list_of_items.append(pair)
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
        
list_of_items_unique = list(set(list_of_items))

df = pd.DataFrame(list_of_items_unique, columns = ['klasyfikacja', 'body']).sort_values('klasyfikacja').reset_index(drop=True)
df['index'] = df.index+1

df_marc = pd.DataFrame(df['body'].str.split('\n').tolist(), df['index']).stack()
df_marc = df_marc.reset_index()[[0, 'index']]
df_marc.columns = ['body', 'index']
df_marc = pd.merge(df_marc, df[['index', 'klasyfikacja']], on='index', how='inner')

df_marc['field'] = df_marc['body'].replace(r'(^)(...)(.+?$)', r'\2', regex = True)
df_marc['content'] = df_marc['body'].replace(r'(^)(......)(.+?$)', r'\3', regex = True).str.strip()
df_marc = df_marc[['index', 'field', 'content', 'klasyfikacja']]
df_marc['content'] = df_marc.groupby(['index', 'field'])['content'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
df_marc = df_marc.drop_duplicates().reset_index(drop=True)
df_wide = df_marc.pivot(index = 'index', columns = 'field', values = 'content')
df_wide['id'] = df_wide.index
df_wide = pd.merge(df_wide, df_marc[['index', 'klasyfikacja']].drop_duplicates(), left_on='id', right_on='index', how='inner').drop(columns='index')

fields = df_wide.columns.tolist()
fields.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
df_wide = df_wide.reindex(columns=fields)

df_wide.to_excel('BN_MAK_synat_passim.xlsx', index=False)

print('Done')








