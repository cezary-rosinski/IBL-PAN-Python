import requests
from bs4 import BeautifulSoup
import pandas as pd
import regex as re
import time
from tqdm import tqdm  #licznik
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import pandas as pd
import sys
sys.path.insert(1, 'C:/Users/Cezary/Documents/IBL-PAN-Python')
from my_functions import gsheet_to_df

#%%

url = "https://forum.pkp.sfu.ca/c/questions/5?order=views"
result = requests.get(url)
soup = BeautifulSoup(result.content, 'lxml')




#%%
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException, ElementNotInteractableException, NoAlertPresentException, SessionNotCreatedException, ElementClickInterceptedException, InvalidArgumentException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from bs4 import BeautifulSoup
from tqdm import tqdm
import regex as re
import pandas as pd
#%%

url = 'https://forum.pkp.sfu.ca/c/questions/5?order=views' # replace with the URL of the webpage you want to scrape

# Set up Selenium driver (make sure you have the appropriate driver installed for your browser)

# .Edge, .Firefox, .Chrome
driver = webdriver.Firefox()  

#storing the url in driver
driver.get(url)

#giving some time to load
time.sleep(3)

#getting the height of the webpage for infinite croll web page
last_height = driver.execute_script("return document.body.scrollHeight")

# Scroll down until no more content is loaded
while True:
    #scrolling once code
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    
    #giving time to load
    time.sleep(2) # wait for content to load
    
    #checking new height of webpage
    new_height = driver.execute_script("return document.body.scrollHeight")
    
    #defining the break condition to stop the execution at the end of the webpage
    if new_height == last_height:
        break
    last_height = new_height         #while loop breaks when the last height of web page will not change 

#The resulting parsed HTML is stored in the variable "soup", which can then be used to extract information from the webpage using various BeautifulSoup methods and functions.
soup = BeautifulSoup(driver.page_source, 'html.parser')


higher = soup.select('tr.topic-list-item')
# t = soup.select('td.main-link.clearfix.topic-list-data > span > a')
# views = soup.select('.views .number')


# test = [e.text for e in t]
response = []
errors = []
for e in tqdm(higher):
    # e = higher[31]

    title = e.select_one('a.title')
    post_url = title['href']
    title = title.text
    try:
        views = e.select_one('td.num.views')
        views = re.findall('\d+', views.select_one('span.number')['title'])[0]
        
        posts = e.select_one('td.num.posts-map.posts.topic-list-data')
        posts = re.findall('\d+', posts.select_one('span.number').text)[0]
    except AttributeError:
        posts = e.select_one('span.posts').text
        views = e.select_one('span.views').text
    
    response.append([title, post_url, views, posts])

# response = set(response)
# df = pd.DataFrame(response, columns=['title', 'views', 'posts'])

# df.to_excel('data/ojs_forum.xlsx', index=False)    

# for i, (title, url, view, post) in tqdm(enumerate(response), total=len(response)):

    #!!!!!!!!!!!!!!!!!!!
#przygotować urle, np.   MissingSchema: Invalid URL '/t/the-incident-id-is-n-a/4063'  

def update_post_info(resp):
    try:
        title, url, view, post = resp
        result = requests.get(url)
        soup = BeautifulSoup(result.content, 'lxml')
            
        text = soup.find('div', class_="post").text.strip()
        
        posts_time = soup.find_all('span', class_='crawler-post-infos')
        first_post_time = posts_time[0].find('time')['datetime']
        last_post_time = posts_time[-1].find('time')['datetime']
        resp.extend([text, first_post_time, last_post_time])
    except AttributeError:
        errors.append(url)
    
    
errors = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(update_post_info, response),total=len(response)))    



# post content
url = "https://forum.pkp.sfu.ca/t/the-incident-id-is-n-a/4063"
result = requests.get(url)
soup = BeautifulSoup(result.content, 'lxml')

text = soup.select('#post_1 .cooked')

text = soup.find('div', class_="post").text.strip()

test = text.text




























