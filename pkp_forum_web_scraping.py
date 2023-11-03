import requests
from bs4 import BeautifulSoup
import pandas as pd
import regex as re
import time
from tqdm import tqdm  #licznik
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from datetime import datetime
import json
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import pandas as pd
import sys
sys.path.insert(1, 'C:/Users/Cezary/Documents/IBL-PAN-Python')
from my_functions import gsheet_to_df
import pickle
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException, ElementNotInteractableException, NoAlertPresentException, SessionNotCreatedException, ElementClickInterceptedException, InvalidArgumentException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from collections import Counter
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
    # e = higher[1]
    # e = higher[31]

    category = e.select_one('.category-name').text

    title = e.select_one('a.title')
    post_url = title['href']
    title = title.text
    
    category = e.select_one('.category-name').text
    tags = e.select('.simple')
    if tags:
        tags = [e.text for e in tags]
    else: tags = None
    
    try:
        views = e.select_one('td.num.views')
        views = re.findall('\d+', views.select_one('span.number')['title'])[0]
        
        posts = e.select_one('td.num.posts-map.posts.topic-list-data')
        posts = re.findall('\d+', posts.select_one('span.number').text)[0]
    except AttributeError:
        posts = e.select_one('span.posts').text
        views = e.select_one('span.views').text
    
    response.append([title, post_url, views, posts, category, tags])

# response = set(response)
# df = pd.DataFrame(response, columns=['title', 'views', 'posts'])

# df.to_excel('data/ojs_forum.xlsx', index=False)    

# for i, (title, url, view, post) in tqdm(enumerate(response), total=len(response)):
    
response = response[30:]
response = [(a, f"https://forum.pkp.sfu.ca{b}" if not b.startswith('h') else b, c, d, e, f) for a, b, c, d, e, f in response]  

with open('data/pkp_original_response.pickle', 'wb') as handle:
    pickle.dump(response, handle, protocol=pickle.HIGHEST_PROTOCOL)

# response = [list(e) for e in list(set(response))]

# with open('data/pkp_response.pickle', 'wb') as handle:
#     pickle.dump(response, handle, protocol=pickle.HIGHEST_PROTOCOL)
    
# with open('data/pkp_response.pickle', 'rb') as handle:
#     response = pickle.load(handle)
    
origin_response = [e[:4] for e in response]

for resp in tqdm(origin_response[14561:]):
    title, url, view, post = resp
    result = requests.get(url)
    soup = BeautifulSoup(result.content, 'lxml')
    if result.status_code != 504:
        text = soup.find('div', class_="post").text.strip()
        
        posts_time = soup.find_all('span', class_='crawler-post-infos')
        first_post_time = posts_time[0].find('time')['datetime']
        last_post_time = posts_time[-1].find('time')['datetime']
        resp.extend([text, first_post_time, last_post_time])
    else: print(resp)
        
    
with open('data/pkp_response.pickle', 'wb') as handle:
    pickle.dump(origin_response, handle, protocol=pickle.HIGHEST_PROTOCOL)


def update_post_info(resp):
    # resp = ['Upgrade related - OJS', 'https://forum.pkp.sfu.ca/t/upgrade-related-ojs/20435', '311', '1']
    try:
        title, url, view, post = resp
        result = requests.get(url)
        errors.append([url, result.status_code, result.content])
        soup = BeautifulSoup(result.content, 'lxml')
            
        text = soup.find('div', class_="post").text.strip()
        
        posts_time = soup.find_all('span', class_='crawler-post-infos')
        first_post_time = posts_time[0].find('time')['datetime']
        last_post_time = posts_time[-1].find('time')['datetime']
        resp.extend([text, first_post_time, last_post_time])
    except AttributeError:
        time.sleep(0.3)
        update_post_info(resp)
        # errors.append(url)
    except requests.Timeout:
        time.sleep(0.3)
        update_post_info(resp)
        # result = requests.get(url)
        # print(url, result.status_code)
    
errors = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(update_post_info, response),total=len(response)))

#%%NLP
from keybert import KeyBERT
import spacy
kw_model = KeyBERT()

with open('data/pkp_response.pickle', 'rb') as handle:
    response = pickle.load(handle)
    
interesting = [e for e in response if any('3' in el for el in [e[0], e[4]])]

keywords_dict = {}
for m in tqdm(interesting):
    # m = interesting[2]
    keywords_dict.update({m[0]: {'title': kw_model.extract_keywords(m[0]),
                                 'post': kw_model.extract_keywords(m[4])}})

with open('data/pkp_forum_keywords_extracted.pickle', 'wb') as handle:
    pickle.dump(keywords_dict, handle, protocol=pickle.HIGHEST_PROTOCOL)
    
title_kw = [el for sub in [[a for a,b in v.get('title')] for k,v in keywords_dict.items()] for el in sub]
nlp = spacy.load("en_core_web_sm")

title_kw_lemmas = []
for e in tqdm(title_kw):
    title_kw_lemmas.append(nlp(e))
title_kw_lemmas = [e[0].lemma_ for e in title_kw_lemmas]
title_result = Counter(title_kw_lemmas).most_common(200)
title_df = pd.DataFrame(title_result)

post_kw = [el for sub in [[a for a,b in v.get('post')] for k,v in keywords_dict.items()] for el in sub]

post_kw_lemmas = []
for e in tqdm(post_kw):
    post_kw_lemmas.append(nlp(e))
post_kw_lemmas = [e[0].lemma_ for e in post_kw_lemmas]
post_result = Counter(post_kw_lemmas).most_common(200)
post_df = pd.DataFrame(post_result)


#%% Hanna request

with open('data/pkp_forum_keywords_extracted.pickle', 'rb') as a, open('data/pkp_response.pickle', 'rb') as b, open('data/pkp_original_response.pickle', 'rb') as c:
    keywords_dict = pickle.load(a)
    pkp_response = pickle.load(b)
    pkp_original_response = pickle.load(c)

pkp_response = [e for e in pkp_response if any('3' in el for el in [e[0], e[4]])]

orig_cat_dict = {e[0]:e[4] for e in pkp_original_response}
orig_tag_dict = {e[0]:e[5] for e in pkp_original_response}


for e in tqdm(pkp_response):
    e.append(orig_cat_dict.get(e[0]))
    e.append(orig_tag_dict.get(e[0]))
    
with open('data/pkp_final_response.pickle', 'wb') as handle:
    pickle.dump(pkp_response, handle, protocol=pickle.HIGHEST_PROTOCOL)
    
with open('data/pkp_final_response.pickle', 'rb') as file:
    pkp_response = pickle.load(file)
    
# upgrade = [e for e in pkp_response if e[-2] == 'Software Support' and e[-1] and all(el in e[-1] for el in ['ojs', 'upgrade'])]
tags = [e for e in pkp_response if e[-2] == 'Software Support' and e[-1] and any(el in e[-1] for el in ['install', 'installation', 'multi-installation', 'upgrade', 'upgrade-error', 'post-upgrade'])]

tags_df = pd.DataFrame(tags, columns=['title', 'url', 'views', 'replies', 'text', 'first_post_time', 'last_post_time', 'category', 'tags'])
    
tags_df.to_excel('data/pkp_forum_tags.xlsx', index=False)

#%% tables for keywords

with open('data/pkp_forum_keywords_extracted.pickle', 'rb') as a, open('data/pkp_response.pickle', 'rb') as b, open('data/pkp_original_response.pickle', 'rb') as c:
    keywords_dict = pickle.load(a)
    pkp_response = pickle.load(b)
    pkp_original_response = pickle.load(c)

pkp_response = [e for e in pkp_response if any('3' in el for el in [e[0], e[4]])]

ok_keywords_dict = {k:[el[0] for el in [e for sub in v.values() for e in sub]] for k,v in keywords_dict.items()}

selected_keywors = ["reviewer", "review", "notification", "permission", "register", "access", "archive", "submission", "file", "email", "upload", "export", "pdf", "crossref", "display", "template"]

ok_keywords_dict = {k:[el.lower() for el in v] for k,v in ok_keywords_dict.items() if any(e in selected_keywors for e in v)}

result = [e for e in pkp_response if e[0] in ok_keywords_dict]
[e.append(ok_keywords_dict.get(e[0])) for e in result]

keywords_df = pd.DataFrame(result, columns=['title', 'url', 'views', 'replies', 'text', 'first_post_time', 'last_post_time', 'keywords'])
keywords_df.to_excel('data/pkp_forum_keywords_posts.xlsx', index=False)

#%%rest
import spacy 
from sklearn.feature_extraction.text import TfidfVectorizer 
nlp = spacy.load("en_core_web_sm")
doc = nlp(q)
noun_phrases = [chunk.text for chunk in doc.noun_chunks] 
vectorizer = TfidfVectorizer() 
tfidf = vectorizer.fit_transform([doc.text])
top_phrases = sorted(vectorizer.vocabulary_, key=lambda x: tfidf[0, vectorizer.vocabulary_[x]], reverse=True)[:3]

import yake
kw_extractor = yake.KeywordExtractor()
keywords = kw_extractor.extract_keywords(q)
keywords[:5]


from rake_nltk import Rake
import nltk
nltk.download('stopwords')
nltk.download('punkt')
rake = Rake()
rake.extract_keywords_from_text(q)
keywords = rake.get_ranked_phrases()
keywords[:5]


import torch
import transformers

# Load the BERT model and create a new tokenizer 
model = transformers.BertModel.from_pretrained("bert-base-uncased") 
tokenizer = transformers.BertTokenizer.from_pretrained("bert-base-uncased")

# Tokenize and encode the text 
input_ids = tokenizer.encode(q, add_special_tokens=True) 

# Use BERT to encode the meaning and context of the words and phrases in the text 
outputs = model(torch.tensor([input_ids]))

# Use the attention weights of the tokens to identify the most important words and phrases 
attention_weights = outputs[-1]
top_tokens = sorted(attention_weights[0], key=lambda x: x[1], reverse=True)[:3]

# Decode the top tokens and print the top 3 keywords 
top_keywords = [tokenizer.decode([token[0]]) for token in top_tokens]
print(top_keywords)





# post content
url = "https://forum.pkp.sfu.ca/t/the-incident-id-is-n-a/4063"
result = requests.get(url)
soup = BeautifulSoup(result.content, 'lxml')

text = soup.select('#post_1 .cooked')

text = soup.find('div', class_="post").text.strip()

test = text.text


for url, status, content in test:
    soup = BeautifulSoup(content, 'lxml')
        
    text = soup.find('div', class_="post").text.strip()
    
    posts_time = soup.find_all('span', class_='crawler-post-infos')
    first_post_time = posts_time[0].find('time')['datetime']
    last_post_time = posts_time[-1].find('time')['datetime']
    
























