from tqdm import tqdm
import pandas as pd
from requests.exceptions import ConnectionError, HTTPError, ContentDecodingError, ChunkedEncodingError
from concurrent.futures import ThreadPoolExecutor
import requests
import regex as re
from concurrent.futures import as_completed
from requests_futures.sessions import FuturesSession
import nest_asyncio
nest_asyncio.apply()
import asyncio
import aiohttp
import ssl

#%%
path = r'data\CRAFT-OA\beacon.csv'
ojs_data = pd.read_csv(path)

ojs_data_selected = ojs_data[['oai_url', 'repository_name', 'country_consolidated']].drop_duplicates()
ojs_data_selected = ojs_data_selected.loc[ojs_data_selected['repository_name'].notnull()]

ojs_data_selected = ojs_data_selected.loc[ojs_data_selected['country_consolidated'] == 'PL']

oai_urls = set(ojs_data_selected['oai_url'].to_list())


url_list = list(oai_urls)[:]


async def fetch(session, url):
    async with session.get(url, ssl=ssl.SSLContext()) as response:
        return await response.text()


async def fetch_all(urls, loop):
    async with aiohttp.ClientSession(loop=loop) as session:
        results = await asyncio.gather(*[fetch(session, url + '?verb=Identify') for url in urls], return_exceptions=True)
        return results


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    urls = url_list
    htmls = loop.run_until_complete(fetch_all(urls, loop))
    # print(htmls)

oai_harvesting = []
pattern_email = "(?<=\<adminEmail\>)(.*)(?=\<\/adminEmail\>)"
pattern_url = "(?<=\<baseURL\>)(.*)(?=\<\/baseURL\>)"
pattern_name = "(?<=\<repositoryName\>)(.*)(?=\<\/repositoryName\>)"
for e in htmls:
    if e and not isinstance(e, aiohttp.client_exceptions.ClientConnectorError) and re.search(pattern_url, e):
        oai_harvesting.append([re.search(pattern_url, e).group(), re.search(pattern_name, e).group(), re.search(pattern_email, e).group()])
    
df = pd.DataFrame(oai_harvesting, columns=['oai_url', 'repository_name', 'email']).drop_duplicates() 
   
# df = pd.DataFrame().from_dict(oai_harvesting, orient='index').reset_index().rename(columns={'index': 'oai_url', 0: 'email'})
# df = pd.merge(ojs_data_selected, df, on='oai_url', how='left')

df.to_excel('data\CRAFT-OA\ojs_harvested_email.xlsx', index=False)
    
    
#%% Notes

# pattern = "(?<=\<adminEmail\>)(.*)(?=\<\/adminEmail\>)"
# session = requests.Session()

# # oai_urls = list(oai_urls)[:200]
# # oai_harvesting = {}
# # for url in tqdm(oai_urls):
# def get_oai_email(url):
#     # url = list(oai_urls)[0]
#     # url = 'https://journals.akademicka.pl/index/oai'
#     try:
#         r = session.get(url + '?verb=Identify')
#         if re.search(pattern, r.text):
#             email = re.search(pattern, r.text).group()
#             oai_harvesting.update({url: email})
#         else: oai_harvesting.update({url: None})
#     except (ConnectionError, HTTPError, ChunkedEncodingError, ContentDecodingError):
#         oai_harvesting.update({url: None})

# oai_harvesting = {}
# with ThreadPoolExecutor() as excecutor:
#     list(tqdm(excecutor.map(get_oai_email, oai_urls),total=len(oai_urls)))
    
    
    
    
    
    
    