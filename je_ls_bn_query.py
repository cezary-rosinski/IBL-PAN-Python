import requests

url = 'https://data.bn.org.pl/api/networks/bibs.json?'
params = {'limit': 100, 'marc': '773t Literatura na Świecie'}


result = requests.get(url, params=params)
result = result.json()
next_page = result.get('nextPage')

start_url = 'https://data.bn.org.pl/api/networks/bibs.marc?limit=100&amp;marc=773t+Literatura+na+%C5%9Awiecie'
#link powyżej też trzeba edytować; najlepiej zaznaczając w API BN format marc i skopiować URL


ls_response = requests.get(start_url).text
progress = 1
while next_page:
    print(progress)
    new_next_url = next_page.replace('json?', 'marc?')
    temp_response = requests.get(new_next_url).text
    ls_response += temp_response
    result = requests.get(next_page)
    result = result.json()
    next_page = result.get('nextPage')
    progress += 1
    
with open('ls_bn.mrc', 'w', encoding='utf-8') as file:
    file.write(ls_response)