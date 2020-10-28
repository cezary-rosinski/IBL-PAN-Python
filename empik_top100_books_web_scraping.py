import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime

now = datetime.datetime.now()
year = now.year
month = now.month
day = now.day

empik_books = []

url = f"https://www.empik.com/bestsellery/ksiazki?sort=topShort_Long&resultsPP=60&start=1"
results = requests.get(url)
soup = BeautifulSoup(results.text, "html.parser")

books = soup.findAll('a', attrs={'class': 'img seoImage'})

position = 1
for book in books:
    empik_books.append([position, book['title'], book['href']])
    position += 1

url = 'https://www.empik.com/bestsellery/ksiazki?searchCategory=31&hideUnavailable=true&start=61&resultsPP=60'
results = requests.get(url)
soup = BeautifulSoup(results.text, "html.parser")

books = soup.findAll('a', attrs={'class': 'img seoImage'})

for book in books:
    empik_books.append([position, book['title'], book['href']])
    position += 1


for i, (no, book, url) in enumerate(empik_books):
    print(f"{i+1}/{len(empik_books)}")
    url = 'https://www.empik.com/' + url
    results = requests.get(url)
    soup = BeautifulSoup(results.text, "html.parser")
    description = soup.select_one('.ta-product-description').text.strip()
    empik_books[i].append(description)
    
df = pd.DataFrame(empik_books, columns=['pozycja', 'tytuł + autor', 'link', 'opis'])
df.to_excel(f"empik_top100_books_{year}-{month}-{day}.xlsx", index=False)

print('Done')

