from crossref.restful import Works
from tqdm import tqdm

works = Works()
test = works.doi('10.1086/448236')

'author',
'crated'.get('date-time')
'publisher'
'title'
'resource'.get('primary').get('URL')


test2 = works.doi('10.7910/DVN/GQQKWK')

result = []
for e in tqdm(a):
    test = works.doi(e)
    result.append(test)