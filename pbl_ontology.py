from owlready2 import *
import types
import io
import pandas as pd
import re
from my_functions import f, df_to_mrc, mrc_to_mrk

pbl = get_ontology('http://test.org/pbl.owl')

class Book(Thing):
    namespace = pbl
    
print(Book.iri)

class AuthorsAndAnonymousBook(Book):
    pass

print(AuthorsAndAnonymousBook.is_a)

print(AuthorsAndAnonymousBook.ancestors())

class CollectiveBook(Book):
    pass



# bibframe ontology
    
bibframe = get_ontology('http://id.loc.gov/ontologies/bibframe/')
























# najdłuższy rekord książki w MARC w PBL

paths_in = ['F:/Cezary/Documents/IBL/Libri/Iteracja 10.2020/pbl_marc_books.mrk', 
            'F:/Cezary/Documents/IBL/Libri/Iteracja 10.2020/libri_marc_bn_books.mrk']

full_data = pd.DataFrame()  

r_len = 0
max_r_len = 0
r_with_max_len = ''

for path_in in paths_in:
    reader = io.open(path_in, 'rt', encoding = 'utf-8').read().splitlines()
    mrk_list = []
    for row in reader:
        if '=856' in row or '=650' in row:
            pass
        elif '=LDR' not in row:
            mrk_list[-1] += '\n' + row
        else:
            mrk_list.append(row)
    mrk_list = [r.splitlines() for r in mrk_list]
    r_len = len(max(mrk_list, key=len))
    if r_len > max_r_len:
        max_r_len = r_len
        r_with_max_len = max(mrk_list, key=len)
  



# Bibframe | MARC21 | PBL WAW | PBL ORACLE
# marc21 to bibframe to pbl waw?































