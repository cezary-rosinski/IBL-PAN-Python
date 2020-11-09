import io
import pandas as pd
import re
from my_functions import f, df_to_mrc, mrc_to_mrk

paths_in = ['F:/Cezary/Documents/IBL/Libri/Iteracja 10.2020/pbl_marc_books.mrk', 
            'F:/Cezary/Documents/IBL/Libri/Iteracja 10.2020/pbl_marc_articles.mrk',
            'F:/Cezary/Documents/IBL/Libri/Iteracja 10.2020/libri_marc_bn_books.mrk',
            'F:/Cezary/Documents/IBL/Libri/Iteracja 10.2020/libri_marc_bn_articles.mrk']

full_data = pd.DataFrame()  

for path_in in paths_in:
    reader = io.open(path_in, 'rt', encoding = 'utf-8').read().splitlines()
    mrk_list = []
    for row in reader:
        if '=LDR' not in row:
            mrk_list[-1] += '\n' + row
        else:
            mrk_list.append(row)
    mrk_list = [r for r in mrk_list if re.findall('\=650.+czeska', r.lower())]    
    for record in mrk_list:
        record = record.split('=')
        record = list(filter(None, record))
        for i, row in enumerate(record):
            record[i] = record[i].rstrip().split('  ', 1)
        df = pd.DataFrame(record, columns = ['field', 'content'])
        df['id'] = df.apply(lambda x: f(x, '001'), axis = 1)
        df['id'] = df['id'].ffill().bfill()
        df['content'] = df.groupby(['id', 'field'])['content'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
        df = df.drop_duplicates().reset_index(drop=True)
        df_wide = df.pivot(index = 'id', columns = 'field', values = 'content')
        full_data = full_data.append(df_wide)
        
fields_order = full_data.columns.tolist()
fields_order.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
full_data = full_data.reindex(columns=fields_order)

df_to_mrc(full_data, '❦', 'F:/Cezary/Documents/IBL/Libri/Iteracja 10.2020/cz_from_pbl.mrc')

mrc_to_mrk('F:/Cezary/Documents/IBL/Libri/Iteracja 10.2020/cz_from_pbl.mrc', 'F:/Cezary/Documents/IBL/Libri/Iteracja 10.2020/cz_from_pbl.mrk')
        































