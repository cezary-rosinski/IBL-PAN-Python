import pandas as pd
from ast import literal_eval
from collections import Counter

df = pd.read_excel('translations_after_second_manual_2022-09-02.xlsx')

test = df.head(10)
df.columns.values

test = df.loc[df['work_id'] == 4724584]

geo_df = df[['001', 'geonames_country']]
geo_df = geo_df.loc[geo_df['geonames_country'].notnull()]
geo_df['geonames_country'] = geo_df['geonames_country'].apply(lambda x: literal_eval(x))
geo_df = geo_df.explode('geonames_country')

geo_counter = Counter(geo_df['geonames_country'])
geo_dict = dict(geo_counter)

dict_conversion = {'Bosnia and Herzegovina': 'Bosnia and Herz.',
                   'Dominican Republic': 'Dominican Rep.',
                   'Faroe Islands': 'Faeroe Is.',
                   'North Macedonia': 'Macedonia',
                   'United States': 'United States of America',
                   'Vatican City': 'Vatican'}

new_df = pd.DataFrame.from_dict(geo_dict, orient='index').reset_index().rename(columns={'index':'Country', 0:'Books'})
new_df['Country'] = new_df['Country'].apply(lambda x: dict_conversion.get(x, x))
new_df.to_csv('translations_cartogram.csv', index=False)


no_country_df = df.loc[df['geonames_country'].isnull()]

#generate cartogram here: https://go-cart.io/cartogram