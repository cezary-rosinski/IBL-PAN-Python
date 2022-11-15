import pandas as pd
from tqdm import tqdm
from my_functions import marc_parser_dict_for_field as marc_parser
from collections import Counter
import json

translations_df = pd.read_excel('translations_after_manual_2022-11-02.xlsx') 
translations_df_clustered = translations_df.loc[translations_df['work_id'].notnull()]
clustered_grouped = translations_df_clustered.groupby('work_id')

# list(clustered_grouped.groups.keys())[100]
audience_fiction_dict = {}
for name, group in tqdm(clustered_grouped, total=len(clustered_grouped)):
    name = int(name)
    author_id = group['author_id'].to_list()[0]
    work_id = name
    author_name = Counter([ele for sub in [[el['$a'] for el in marc_parser(e, '\\$') if '$a' in el] for e in group['100'].to_list()] for ele in sub]).most_common(1)[0][0].strip()
    work_name = Counter(group['simple_target_title'].to_list()).most_common(1)[0][0]
    audience = '❦'.join(group['audience'].to_list()).split('❦')
    fiction_type = '❦'.join(group['fiction_type'].to_list()).split('❦')
    temp_dict = {'author_id': author_id,
                 'author_name': author_name,
                 'work_id': work_id,
                 'work_name': work_name,
                 'audience': audience,
                 'fiction_type': fiction_type}
    audience_fiction_dict.update({name:temp_dict})

audience_fiction_df = pd.DataFrame(audience_fiction_dict.values())    

with open('audience_fiction.json', 'wt', encoding='utf-8') as file:
    json.dump(audience_fiction_dict, file, indent=4, ensure_ascii=False)
    
audience_fiction_df.to_excel('audience_fiction.xlsx', index=False)    
    
    
    
    
    
    
    
    
    
    


#czy uwzględniać tutaj też utwory, które nie mają work_id (np. grupując je po AYL)?