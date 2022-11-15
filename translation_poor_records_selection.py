import pandas as pd
from tqdm import tqdm

translations_df = pd.read_excel('translations_after_manual_2022-11-02.xlsx') 
translations_df_clustered = translations_df.loc[translations_df['work_id'].notnull()]
all_grouped = translations_df_clustered.groupby(['author_id', 'language', 'year'])
all_grouped_groups = list(all_grouped.groups.keys())

test = translations_df.head(100)

without_clusters = translations_df.loc[translations_df['work_id'].isnull()]
without_clusters.columns.values
groupby = without_clusters.groupby(['author_id', 'language', 'year'])
groupby_groups = list(groupby.groups.keys())

groupby_groups_ok = [e for e in groupby_groups if e not in all_grouped_groups]

test_list = []
for group in tqdm(groupby_groups):
    for group2 in all_grouped_groups:
        if group == group2:
            test_list.append(group)
            continue
        

new_poor_records = groupby.head(1).reset_index(drop=True) #4622 to 4613

selected_columns = new_poor_records[['author_id', 'language', 'year']].sort_values(['author_id', 'language', 'year'])
