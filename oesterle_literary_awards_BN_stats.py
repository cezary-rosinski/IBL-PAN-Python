import json
import pandas as pd
import matplotlib.pyplot as plt
from tqdm import tqdm
import seaborn as sns
from collections import OrderedDict


#%%

# path = 'data/oesterle_bn_stats.json'
path = 'data/four_categories_new data_from_Czarek_files.json'

with open(path, 'rt', encoding='utf-8') as f:
    data = json.load(f)
data.pop('polish literature')
    
# test = {k:v for k,v in data.items() if k in ['belarusian literature', 'ukrainian literature', 'austrian literature', 'german literature', 'french literature', 'spanish literature', 'japanese literature']}

test = {k:v for k,v in data.items() if k in ['belarusian literature', 'ukrainian literature', 'austrian literature', 'american literature', 'german literature', 'french literature', 'spanish literature', 'japanese literature']}

test = OrderedDict(test)


#other należy do secondary doliczyć


belarusian = pd.DataFrame().from_dict(test.get('belarusian literature'), orient='index').sort_index()
ukrainian = pd.DataFrame().from_dict(test.get('ukrainian literature'), orient='index').reset_index().rename(columns={'index': 'year'})
austrian = pd.DataFrame().from_dict(test.get('austrian literature'), orient='index').sort_index()
american = pd.DataFrame().from_dict(test.get('american literature'), orient='index').sort_index()
german = pd.DataFrame().from_dict(test.get('german literature'), orient='index').reset_index().rename(columns={'index': 'year'})
french = pd.DataFrame().from_dict(test.get('french literature'), orient='index').reset_index().rename(columns={'index': 'year'})
spanish = pd.DataFrame().from_dict(test.get('spanish literature'), orient='index').reset_index().rename(columns={'index': 'year'})
japanese = pd.DataFrame().from_dict(test.get('japanese literature'), orient='index').reset_index().rename(columns={'index': 'year'})



belarusian.plot()
american.plot()



# Tworzenie wykresu zbiorczego dla wszystkich literatur
fig, axes = plt.subplots(nrows=len(test), ncols=1, figsize=(10, 6 * len(test)), dpi=200)

for i, (literature, values) in tqdm(enumerate(test.items()), total = len(test)):
    years = list(values.keys())
    literature_values = [entry["literature"] for entry in values.values()]
    secondary_values = [entry["secondary"] for entry in values.values()]

    sorted_indices = sorted(range(len(years)), key=lambda k: years[k])
    years = [years[i] for i in sorted_indices]
    literature_values = [literature_values[i] for i in sorted_indices]
    secondary_values = [secondary_values[i] for i in sorted_indices]    

    ax = axes[i]
    ax.bar(years, literature_values, label='Literature')
    ax.bar(years, secondary_values, label='Secondary', alpha=0.5)
    ax.set_title(f'Comparison of literature and secondary in {literature}')
    ax.set_xlabel('Year')
    ax.set_ylabel('Number of books published')
    ax.legend()
    
    ax.set_xticklabels(years, rotation=90)

plt.tight_layout()
plt.show()

# Tworzenie wykresu z udziałem literatury przedmiotowej lub odsetkiem literatury przedmiotowej
ttt = {k:v for k,v in test.items() if k == 'austrian literature'}
fig, axes = plt.subplots(nrows=len(test), ncols=1, figsize=(10, 6 * len(test)), dpi=200)

for i, (literature, values) in tqdm(enumerate(test.items()), total = len(test)):
    i = 0
    literature, values = tuple(test.items())[i]
    years = list(values.keys())
    literature_values = [entry["literature"] for entry in values.values()]
    secondary_values = [entry["secondary"] for entry in values.values()]
    
    graph_values = [l + s for l, s in zip(literature_values, secondary_values)]

    sorted_indices = sorted(range(len(years)), key=lambda k: years[k])
    years = [years[i] for i in sorted_indices]
    literature_values = [literature_values[i] for i in sorted_indices]
    # secondary_values = [round(secondary_values[i]/graph_values[i], 2) for i in sorted_indices]   
    secondary_values = [round(secondary_values[i]/literature_values[i], 2) for i in sorted_indices]  

    ax = axes[i]
    # ax.bar(years, literature_values, label='Literature')
    ax.bar(years, secondary_values, label='Secondary', alpha=0.5)
    ax.set_title(f'Percentage of the secondary literature in {literature}')
    ax.set_xlabel('Year')
    ax.set_ylabel('Percentage of the secondary literature')
    ax.legend()
    
    ax.set_xticklabels(years, rotation=90)

plt.tight_layout()
plt.show()

# Przygotowanie danych do wykresu scatter plot
scatter_data = []

for literature, values in test.items():
    years = list(values.keys())
    total_combined = [entry["literature"] for entry in values.values()]
    # total_podmiotowa = [entry["literature"] for entry in values.values()]
    # total_przedmiotowa = [entry["secondary"] for entry in values.values()]
    # total_combined = [p + s for p, s in zip(total_podmiotowa, total_przedmiotowa)]
    
    scatter_data.extend(zip(years, total_combined, [literature] * len(years)))

# Tworzenie DataFrame z danymi
scatter_df = pd.DataFrame(scatter_data, columns=['Rok', 'Suma książek', 'Literatura'])

# Posortuj DataFrame według roku (oś x) rosnąco
scatter_df = scatter_df.sort_values(by='Rok')

# Użycie innego palety kolorów
palette = "Set1"  # Change this to the desired palette name

# Tworzenie wykresu punktowo-liniowego (scatter plot) z wybraną paletą kolorów i posortowanymi danymi
plt.figure(figsize=(10, 6), dpi=150)
sns.set_palette(palette)  # Set the desired palette
for literature in scatter_df['Literatura'].unique():
    subset = scatter_df[scatter_df['Literatura'] == literature]
    plt.scatter(subset['Rok'], subset['Suma książek'], label=literature, alpha=0.7)

plt.title('Scatter plot – Sum of literature books in different years')
# plt.title('Scatter plot - Suma książek podmiotowych i przedmiotowych w różnych latach')
plt.xlabel('Year')
plt.ylabel('Sum')
plt.legend()

plt.xticks(rotation=90)

plt.show()

## prace 19.09.2023
# Calculate total summed secondary literature and summed literature for each language
summed_secondary = {}
summed_literature = {}

for language, years_data in test.items():
    summed_secondary[language] = sum(year_data['secondary'] for year_data in years_data.values())
    summed_literature[language] = sum(year_data['literature'] for year_data in years_data.values())

# Calculate the quotient
quotient = {language: summed_secondary[language] / summed_literature[language] for language in test.keys()}

# Create a bar chart
languages = list(quotient.keys())
quotients = list(quotient.values())

plt.figure(figsize=(10, 6), dpi=200)
plt.bar(languages, quotients, color='skyblue')
plt.xlabel('National Literature')
plt.ylabel('Quotient (Secondary Literature / Literature)')
plt.title('Quotient of Secondary Literature divided by Literature in 1989-2023')
plt.xticks(rotation=45)
plt.tight_layout()

plt.show()


# Przygotowanie danych do wykresu scatter plot dla ilorazu przedmiotowa/ podmiotowa
scatter_data = []

for literature, values in test.items():
    years = list(values.keys())
    total_podmiotowa = [entry["literature"] for entry in values.values()]
    total_przedmiotowa = [entry["secondary"] for entry in values.values()]
    # total_combined = [p + s for p, s in zip(total_podmiotowa, total_przedmiotowa)]
    total_combined = [s/p if p>0 else s for p, s in zip(total_podmiotowa, total_przedmiotowa)]
    
    scatter_data.extend(zip(years, total_combined, [literature] * len(years)))

# Tworzenie DataFrame z danymi
scatter_df = pd.DataFrame(scatter_data, columns=['Rok', 'Suma książek', 'Literatura'])

# Posortuj DataFrame według roku (oś x) rosnąco
scatter_df = scatter_df.sort_values(by='Rok')

# Użycie innego palety kolorów
palette = "Set1"  # Change this to the desired palette name

# Tworzenie wykresu punktowo-liniowego (scatter plot) z wybraną paletą kolorów i posortowanymi danymi
plt.figure(figsize=(10, 6), dpi=200)
sns.set_palette(palette)  # Set the desired palette
for literature in scatter_df['Literatura'].unique():
    subset = scatter_df[scatter_df['Literatura'] == literature]
    plt.scatter(subset['Rok'], subset['Suma książek'], label=literature, alpha=0.7)

plt.title('Scatter plot – The quotient of secondary to literature books in different years')
plt.xlabel('Year')
plt.ylabel('Secondary book quotient')
plt.legend()

plt.xticks(rotation=90)

plt.show()









