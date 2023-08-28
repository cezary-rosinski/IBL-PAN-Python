import json
import pandas as pd
import matplotlib.pyplot as plt
from tqdm import tqdm
import seaborn as sns


#%%
with open('data/oesterle_bn_stats.json', 'rt', encoding='utf-8') as f:
    data = json.load(f)
data.pop('polish literature')   
    
{tuple(v.keys()) for k,v in data.items()}


test = {k:v for k,v in data.items() if k in ['belarusian literature', 'ukrainian literature', 'austrian literature', 'american literature', 'german literature', 'french literature', 'spanish literature', 'japanese literature']}

belarusian = pd.DataFrame().from_dict(test.get('belarusian literature'), orient='index').sort_index()
ukrainian = pd.DataFrame().from_dict(test.get('ukrainian literature'), orient='index').reset_index().rename(columns={'index': 'year'})
austrian = pd.DataFrame().from_dict(test.get('austrian literature'), orient='index').reset_index().rename(columns={'index': 'year'})
american = pd.DataFrame().from_dict(test.get('american literature'), orient='index').sort_index()
german = pd.DataFrame().from_dict(test.get('german literature'), orient='index').reset_index().rename(columns={'index': 'year'})
french = pd.DataFrame().from_dict(test.get('french literature'), orient='index').reset_index().rename(columns={'index': 'year'})
spanish = pd.DataFrame().from_dict(test.get('spanish literature'), orient='index').reset_index().rename(columns={'index': 'year'})
japanese = pd.DataFrame().from_dict(test.get('japanese literature'), orient='index').reset_index().rename(columns={'index': 'year'})



belarusian.plot()
american.plot()





# Dane przykładowe (możesz zastąpić je swoimi danymi)
literatury_narodowe = ['Polska', 'USA', 'Francja', 'Japonia']
podmiotowa = [120, 180, 150, 90]
przedmiotowa = [90, 150, 100, 80]

# Tworzenie wykresu
plt.figure(figsize=(10, 6))
plt.bar(literatury_narodowe, podmiotowa, label='Literatura podmiotowa')
plt.bar(literatury_narodowe, przedmiotowa, label='Literatura przedmiotowa', alpha=0.5)

# Dodanie etykiet, tytułu, legendy
plt.xlabel('Literatury narodowe')
plt.ylabel('Liczba wydanych książek')
plt.title('Porównanie literatury podmiotowej i przedmiotowej dla różnych literatur narodowych')
plt.legend()

# Pokazanie wykresu
plt.tight_layout()
plt.show()


#


# Tworzenie wykresów
fig, axes = plt.subplots(nrows=len(data), ncols=1, figsize=(10, 6 * len(data)))

for i, (literature, values) in tqdm(enumerate(test.items()), total = len(data)):
    years = list(values.keys())
    literature_values = [entry["literature"] for entry in values.values()]
    secondary_values = [entry["secondary"] for entry in values.values()]

    ax = axes[i]
    ax.bar(years, literature_values, label='Literatura podmiotowa')
    ax.bar(years, secondary_values, label='Literatura przedmiotowa', alpha=0.5)
    ax.set_title(f'Porównanie literatury podmiotowej i przedmiotowej w {literature}')
    ax.set_xlabel('Rok')
    ax.set_ylabel('Liczba wydanych książek')
    ax.legend()

plt.tight_layout()
plt.show()


#

# Tworzenie list dla box plot
box_data = []

for literature, values in test.items():
    literature_values = [entry["literature"] for entry in values.values()]
    secondary_values = [entry["secondary"] for entry in values.values()]
    
    box_data.extend(literature_values)
    box_data.extend(secondary_values)

# Tworzenie list dla scatter plot
scatter_data = []

for literature, values in test.items():
    literature_values = [entry["literature"] for entry in values.values()]
    secondary_values = [entry["secondary"] for entry in values.values()]
    
    for year, literature_val, secondary_val in zip(values.keys(), literature_values, secondary_values):
        scatter_data.append((literature, year, literature_val, secondary_val))

# Tworzenie DataFrame dla scatter plot
scatter_df = pd.DataFrame(scatter_data, columns=['Literatura', 'Rok', 'Literatura podmiotowa', 'Literatura przedmiotowa'])

# Tworzenie wykresu pudełkowego (box plot)
plt.figure(figsize=(10, 6))
sns.boxplot(data=box_data)
plt.title('Wykres pudełkowy dla literatury podmiotowej i przedmiotowej')
plt.xlabel('Rodzaj literatury')
plt.ylabel('Liczba wydanych książek')
plt.show()

# Tworzenie wykresu punktowo-liniowego (scatter plot)
plt.figure(figsize=(10, 6))
sns.scatterplot(data=scatter_df, x='Literatura podmiotowa', y='Literatura przedmiotowa', hue='Literatura', palette='Set1')
plt.title('Wykres punktowo-liniowy dla literatury podmiotowej i przedmiotowej')
plt.xlabel('Literatura podmiotowa')
plt.ylabel('Literatura przedmiotowa')
plt.legend(title='Literatura')
plt.show()


#


# Przygotowanie danych do wykresu
scatter_data = []

for literature, values in test.items():
    years = list(values.keys())
    total_podmiotowa = [entry["literature"] for entry in values.values()]
    total_przedmiotowa = [entry["secondary"] for entry in values.values()]
    total_combined = [p + s for p, s in zip(total_podmiotowa, total_przedmiotowa)]
    
    scatter_data.extend(zip(years, total_combined, [literature] * len(years)))

# Tworzenie DataFrame z danymi
scatter_df = pd.DataFrame(scatter_data, columns=['Rok', 'Suma książek', 'Literatura'])

# Posortuj DataFrame według roku (oś x) rosnąco
scatter_df = scatter_df.sort_values(by='Rok')

# Użycie innego palety kolorów
palette = "Set1"  # Change this to the desired palette name

# Tworzenie wykresu punktowo-liniowego (scatter plot) z wybraną paletą kolorów i posortowanymi danymi
plt.figure(figsize=(10, 6))
sns.set_palette(palette)  # Set the desired palette
for literature in scatter_df['Literatura'].unique():
    subset = scatter_df[scatter_df['Literatura'] == literature]
    plt.scatter(subset['Rok'], subset['Suma książek'], label=literature, alpha=0.7)

plt.title('Scatter plot - Suma książek podmiotowych i przedmiotowych w różnych latach')
plt.xlabel('Rok wydania książki')
plt.ylabel('Suma książek')
plt.legend()
plt.show()
















