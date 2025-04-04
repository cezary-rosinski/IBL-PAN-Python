import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.metrics import accuracy_score
import ast
from tqdm import tqdm

#%%
# Wczytanie danych z pliku Excel (dostosuj nazwę pliku i arkusz w razie potrzeby)
df = pd.read_excel("data/Gortych/gewalt_research_examples.xlsx")
# df = pd.read_excel("data/Gortych/gortych_perlentaucher.xlsx", sheet_name='Sheet1')

# Załóżmy, że kolumny nazywają się: 'year', 'keywords', 'description', 'reviews', 'important_for_research'
# Przygotowanie kolumny tekstowej – łączymy opis, recenzje i słowa kluczowe
df['keywords'] = df['keywords'].apply(lambda x: ast.literal_eval(x))
df['keywords_text'] = df['keywords'].apply(lambda x: " ".join(x) if isinstance(x, set) else str(x))
df['rezensionsnotiz'] = df['rezensionsnotiz'].apply(lambda x: ast.literal_eval(x))
df['stichworter'] = df['stichworter'].apply(lambda x: ast.literal_eval(x))
df['combined_text'] = df.apply(
    lambda row: ' '.join(filter(None, [
        ' '.join(row['rezensionsnotiz']) if row['rezensionsnotiz'] else '',
        row['klappentext'] if row['klappentext'] else '',
        row['keywords_text'] if row['keywords_text'] else '',
        ' '.join(row['stichworter']) if row['stichworter'] else ''
    ])),
    axis=1
)

# Przygotowanie zbioru cech i etykiety
X = df[['year', 'combined_text']]
y = df['important for research']

# Podział na zbiór treningowy i testowy
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Definicja przetwarzania kolumn
preprocessor = ColumnTransformer(
    transformers=[
        ('text', TfidfVectorizer(stop_words='english'), 'combined_text'),
        ('year', 'passthrough', ['year'])
    ]
)

# Budowanie pipeline’u: preprocessing -> klasyfikator
pipeline = Pipeline(steps=[
    ('preprocessor', preprocessor),
    ('classifier', RandomForestClassifier(random_state=42))
])

# Trenowanie klasyfikatora
pipeline.fit(X_train, y_train)

# Ewaluacja na zbiorze testowym
y_pred = pipeline.predict(X_test)
print("Accuracy:", accuracy_score(y_test, y_pred))

# Opcjonalnie: GridSearchCV do optymalizacji hiperparametrów
param_grid = {
    'classifier__n_estimators': [50, 100, 150],
    'classifier__max_depth': [None, 10, 20]
}

grid_search = GridSearchCV(pipeline, param_grid, cv=5, scoring='accuracy')
grid_search.fit(X_train, y_train)
print("Best parameters:", grid_search.best_params_)
print("Best cross-val accuracy:", grid_search.best_score_)

# Użycie wytrenowanego modelu na nowych danych
def predict_book_usefulness(year, description, reviews, keywords_dg, keywords):
    reviews = " ".join(reviews) if isinstance(reviews, list) else str(reviews)
    keywords_dg = " ".join(keywords_dg) if isinstance(keywords_dg, set) else str(keywords_dg)
    keywords_text = " ".join(keywords) if isinstance(keywords, list) else str(keywords)
    combined_text = f"{description} {reviews} {keywords_dg} {keywords_text}"
    data = pd.DataFrame({'year': [year], 'combined_text': [combined_text]})
    prediction = pipeline.predict(data)
    return prediction[0]

# Przykład użycia funkcji
example_prediction = predict_book_usefulness(
    year=2005,
    description="Przykładowy opis książki zawierający odniesienia do przemocy prawicowej i neonazizmu.",
    reviews="Recenzja podkreśla intersekcjonalne ujęcie przemocy.",
    keywords=["przemoc prawicowa", "intersekcjonalna"]
)

example_prediction = predict_book_usefulness(
    year=2023,
    description='Deutschland, Anfang der siebziger Jahre: ein Land voller Angst vor allem Fremden. Der einzige Italiener an der Schule wirkt wie ein außerirdisches Wesen. In den Achtzigern sind es die Türken, die zum ersten Mal die Tische vor die Wirtschaft stellen. Während die Wetterauer den ersten Döner im Landkreis als Widerstandsnahrung feiern, erobert der lange verschwundene Hitler den öffentlichen Raum in Funk und Fernsehen. In den Neunzigern träumt der Erzähler seinen großen Traum vom Wetterauer Land, verschwindet allerdings erst mal mit seiner Cousine unter einer Bettdecke am Ostrand der neuen Republik. Die Heimkunft gelingt innerfamiliär, das Haus der Großmutter wird als musealer Ort rekonstruiert, während im Ort wenigstens der Grundriss der 1938 niedergebrannten Synagoge wiederhergestellt wird. Aber noch im neuen Jahrtausend, als die ganze Republik ständig den Begriff "Heimat" diskutiert, will niemand vom früheren Leben in der konkreten Heimat wissen, als es die noch gab, die es seit ihrer Deportation nicht mehr gab. Mit Gespür für alles Abgründige in der gelebten Normalität erzählt Andreas Maier von Deutschland zwischen Weltkrieg, Mauerfall und Jahrtausendwende; davon, wie es sich die Menschen gemütlich machen in vierzig Jahren Geschichte.',
    reviews=['Ebenso "intim wie ausschweifend" ist der neunte Band von Andreas Maiers Erzählzyklus "Ortsumgehung" Rezensent Paul Jandl zufolge. Der Autor setzt sich mit seiner Kindheit und Jugend in der Wetterau bei Frankfurt auseinander und zeichnet gleichzeitig ein Bild Deutschlands von den siebziger bis zu den neunziger Jahren, lesen wir. Maier wirft dabei anekdotische Schlaglichter, so Jandl auf eine "linksutopische Jugend mit Batiktüchern", die Aufklärung über den Holocaust in der Schule, den Mauerfall. Der Roman wird dabei durchzogen von der Frage, was Heimat sein kann und wem man die Definition von Heimat gerade nicht überlassen sollte, schreibt der Kritiker, nämlich denen, die mit ihr nur "herumtümeln" wollen. Vor allem die Erinnerung selbst wird in diesem Roman zur Heimat, schließt Jandl.', 'Was zunächst wie eine gekonnt erzählte Anekdote erscheinen mag, trifft in Wahrheit ins "Grundsätzliche", betont Rezensent Dirk Knipphals. Andreas Meier schreibt deutsche Mentalitätsgeschichte, und zwar immer entlang konkreter, individueller Erfahrungen. Die Dynamik seines Textes gleicht allerdings weniger einem bedächtigen Schürfen, sondern einem sprunghaften Zupacken. Die Szenen aus vergangenen Alltagen, die er so erhascht, zeigen eindrücklich, wie weit entfernt von uns diese Vergangenheiten einerseits sind und wie präsent sie doch andererseits sind als Grundierung gegenwärtiger Lebenswelten. Anders gesagt: Es geht darum, sich Heimat zu erschreiben - was sowohl Aneignungs- als auch Distanzierungsprozesse mit einschließt. Schließlich ist Meier in einem Spalt aufgewachsen, zwischen einem "affirmativ noch mit Blut und Boden gründelnden" Heimatbegriff einerseits und der kritischen Ablehnung in linken Diskursen andererseits. Diese Lücke will er nicht etwa schließen, betont Knipphals, sondern eher "literarisch ausmessen". Doch all das, was sich über diesen Roman sagen lässt, bzw. was sich über das sagen lässt, was dieser Roman sagen kann, klingt viel zu abstrakt, bemerkt der Rezensent, besser liest man es bei Meier - stets konkret.', 'Rezensent Marcus Hladec liest mit diesem Buch von Andreas Maier, das den neunten Band seines erzählerischen Zyklus "Ortsumgehung" darstellt, einen "autobiografischen Essay-Roman", in dem der Autor Erinnerungen an seine eigene Heimat niederschreibt, und dabei die Bedeutung des Begriffes in all seinen Dimensionen zu fassen versucht. Maiers Buch beginnt in den siebziger Jahren, so der Rezensent, Heimatfilme laufen im Fernsehen, die Deutschen haben Angst vor allem Fremden, in der Schule werden Ausländer ausgegrenzt. Von hier bis in die Nuller-Jahre greift der Autor unterschiedliche Aspekte des Heimat-Themas auf, einen Schüleraustausch, die NS-Aufklärung in der Schule, die Mauer und ihren Fall, bis hin zum ehelich-häuslichen Leben als Erwachsener. Mit Wortexperimenten und philosophischen Reflexionen umkreist der Autor dabei den Begriff und das Konzept "Heimat", schreibt Hladec, und zeigt dabei dem Rezensenten zu Folge vor allem, wie schillernd dessen Bedeutung ist.', 'Der neunte autofiktionale Streich von Andreas Maier liegt vor und Rezensentin Martina Wagner-Egelhaaf ist sehr angetan von den Kindheitserinnerungen des Autors, der sich die Wirtschaftswunderjahre vorgeknöpft hat. Was und wie Maier über die Zeit knapp 15 Jahre nach der Shoah aus der Ich-Perspektive erzählt, ist in einem pädagogisch klugen Duktus geschrieben und wirke dabei zugleich melancholisch und rigide, findet die Rezensentin. Heimat sei für den Autor ein Synonym für das Schweigen. Dessen Dämonen hätten Maier zu einem Heimatlosen gemacht. Wagner-Egelhaaf kann gut nachvollziehen, wieso der Autor sein Buch Edgar Reitz gewidmet hat.'],
    keywords_dg = {'neunziger jahre', 'mauerfall', 'neunziger ', 'mauer', 'wende'},
    keywords=['Maier, Andreas', 'Heimat', 'Bundesrepublik Deutschland', 'Bonner Republik', 'Wende', 'Westdeutschland', 'Autofiktion', 'Wirtschaftswunder']
)


print("Przydatna do badań:", example_prediction)


#%%

def predict_book_usefulness(row):
    reviews = " ".join(row['rezensionsnotiz']) if isinstance(row['rezensionsnotiz'], list) else str(row['rezensionsnotiz'])
    keywords_dg = " ".join(row['keywords']) if isinstance(row['keywords'], set) else str(row['keywords'])
    keywords_text = " ".join(row['stichworter']) if isinstance(row['stichworter'], list) else str(row['stichworter'])
    description = row['klappentext']
    combined_text = f"{description} {reviews} {keywords_dg} {keywords_text}"
    data = pd.DataFrame({'year': [row['year']], 'combined_text': [combined_text]})
    prediction = pipeline.predict(data)
    return bool(prediction[0])


df_to_classify = pd.read_excel("data/Gortych/gortych_perlentaucher.xlsx", sheet_name='Sheet1')

df_to_classify['keywords'] = df_to_classify['keywords'].apply(lambda x: ast.literal_eval(x))
df_to_classify['keywords_text'] = df_to_classify['keywords'].apply(lambda x: " ".join(x) if isinstance(x, set) else str(x))
df_to_classify['rezensionsnotiz'] = df_to_classify['rezensionsnotiz'].apply(lambda x: ast.literal_eval(x))
df_to_classify['stichworter'] = df_to_classify['stichworter'].apply(lambda x: ast.literal_eval(x))

results = []
for i, row in tqdm(df_to_classify.iterrows(), total=df_to_classify.shape[0]):
    results.append(predict_book_usefulness(row))
    
df_test = pd.DataFrame(results)
























