import pandas as pd
import matplotlib.pyplot as plt
from sentence_transformers import SentenceTransformer, util
from tqdm import tqdm

#%%
# 1. Wczytanie danych
# df = pd.read_csv("jesuits_conflict")
# df = pd.read_csv("dominican_conflict.csv")
# df = pd.read_csv("franciscan_conflict.csv")

# 2. Załadowanie modelu osadzeń
model = SentenceTransformer('distiluse-base-multilingual-cased-v2')

# 3. Definicja prototypowych haseł
# conflict_words = ["war", "battle", "conflict", "rebellion", "martyrdom", "persecution"]
# peace_words    = ["peace", "dialog", "reconciliation", "treaty", "council", "mediation"]

conflict_words = list(set(["conflict", "dispute", "clash", "confrontation", "tension", "hostility", "antagonism", "discord", "strife", "friction", "rivalry", "feud", "battle", "warfare", "skirmish", "fight", "struggle", "opposition", "contention", "impasse", "stalemate", "collision", "belligerence", "enmity", "dissent", "grievance", "quarrel", "disagreement", "breakdown", "confront", "resistance", "turmoil", "upheaval", "discordance", "polarization", "antagonistic", "hostile", "turbulence", 'war', 'rebellion', 'martyrdom', 'persecution']))
peace_words = list(set(["peace", "harmony", "tranquility", "serenity", "calm", "quiet", "stillness", "accord", "concord", "unity", "solidarity", "agreement", "understanding", "tolerance", "mutuality", "cooperation", "collaboration", "reconciliation", "conciliation", "mediation", "dialogue", "rapprochement", "settlement", "resolution", "pacification", "peacemaking", "pacifism", "nonviolence", "goodwill", "amity", "friendship", "stability", "equanimity", "composure", "balance", "civility", "détente", "solidification", "cohesion", "mutual respect", 'dialog', 'treaty', 'council']))

# 4. Obliczenie osadzeń i ręczne uśrednienie
conflict_embeddings = model.encode(conflict_words, convert_to_tensor=True)
peace_embeddings    = model.encode(peace_words,    convert_to_tensor=True)

conflict_proto = conflict_embeddings.mean(dim=0, keepdim=True)  # (1, dim)
peace_proto    = peace_embeddings.mean(dim=0,    keepdim=True)

# 5. Funkcja obliczająca semantyczne podobieństwo
def semantic_scores(text):
    emb = model.encode(str(text), convert_to_tensor=True).unsqueeze(0)
    sim_conflict = util.cos_sim(emb, conflict_proto).item()
    sim_peace    = util.cos_sim(emb, peace_proto).item()
    return sim_conflict, sim_peace

dfs = {'A semantic conflict map of the Society of Jesus': pd.read_csv("data/Frankfurt/jesuits_conflict.csv"),
        'A semantic conflict map of the Dominican Order': pd.read_csv("data/Frankfurt/dominican_conflict.csv"),
        'A semantic conflict map of the Franciscan Order': pd.read_csv("data/Frankfurt/franciscan_conflict.csv"),
        'A semantic conflict map of the generals of the Society of Jesus': pd.read_csv("data/Frankfurt/generals_jesuits_conflict.csv"),
        'A semantic conflict map of the Jesuit Missionaries': pd.read_csv("data/Frankfurt/jesuits_missionaries.csv")}

for title, df in tqdm(dfs.items()):
    
    # df = pd.read_csv("data/Frankfurt/franciscan_conflict.csv")
    # title = 'A semantic map of the conflictuality of the Society of Jesus'
# 7. Agregacja na poziomie osoby
    
    # 6. Obliczenie podobieństw dla każdej trójki
    df[['sim_conflict', 'sim_peace']] = df['entity'].apply(
        lambda x: pd.Series(semantic_scores(x))
    )
    
    scores = df.groupby('person').agg({
        'sim_conflict': 'sum',
        'sim_peace': 'sum'
    }).reset_index()
    
    # 8. Przekształcenie na osie: X i Y
    scores['X'] = scores['sim_peace'] - scores['sim_conflict']
    scores['Y'] = scores['sim_peace'] + scores['sim_conflict']
    
    # 9. Wizualizacja
    plt.figure(figsize=(8,6))
    plt.scatter(scores['X'], scores['Y'], marker='x')
    plt.axhline(0, color='gray', linestyle='--')
    plt.axvline(0, color='gray', linestyle='--')
    plt.xlabel('Peace – Conflict (X)')
    plt.ylabel('Low Intensity – High Intensity (Y)')
    # plt.title('Mapa semantyczna jezuitów: nastawienie vs intensywność')
    # plt.title('Mapa semantyczna dominikanów: nastawienie vs intensywność')
    plt.title(title)
    plt.grid(True)
    
    plt.xlim(-1.2, 1.0)
    plt.ylim(-2, 40)
    
    plt.savefig(
    f"data/Frankfurt/{title}.png",    # nazwa pliku, format określa się przez rozszerzenie
    dpi=300,                # rozdzielczość w dpi (domyślnie 100)
    bbox_inches="tight",    # przytnie marginesy wokół wykresu
    transparent=True       # True, jeśli chcesz przezroczyste tło
    )

# for _, row in scores.iterrows():
#     plt.text(row['X'], row['Y'], row['person'], fontsize=8, alpha=0.7)

# plt.tight_layout()
# plt.show()
