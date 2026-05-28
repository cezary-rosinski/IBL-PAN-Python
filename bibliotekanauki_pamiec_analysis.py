from pathlib import Path
import pandas as pd
import re
import matplotlib.pyplot as plt

#%% slajd 3
BASE = 'data/bn_pamiec_fast/'

# === ŚCIEŻKI ===

METADATA_FULLTEXT = Path(BASE + "pamiec_metadata_fulltext.csv")
CANDIDATE = Path(BASE + "pamiec_outputs/pamiec_02_memory_studies_candidate.csv")

OUT_DIR = Path(BASE + "pamiec_demo_outputs/slide_03")
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_SUMMARY = OUT_DIR / "slide_03_korpus_summary.csv"
OUT_TOP_JOURNALS = OUT_DIR / "slide_03_top_journals.csv"
OUT_TEXT = OUT_DIR / "slide_03_numbers_for_slide.txt"


# === WCZYTANIE KORPUSU KANDYDACKIEGO ===

df = pd.read_csv(CANDIDATE, dtype=str, encoding="utf-8-sig")

df["year_num"] = pd.to_numeric(df["year_num"], errors="coerce")

if "journal" not in df.columns:
    df["journal"] = df["source"].fillna("").str.split(";").str[0].str.strip()

df["journal_clean"] = df["journal"].fillna("").str.strip()
df.loc[df["journal_clean"] == "", "journal_clean"] = "brak danych"

df["has_description_bool"] = df["description"].fillna("").str.strip().ne("")
df["has_pdf_bool"] = df["pdf_url"].fillna("").str.strip().ne("")


# === PEŁNE TEKSTY ===
# Zakładamy, że plik pełnotekstowy ma bn_article_id.
# Czytamy tylko kolumny potrzebne do policzenia pokrycia, żeby nie obciążać pamięci.

candidate_ids = set(df["bn_article_id"].dropna().astype(str))

fulltext_ids = set()
fulltext_rows = 0

CHUNKSIZE = 50_000

for chunk in pd.read_csv(
    METADATA_FULLTEXT,
    dtype=str,
    encoding="utf-8",
    chunksize=CHUNKSIZE,
    low_memory=False
):
    if "bn_article_id" not in chunk.columns:
        raise ValueError("Brakuje kolumny bn_article_id w pamiec_metadata_fulltext.csv")

    chunk["bn_article_id"] = chunk["bn_article_id"].astype(str)
    matched = chunk[chunk["bn_article_id"].isin(candidate_ids)]

    fulltext_rows += len(matched)
    fulltext_ids.update(matched["bn_article_id"].dropna().astype(str).tolist())


# === PODSTAWOWE LICZBY DO SLAJDU ===

summary = {
    "records_candidate": len(df),
    "unique_records_candidate": df["bn_article_id"].nunique(),
    "first_year": int(df["year_num"].min()) if df["year_num"].notna().any() else None,
    "last_year": int(df["year_num"].max()) if df["year_num"].notna().any() else None,
    "journals": df["journal_clean"].nunique(),
    "records_with_description": int(df["has_description_bool"].sum()),
    "records_with_pdf_url": int(df["has_pdf_bool"].sum()),
    "records_with_fulltext": len(fulltext_ids),
    "fulltext_coverage_percent": round(len(fulltext_ids) / len(df) * 100, 1) if len(df) else 0,
}

summary_df = pd.DataFrame([summary])
summary_df.to_csv(OUT_SUMMARY, index=False, encoding="utf-8-sig")


# === TOP CZASOPISMA ===

top_journals = (
    df.groupby("journal_clean")
    .agg(
        records=("bn_article_id", "nunique"),
        first_year=("year_num", "min"),
        last_year=("year_num", "max"),
    )
    .reset_index()
    .sort_values("records", ascending=False)
)

top_journals.to_csv(OUT_TOP_JOURNALS, index=False, encoding="utf-8-sig")


# === TEKST DO SLAJDU ===

slide_text = f"""
KORPUS ROBOCZY: METADANE + PEŁNE TEKSTY

Liczba rekordów po filtracji: {summary["records_candidate"]}
Zakres lat: {summary["first_year"]}–{summary["last_year"]}
Liczba czasopism: {summary["journals"]}
Rekordy z abstraktem/opisem: {summary["records_with_description"]}
Rekordy z adresem PDF: {summary["records_with_pdf_url"]}
Rekordy z pełnym tekstem: {summary["records_with_fulltext"]}
Pokrycie pełnotekstowe: {summary["fulltext_coverage_percent"]}%

Formuła ostrożna:
To nie jest finalna historia polskiej pamięciologii, lecz roboczy korpus demonstracyjny,
który pozwala sprawdzić, jak metadane i pełne teksty mogą służyć do badania trajektorii
pojęć, czasopism i języków badawczych.
""".strip()

OUT_TEXT.write_text(slide_text, encoding="utf-8")

print(slide_text)
print("\nZapisano:")
print(OUT_SUMMARY)
print(OUT_TOP_JOURNALS)
print(OUT_TEXT)

#%% slajd 4

# =============================================================================
# 0. ŚCIEŻKI
# =============================================================================

INPUT = Path("data/bn_pamiec_fast/pamiec_outputs/pamiec_02_memory_studies_candidate.csv")

OUT_DIR = Path("data/bn_pamiec_fast/pamiec_demo_outputs/slide_04")
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_YEAR_COUNTS = OUT_DIR / "slide_04_publications_by_year.csv"
OUT_PERIODS = OUT_DIR / "slide_04_period_summary.csv"
OUT_FIG = OUT_DIR / "slide_04_os_czasu_pamieci.png"
OUT_TEXT = OUT_DIR / "slide_04_commentary_for_slide.txt"


# =============================================================================
# 1. WCZYTANIE DANYCH
# =============================================================================

df = pd.read_csv(INPUT, dtype=str, encoding="utf-8-sig")

if "year_num" in df.columns:
    df["year"] = pd.to_numeric(df["year_num"], errors="coerce")
else:
    df["year"] = pd.to_numeric(df["year"], errors="coerce")

df = df.dropna(subset=["year"]).copy()
df["year"] = df["year"].astype(int)

# Usuwamy lata ewidentnie problematyczne, jeśli takie się pojawią.
df = df[(df["year"] >= 1900) & (df["year"] <= 2026)].copy()


# =============================================================================
# 2. PUBLIKACJE WEDŁUG ROKU
# =============================================================================

year_counts = (
    df.groupby("year")
    .size()
    .reset_index(name="records")
    .sort_values("year")
)

# Uzupełniamy brakujące lata zerami, żeby wykres nie miał luk.
all_years = pd.DataFrame({
    "year": range(year_counts["year"].min(), year_counts["year"].max() + 1)
})

year_counts = (
    all_years
    .merge(year_counts, on="year", how="left")
    .fillna({"records": 0})
)

year_counts["records"] = year_counts["records"].astype(int)

# Średnia krocząca wygładza przypadkowe skoki roczne.
year_counts["moving_avg_3y"] = (
    year_counts["records"]
    .rolling(window=3, center=True, min_periods=1)
    .mean()
)

year_counts["moving_avg_5y"] = (
    year_counts["records"]
    .rolling(window=5, center=True, min_periods=1)
    .mean()
)

# Zmiana rok do roku i zmiana względem średniej z poprzednich 5 lat.
year_counts["previous_5y_avg"] = (
    year_counts["records"]
    .shift(1)
    .rolling(window=5, min_periods=1)
    .mean()
)

year_counts["growth_vs_previous_5y"] = (
    year_counts["records"] - year_counts["previous_5y_avg"]
)

year_counts["growth_ratio_vs_previous_5y"] = (
    year_counts["records"] / year_counts["previous_5y_avg"]
)

year_counts.replace([float("inf"), -float("inf")], pd.NA, inplace=True)

year_counts.to_csv(OUT_YEAR_COUNTS, index=False, encoding="utf-8-sig")


# =============================================================================
# 3. OKRESY: DEKADY / FAZY
# =============================================================================

def assign_period(year):
    if year < 1990:
        return "przed 1990"
    elif year < 2000:
        return "1990–1999"
    elif year < 2010:
        return "2000–2009"
    elif year < 2020:
        return "2010–2019"
    else:
        return "2020–2026"


df["period"] = df["year"].apply(assign_period)

period_summary = (
    df.groupby("period")
    .agg(
        records=("bn_article_id", "nunique"),
        first_year=("year", "min"),
        last_year=("year", "max"),
    )
    .reset_index()
)

period_order = ["przed 1990", "1990–1999", "2000–2009", "2010–2019", "2020–2026"]
period_summary["period"] = pd.Categorical(
    period_summary["period"],
    categories=period_order,
    ordered=True
)

period_summary = period_summary.sort_values("period")
period_summary.to_csv(OUT_PERIODS, index=False, encoding="utf-8-sig")


# =============================================================================
# 4. LATA SZCZEGÓLNIE WYSOKIEJ WIDZIALNOŚCI
# =============================================================================

# Bierzemy lata od 1990, żeby nie wzmacniać przypadkowych efektów małej próby.
recent = year_counts[year_counts["year"] >= 1990].copy()

top_years = (
    recent.sort_values("records", ascending=False)
    .head(10)[["year", "records", "moving_avg_5y"]]
)

# Lata, w których wynik jest wyraźnie wyższy niż średnia z poprzednich 5 lat.
growth_candidates = recent[
    (recent["previous_5y_avg"] >= 3) &
    (recent["growth_ratio_vs_previous_5y"] >= 1.5)
].copy()

growth_candidates = growth_candidates.sort_values(
    "growth_ratio_vs_previous_5y",
    ascending=False
).head(10)


# =============================================================================
# 5. WYKRES DO SLAJDU
# =============================================================================

plt.figure(figsize=(13, 7))

plt.bar(
    year_counts["year"],
    year_counts["records"],
    alpha=0.45,
    label="liczba publikacji rocznie"
)

plt.plot(
    year_counts["year"],
    year_counts["moving_avg_5y"],
    linewidth=2.5,
    label="średnia krocząca 5-letnia"
)

# Zaznaczamy najważniejsze fazy, ale ostrożnie — to są przedziały robocze.
plt.axvspan(1990, 1999, alpha=0.08)
plt.axvspan(2000, 2009, alpha=0.08)
plt.axvspan(2010, 2019, alpha=0.08)
plt.axvspan(2020, 2026, alpha=0.08)

plt.title('Kiedy „pamięć” staje się kategorią centralną?', fontsize=16)
plt.xlabel("Rok")
plt.ylabel("Liczba publikacji w korpusie")
plt.grid(axis="y", alpha=0.3)
plt.legend()

plt.tight_layout()
plt.savefig(OUT_FIG, dpi=300)
plt.close()


# =============================================================================
# 6. TEKST INTERPRETACYJNY DO SLAJDU
# =============================================================================

total_records = len(df)
first_year = int(df["year"].min())
last_year = int(df["year"].max())

peak_year = int(top_years.iloc[0]["year"]) if len(top_years) else None
peak_records = int(top_years.iloc[0]["records"]) if len(top_years) else None

period_lines = []
for _, row in period_summary.iterrows():
    period_lines.append(
        f"- {row['period']}: {int(row['records'])} publikacji"
    )

top_year_lines = []
for _, row in top_years.head(5).iterrows():
    top_year_lines.append(
        f"- {int(row['year'])}: {int(row['records'])} publikacji"
    )

commentary = f"""
SLAJD 4
Kiedy „pamięć” staje się kategorią centralną?

Dane:
- liczba rekordów w korpusie kandydackim: {total_records}
- zakres lat: {first_year}–{last_year}
- rok o największej liczbie publikacji: {peak_year} ({peak_records} publikacji)

Publikacje według głównych okresów:
{chr(10).join(period_lines)}

Lata o najwyższej widzialności tematu:
{chr(10).join(top_year_lines)}

Proponowany komentarz do slajdu:

Ten wykres nie jest jeszcze interpretacją historii pamięciologii. Jest mapą widzialności tematu w korpusie. Pokazuje, w których momentach słownik pamięci staje się wyraźniej obecny w publikacjach naukowych. Dopiero kolejne warstwy — trajektorie pojęć, czasopisma, autorzy teoretyczni i pełne teksty — pozwalają zapytać, co właściwie rośnie: liczba publikacji, określony język teoretyczny, aktywność konkretnych czasopism czy przenikanie problematyki pamięci między dyscyplinami.

Najbezpieczniejsza teza:
Nie mówimy jeszcze, że wykres pokazuje pełną historię polskiej pamięciologii. Mówimy, że wskazuje okresy zwiększonej widzialności pamięci jako kategorii badawczej w wyselekcjonowanym korpusie.
""".strip()

OUT_TEXT.write_text(commentary, encoding="utf-8")


# =============================================================================
# 7. PODSUMOWANIE W KONSOLI
# =============================================================================

print("Gotowe: slajd 4")
print()
print("Zapisano:")
print("-", OUT_YEAR_COUNTS)
print("-", OUT_PERIODS)
print("-", OUT_FIG)
print("-", OUT_TEXT)
print()
print(commentary)
#%% slajd 5
# -*- coding: utf-8 -*-
"""
Slajd 5 demonstratora:
Od kategorii teoretycznej do wspólnego języka humanistyki

Cel:
- wykryć wybrane pojęcia pamięciologiczne w tytułach, abstraktach i metadanych,
- policzyć ich występowanie w czasie,
- przygotować wykres trajektorii pojęć,
- zapisać komentarz interpretacyjny do slajdu.
"""

from pathlib import Path
import re
import pandas as pd
import matplotlib.pyplot as plt


# =============================================================================
# 0. ŚCIEŻKI
# =============================================================================

INPUT = Path("data/bn_pamiec_fast/pamiec_outputs/pamiec_02_memory_studies_candidate.csv")

OUT_DIR = Path("data/bn_pamiec_fast/pamiec_demo_outputs/slide_05")
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_CONCEPT_COUNTS = OUT_DIR / "slide_05_concept_counts.csv"
OUT_TRAJECTORIES_LONG = OUT_DIR / "slide_05_concept_trajectories_long.csv"
OUT_TRAJECTORIES_WIDE = OUT_DIR / "slide_05_concept_trajectories_wide.csv"
OUT_FIG_LINES = OUT_DIR / "slide_05_trajektorie_pojec.png"
OUT_FIG_STACKED = OUT_DIR / "slide_05_trajektorie_pojec_stacked.png"
OUT_TEXT = OUT_DIR / "slide_05_commentary_for_slide.txt"


# =============================================================================
# 1. WCZYTANIE DANYCH
# =============================================================================

df = pd.read_csv(INPUT, dtype=str, encoding="utf-8-sig")

if "year_num" in df.columns:
    df["year"] = pd.to_numeric(df["year_num"], errors="coerce")
else:
    df["year"] = pd.to_numeric(df["year"], errors="coerce")

df = df.dropna(subset=["year"]).copy()
df["year"] = df["year"].astype(int)

df = df[(df["year"] >= 1900) & (df["year"] <= 2026)].copy()

for col in ["title", "description", "journal", "source", "publisher"]:
    if col not in df.columns:
        df[col] = ""

df["analysis_text"] = (
    df["title"].fillna("") + " " +
    df["description"].fillna("") + " " +
    df["journal"].fillna("") + " " +
    df["source"].fillna("") + " " +
    df["publisher"].fillna("")
).str.lower()

df["analysis_text"] = (
    df["analysis_text"]
    .str.replace(r"\s+", " ", regex=True)
    .str.replace("–", "-", regex=False)
    .str.replace("—", "-", regex=False)
    .str.strip()
)


# =============================================================================
# 2. SŁOWNIK POJĘĆ DO SLAJDU
# =============================================================================
# Uwaga:
# Słownik jest celowo interpretacyjny i kontrolowany.
# Nie chodzi o złapanie wszystkich słów, lecz o uchwycenie kilku rozpoznawalnych
# trajektorii: od terminów teoretycznych do szerszego języka humanistyki.

concept_patterns = {
    "pamięć zbiorowa": (
        r"\b(pamięć|pamięci|pamięcią|pamięciach|pamięciami)"
        r"\s+zbiorow[a-ząćęłńóśźż]*\b"
        r"|\bcollective memor(?:y|ies)\b"
    ),

    "pamięć kulturowa": (
        r"\b(pamięć|pamięci|pamięcią|pamięciach|pamięciami)"
        r"\s+kulturow[a-ząćęłńóśźż]*\b"
        r"|\bcultural memor(?:y|ies)\b"
    ),

    "pamięć historyczna": (
        r"\b(pamięć|pamięci|pamięcią|pamięciach|pamięciami)"
        r"\s+historyczn[a-ząćęłńóśźż]*\b"
        r"|\bhistorical memor(?:y|ies)\b"
    ),

    "postpamięć": (
        r"\bpostpamię[a-ząćęłńóśźż]*\b"
        r"|\bpost-memory\b"
        r"|\bpostmemory\b"
    ),

    "polityka pamięci": (
        r"\bpolityk[a-ząćęłńóśźż]*\s+"
        r"(pamięci|pamięcią|pamięciach|pamięciami)\b"
        r"|\bpolitics of memor(?:y|ies)\b"
        r"|\bmemory politics\b"
    ),

    "miejsce pamięci": (
        r"\bmiejsc[a-ząćęłńóśźż]*\s+"
        r"(pamięci|pamięcią|pamięciach|pamięciami)\b"
        r"|\bsite[s]?\s+of\s+memor(?:y|ies)\b"
        r"|\bmemory site[s]?\b"
        r"|\blieux de mémoire\b"
    ),

    "świadectwo": (
        r"\bświadectw[a-ząćęłńóśźż]*\b"
        r"|\bświadk[a-ząćęłńóśźż]*\b"
        r"|\btestimon(?:y|ies|ial|ials)\b"
        r"|\bwitness(?:es|ing)?\b"
    ),

    "trauma": (
        r"\btraum[a-ząćęłńóśźż]*\b"
        r"|\btrauma(?:s|tic|tized|tised|tization|tisation)?\b"
    ),

    "Zagłada / Holocaust": (
        r"\bzagład[a-ząćęłńóśźż]*\b"
        r"|\bholocaust\b"
        r"|\bshoah\b"
        r"|\bauschwitz\b"
    ),

    "tożsamość": (
        r"\btożsamoś[ćc][a-ząćęłńóśźż]*\b"
        r"|\bidentit(?:y|ies|arian)?\b"
    ),

    "archiwum": (
        r"\barchiw[a-ząćęłńóśźż]*\b"
        r"|\barchive[s]?\b"
        r"|\barchival\b"
        r"|\barchiv(?:e|es|al|ing)\b"
    ),

    "dziedzictwo": (
        r"\bdziedzictw[a-ząćęłńóśźż]*\b"
        r"|\bheritage[s]?\b"
    ),

    "autobiografia / wspomnienia": (
        r"\bautobiografi[a-ząćęłńóśźż]*\b"
        r"|\bwspomnieni[a-ząćęłńóśźż]*\b"
        r"|\bpamiętnik[a-ząćęłńóśźż]*\b"
        r"|\bdziennik[a-ząćęłńóśźż]*\b"
        r"|\bautobiograph(?:y|ies|ical)\b"
        r"|\bmemoir[s]?\b"
        r"|\bdiar(?:y|ies|istic)\b"
    ),

    "narracja": (
        r"\bnarracj[a-ząćęłńóśźż]*\b"
        r"|\bnarracyjn[a-ząćęłńóśźż]*\b"
        r"|\bnarrativ(?:e|es|ity|ization|isation)?\b"
        r"|\bnarration[s]?\b"
    ),

    "literatura": (
        r"\bliteratur[a-ząćęłńóśźż]*\b"
        r"|\bliterack[a-ząćęłńóśźż]*\b"
        r"|\bliteraturoznaw[a-ząćęłńóśźż]*\b"
        r"|\bliterary\b"
        r"|\bliterature[s]?\b"
        r"|\bliterary studies\b"
    ),
}


# Pojęcia, które szczególnie dobrze nadają się do wykresu na slajdzie.
# Po uruchomieniu możesz je zmienić na podstawie wyników z concept_counts.csv.
preferred_slide_concepts = [
    "pamięć zbiorowa",
    "pamięć kulturowa",
    "pamięć historyczna",
    "postpamięć",
    "polityka pamięci",
    "miejsce pamięci",
    "świadectwo",
    "trauma",
]


# =============================================================================
# 3. WYKRYWANIE POJĘĆ
# =============================================================================

for concept, pattern in concept_patterns.items():
    df[f"concept__{concept}"] = df["analysis_text"].str.contains(
        pattern,
        regex=True,
        na=False,
        case=False
    )


# =============================================================================
# 4. TABELA CZĘSTOŚCI POJĘĆ
# =============================================================================

concept_rows = []

for concept in concept_patterns:
    col = f"concept__{concept}"
    subset = df[df[col]].copy()
    years = subset["year"].dropna()

    concept_rows.append({
        "concept": concept,
        "records": len(subset),
        "share_of_corpus_percent": round(len(subset) / len(df) * 100, 2) if len(df) else 0,
        "first_year": int(years.min()) if len(years) else None,
        "last_year": int(years.max()) if len(years) else None,
        "peak_year": int(subset.groupby("year").size().idxmax()) if len(subset) else None,
        "peak_year_records": int(subset.groupby("year").size().max()) if len(subset) else 0,
    })

concept_counts = pd.DataFrame(concept_rows).sort_values(
    "records",
    ascending=False
)

concept_counts.to_csv(OUT_CONCEPT_COUNTS, index=False, encoding="utf-8-sig")


# =============================================================================
# 5. TRAJEKTORIE POJĘĆ
# =============================================================================

trajectory_rows = []

for concept in concept_patterns:
    col = f"concept__{concept}"

    temp = (
        df[df[col]]
        .groupby("year")
        .size()
        .reset_index(name="records")
    )

    temp["concept"] = concept
    trajectory_rows.append(temp)

trajectories_long = pd.concat(trajectory_rows, ignore_index=True)

trajectories_long.to_csv(
    OUT_TRAJECTORIES_LONG,
    index=False,
    encoding="utf-8-sig"
)

all_years = pd.DataFrame({
    "year": range(df["year"].min(), df["year"].max() + 1)
})

trajectories_wide = all_years.copy()

for concept in concept_patterns:
    temp = trajectories_long[trajectories_long["concept"] == concept][["year", "records"]]
    temp = temp.rename(columns={"records": concept})
    trajectories_wide = trajectories_wide.merge(temp, on="year", how="left")

for concept in concept_patterns:
    trajectories_wide[concept] = trajectories_wide[concept].fillna(0).astype(int)

# Średnia krocząca 3-letnia dla czytelniejszego slajdu.
for concept in concept_patterns:
    trajectories_wide[f"{concept}__ma3"] = (
        trajectories_wide[concept]
        .rolling(window=3, center=True, min_periods=1)
        .mean()
    )

trajectories_wide.to_csv(
    OUT_TRAJECTORIES_WIDE,
    index=False,
    encoding="utf-8-sig"
)


# =============================================================================
# 6. WYBÓR POJĘĆ DO WYKRESU
# =============================================================================

available_preferred = [
    c for c in preferred_slide_concepts
    if c in concept_counts["concept"].tolist()
]

# Bierzemy preferowane pojęcia, ale tylko te, które rzeczywiście występują.
available_preferred = [
    c for c in available_preferred
    if int(concept_counts.loc[concept_counts["concept"] == c, "records"].iloc[0]) > 0
]

# Jeśli pojęć jest zbyt mało, uzupełniamy najczęstszymi.
if len(available_preferred) < 5:
    top_extra = (
        concept_counts[~concept_counts["concept"].isin(available_preferred)]
        .head(8)["concept"]
        .tolist()
    )
    available_preferred.extend(top_extra)

slide_concepts = available_preferred[:8]


# =============================================================================
# 7. WYKRES LINIOWY: TRAJEKTORIE POJĘĆ
# =============================================================================

plt.figure(figsize=(13, 7))

for concept in slide_concepts:
    plt.plot(
        trajectories_wide["year"],
        trajectories_wide[f"{concept}__ma3"],
        linewidth=2,
        marker="o",
        markersize=3,
        label=concept
    )

plt.title("Od kategorii teoretycznej do wspólnego języka humanistyki", fontsize=15)
plt.xlabel("Rok")
plt.ylabel("Liczba publikacji, średnia krocząca 3-letnia")
plt.grid(True, alpha=0.3)
plt.legend(loc="upper left", fontsize=9)
plt.tight_layout()
plt.savefig(OUT_FIG_LINES, dpi=300)
plt.close()


# =============================================================================
# 8. WYKRES SKUMULOWANY: WIELOŚĆ JĘZYKÓW PAMIĘCI
# =============================================================================

# Dla wersji skumulowanej ograniczamy się do 6 pojęć,
# bo slajd inaczej robi się nieczytelny.
stacked_concepts = slide_concepts[:6]

plt.figure(figsize=(13, 7))

plt.stackplot(
    trajectories_wide["year"],
    [trajectories_wide[c] for c in stacked_concepts],
    labels=stacked_concepts,
    alpha=0.8
)

plt.title("Wielość języków pamięci w korpusie", fontsize=15)
plt.xlabel("Rok")
plt.ylabel("Liczba publikacji")
plt.grid(True, alpha=0.3)
plt.legend(loc="upper left", fontsize=9)
plt.tight_layout()
plt.savefig(OUT_FIG_STACKED, dpi=300)
plt.close()


# =============================================================================
# 9. KOMENTARZ DO SLAJDU
# =============================================================================

top_concepts_for_text = concept_counts.head(8)

top_lines = []
for _, row in top_concepts_for_text.iterrows():
    first = int(row["first_year"]) if pd.notna(row["first_year"]) else "brak"
    last = int(row["last_year"]) if pd.notna(row["last_year"]) else "brak"
    peak = int(row["peak_year"]) if pd.notna(row["peak_year"]) else "brak"
    top_lines.append(
        f"- {row['concept']}: {int(row['records'])} tekstów, zakres {first}–{last}, maksimum w roku {peak}"
    )

slide_concept_lines = []
for concept in slide_concepts:
    row = concept_counts[concept_counts["concept"] == concept].iloc[0]
    slide_concept_lines.append(
        f"- {concept}: {int(row['records'])} tekstów"
    )

commentary = f"""
SLAJD 5
Od kategorii teoretycznej do wspólnego języka humanistyki

Co pokazuje slajd:
Nie analizujemy już samego słowa „pamięć”, lecz rodzinę pojęć, przez które pamięć funkcjonuje w humanistyce: pamięć zbiorową, pamięć kulturową, postpamięć, politykę pamięci, miejsca pamięci, świadectwo, traumę, Zagładę, archiwum, tożsamość i literaturę.

Pojęcia pokazane na wykresie:
{chr(10).join(slide_concept_lines)}

Najczęstsze rozpoznane pojęcia w korpusie:
{chr(10).join(top_lines)}

Proponowany komentarz do slajdu:

Ten wykres pokazuje, że „pamięć” nie rozwija się jako jedno pojęcie. W korpusie widzimy raczej rodzinę języków pamięci: historyczny, kulturowy, literacki, traumatyczny, świadectwowy, archiwalny i polityczny. Niektóre pojęcia funkcjonują jako kategorie teoretyczne, inne stają się z czasem częścią szerszego języka humanistyki. Właśnie to przejście — od terminu specjalistycznego do wspólnego języka interpretacji — jest jednym z głównych obiektów demonstratora.

Bezpieczna teza:
Wykres nie dowodzi jeszcze pełnej historii polskich badań nad pamięcią. Pokazuje trajektorie pojęć w wyselekcjonowanym korpusie i wskazuje, które terminy warto następnie analizować przez czasopisma, pełne teksty, autorów teoretycznych i konteksty literaturoznawcze.
""".strip()

OUT_TEXT.write_text(commentary, encoding="utf-8")


# =============================================================================
# 10. PODSUMOWANIE W KONSOLI
# =============================================================================

print("Gotowe: slajd 5")
print()
print("Zapisano:")
print("-", OUT_CONCEPT_COUNTS)
print("-", OUT_TRAJECTORIES_LONG)
print("-", OUT_TRAJECTORIES_WIDE)
print("-", OUT_FIG_LINES)
print("-", OUT_FIG_STACKED)
print("-", OUT_TEXT)
print()
print(commentary)

#%% slajd 6

# -*- coding: utf-8 -*-
"""
Slajd 6 demonstratora:
Z czym łączy się pamięć?

Cel:
- zbudować graf współwystępowania pojęć pamięciologicznych,
- pokazać, że „pamięć” nie jest pojedynczym tematem, lecz polem sąsiedztw:
  literatura, Zagłada, trauma, świadectwo, archiwum, tożsamość, dziedzictwo,
  polityka pamięci itd.
"""

from pathlib import Path
import itertools
import pandas as pd
import matplotlib.pyplot as plt
import networkx as nx


# =============================================================================
# 0. ŚCIEŻKI
# =============================================================================

INPUT = Path("data/bn_pamiec_fast/pamiec_outputs/pamiec_02_memory_studies_candidate.csv")

OUT_DIR = Path("data/bn_pamiec_fast/pamiec_demo_outputs/slide_06")
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_NODES = OUT_DIR / "slide_06_concept_nodes.csv"
OUT_EDGES = OUT_DIR / "slide_06_concept_edges.csv"
OUT_EDGES_TOP = OUT_DIR / "slide_06_top_concept_connections.csv"
OUT_GEXF = OUT_DIR / "slide_06_concept_network.gexf"
OUT_FIG = OUT_DIR / "slide_06_z_czym_laczy_sie_pamiec.png"
OUT_TEXT = OUT_DIR / "slide_06_commentary_for_slide.txt"


# =============================================================================
# 1. WCZYTANIE DANYCH
# =============================================================================

df = pd.read_csv(INPUT, dtype=str, encoding="utf-8-sig")

if "year_num" in df.columns:
    df["year"] = pd.to_numeric(df["year_num"], errors="coerce")
else:
    df["year"] = pd.to_numeric(df["year"], errors="coerce")

df = df.dropna(subset=["year"]).copy()
df["year"] = df["year"].astype(int)

df = df[(df["year"] >= 1900) & (df["year"] <= 2026)].copy()

for col in ["title", "description", "journal", "source", "publisher"]:
    if col not in df.columns:
        df[col] = ""

df["analysis_text"] = (
    df["title"].fillna("") + " " +
    df["description"].fillna("") + " " +
    df["journal"].fillna("") + " " +
    df["source"].fillna("") + " " +
    df["publisher"].fillna("")
).str.lower()

df["analysis_text"] = (
    df["analysis_text"]
    .str.replace(r"\s+", " ", regex=True)
    .str.replace("-", "-", regex=False)
    .str.replace("–", "-", regex=False)
    .str.replace("—", "-", regex=False)
)


# =============================================================================
# 2. SŁOWNIK POJĘĆ
# =============================================================================
# Ten słownik jest celowo interpretacyjny i kontrolowany.
# Chodzi o pojęcia rozpoznawalne dla tradycyjnego odbiorcy humanistycznego.

concept_patterns = {
    # =========================================================================
    # RDZEŃ MEMORY STUDIES
    # =========================================================================

    "pamięć zbiorowa": (
        r"\b("
        r"pamięć|pamięci|pamięcią|pamięcią?|"  # podstawowe formy
        r"pamięciach|pamięciami"
        r")\s+zbiorow[a-ząćęłńóśźż]*\b"
        r"|\bcollective memor(?:y|ies)\b"
    ),

    "pamięć kulturowa": (
        r"\b("
        r"pamięć|pamięci|pamięcią|pamięciach|pamięciami"
        r")\s+kulturow[a-ząćęłńóśźż]*\b"
        r"|\bcultural memor(?:y|ies)\b"
    ),

    "pamięć historyczna": (
        r"\b("
        r"pamięć|pamięci|pamięcią|pamięciach|pamięciami"
        r")\s+historyczn[a-ząćęłńóśźż]*\b"
        r"|\bhistorical memor(?:y|ies)\b"
    ),

    "pamięć społeczna": (
        r"\b("
        r"pamięć|pamięci|pamięcią|pamięciach|pamięciami"
        r")\s+społeczn[a-ząćęłńóśźż]*\b"
        r"|\bsocial memor(?:y|ies)\b"
    ),

    "postpamięć": (
        r"\bpostpamię[a-ząćęłńóśźż]*\b"
        r"|\bpost-memory\b"
        r"|\bpostmemory\b"
    ),

    "polityka pamięci": (
        r"\bpolityk[a-ząćęłńóśźż]*\s+"
        r"(pamięci|pamięcią|pamięciach|pamięciami)\b"
        r"|\bpolitics of memor(?:y|ies)\b"
        r"|\bmemory politics\b"
    ),

    "miejsce pamięci": (
        r"\bmiejsc[a-ząćęłńóśźż]*\s+"
        r"(pamięci|pamięcią|pamięciach|pamięciami)\b"
        r"|\bsite[s]?\s+of\s+memor(?:y|ies)\b"
        r"|\bmemory site[s]?\b"
        r"|\blieux de mémoire\b"
    ),

    # =========================================================================
    # POLA TEMATYCZNE I INTERPRETACYJNE
    # =========================================================================

    "Zagłada / Holocaust": (
        r"\bzagład[a-ząćęłńóśźż]*\b"
        r"|\bholocaust\b"
        r"|\bshoah\b"
        r"|\bauschwitz\b"
    ),

    "trauma": (
        r"\btraum[a-ząćęłńóśźż]*\b"
        r"|\btrauma(?:s|tic|tized|tisation|tization)?\b"
    ),

    "świadectwo": (
        r"\bświadectw[a-ząćęłńóśźż]*\b"
        r"|\bświadk[a-ząćęłńóśźż]*\b"
        r"|\btestimon(?:y|ies|ial|ials)\b"
        r"|\bwitness(?:es|ing)?\b"
    ),

    "tożsamość": (
        r"\btożsamoś[ćc][a-ząćęłńóśźż]*\b"
        r"|\bidentit(?:y|ies|arian)?\b"
    ),

    "dziedzictwo": (
        r"\bdziedzictw[a-ząćęłńóśźż]*\b"
        r"|\bheritage[s]?\b"
    ),

    "archiwum": (
        r"\barchiw[a-ząćęłńóśźż]*\b"
        r"|\barchive[s]?\b"
        r"|\barchival\b"
        r"|\barchiv(?:e|es|al|ing)\b"
    ),

    "muzeum": (
        r"\bmuze[a-ząćęłńóśźż]*\b"
        r"|\bmuseum[s]?\b"
        r"|\bmuseal\b"
        r"|\bmuseology\b"
    ),

    "komemoracja": (
        r"\bkomemoracj[a-ząćęłńóśźż]*\b"
        r"|\bupamiętni[a-ząćęłńóśźż]*\b"
        r"|\bcommemorat(?:e|es|ed|ing|ion|ions|ive)\b"
        r"|\bmemoriali[sz](?:e|es|ed|ing|ation)\b"
    ),

    # =========================================================================
    # POLONISTYCZNE I LITERATUROZNAWCZE SĄSIEDZTWA
    # =========================================================================

    "literatura": (
        r"\bliteratur[a-ząćęłńóśźż]*\b"
        r"|\bliterack[a-ząćęłńóśźż]*\b"
        r"|\bliteraturoznaw[a-ząćęłńóśźż]*\b"
        r"|\bliterary\b"
        r"|\bliterature[s]?\b"
        r"|\bliterary studies\b"
    ),

    "narracja": (
        r"\bnarracj[a-ząćęłńóśźż]*\b"
        r"|\bnarracyjn[a-ząćęłńóśźż]*\b"
        r"|\bnarrativ(?:e|es|ity|ization|isation)?\b"
        r"|\bnarration[s]?\b"
    ),

    "autobiografia / wspomnienia": (
        r"\bautobiografi[a-ząćęłńóśźż]*\b"
        r"|\bwspomnieni[a-ząćęłńóśźż]*\b"
        r"|\bpamiętnik[a-ząćęłńóśźż]*\b"
        r"|\bdziennik[a-ząćęłńóśźż]*\b"
        r"|\bautobiograph(?:y|ies|ical)\b"
        r"|\bmemoir[s]?\b"
        r"|\bdiar(?:y|ies|istic)\b"
    ),

    "poezja": (
        r"\bpoezj[a-ząćęłńóśźż]*\b"
        r"|\bpoetyck[a-ząćęłńóśźż]*\b"
        r"|\bwiersz[a-ząćęłńóśźż]*\b"
        r"|\bpoetr(?:y|ies)\b"
        r"|\bpoem[s]?\b"
        r"|\bpoetic[s]?\b"
    ),

    "powieść": (
        r"\bpowieś[ćc][a-ząćęłńóśźż]*\b"
        r"|\bproza\b"
        r"|\bprozatorsk[a-ząćęłńóśźż]*\b"
        r"|\bnovel[s]?\b"
        r"|\bfiction\b"
        r"|\bprose\b"
    ),

    # =========================================================================
    # SZERSZE KONTEKSTY HUMANISTYKI
    # =========================================================================

    "historia": (
        r"\bhistori[a-ząćęłńóśźż]*\b"
        r"|\bhistory\b"
        r"|\bhistorical\b"
        r"|\bhistoric(?:al)?\b"
    ),

    "historiografia": (
        r"\bhistoriografi[a-ząćęłńóśźż]*\b"
        r"|\bhistoriograficzn[a-ząćęłńóśźż]*\b"
        r"|\bhistoriograph(?:y|ies|ical)\b"
    ),

    "edukacja": (
        r"\bedukacj[a-ząćęłńóśźż]*\b"
        r"|\bedukacyjn[a-ząćęłńóśźż]*\b"
        r"|\bnauczani[a-ząćęłńóśźż]*\b"
        r"|\beducat(?:ion|ional|e|ed|ing)\b"
        r"|\bteaching\b"
    ),

    "media": (
        r"\bmedia\b"
        r"|\bmedialn[a-ząćęłńóśźż]*\b"
        r"|\bmedium\b"
        r"|\bmediati(?:on|ons|zed|sed)\b"
        r"|\bdigital media\b"
    ),

    "przestrzeń / miejsce": (
        r"\bprzestrze[ńn][a-ząćęłńóśźż]*\b"
        r"|\bprzestrzenn[a-ząćęłńóśźż]*\b"
        r"|\bmiejsc[a-ząćęłńóśźż]*\b"
        r"|\btopografi[a-ząćęłńóśźż]*\b"
        r"|\bspac(?:e|es|ial)\b"
        r"|\bplace[s]?\b"
        r"|\btopograph(?:y|ies|ical)\b"
    ),
}


# =============================================================================
# 3. WYKRYWANIE POJĘĆ W REKORDACH
# =============================================================================

for concept, pattern in concept_patterns.items():
    df[f"concept__{concept}"] = df["analysis_text"].str.contains(
    pattern,
    regex=True,
    na=False,
    case=False
    )

# Lista pojęć rozpoznanych w każdym rekordzie.
def get_concepts_for_row(row):
    found = []
    for concept in concept_patterns:
        if row[f"concept__{concept}"]:
            found.append(concept)
    return sorted(set(found))

df["concepts_found"] = df.apply(get_concepts_for_row, axis=1)
df["concepts_count"] = df["concepts_found"].apply(len)


# =============================================================================
# 4. TABELA WĘZŁÓW
# =============================================================================

node_rows = []

for concept in concept_patterns:
    col = f"concept__{concept}"
    subset = df[df[col]].copy()

    if len(subset) == 0:
        continue

    years = subset["year"].dropna()

    node_rows.append({
        "id": concept,
        "label": concept,
        "records": int(len(subset)),
        "first_year": int(years.min()) if len(years) else None,
        "last_year": int(years.max()) if len(years) else None,
        "node_type": "concept",
    })

nodes = pd.DataFrame(node_rows).sort_values("records", ascending=False)

nodes.to_csv(OUT_NODES, index=False, encoding="utf-8-sig")


# =============================================================================
# 5. TABELA KRAWĘDZI: WSPÓŁWYSTĘPOWANIE POJĘĆ
# =============================================================================

edge_rows = []

for _, row in df.iterrows():
    concepts = row["concepts_found"]

    if len(concepts) < 2:
        continue

    for source, target in itertools.combinations(concepts, 2):
        edge_rows.append({
            "source": source,
            "target": target,
            "bn_article_id": row.get("bn_article_id", ""),
            "year": row.get("year", None),
            "journal": row.get("journal", ""),
            "title": row.get("title", ""),
        })

edges_raw = pd.DataFrame(edge_rows)

if len(edges_raw) > 0:
    edges = (
        edges_raw
        .groupby(["source", "target"])
        .agg(
            weight=("bn_article_id", "nunique"),
            first_year=("year", "min"),
            last_year=("year", "max"),
            example_title=("title", "first"),
        )
        .reset_index()
        .sort_values("weight", ascending=False)
    )
else:
    edges = pd.DataFrame(
        columns=["source", "target", "weight", "first_year", "last_year", "example_title"]
    )

# Minimalna waga dla wersji prezentacyjnej.
# Dla Gephi możesz użyć wszystkich krawędzi, ale na PNG lepiej odsiać najsłabsze.
MIN_EDGE_WEIGHT_FOR_EXPORT = 2

edges_export = edges[edges["weight"] >= MIN_EDGE_WEIGHT_FOR_EXPORT].copy()

edges.to_csv(OUT_EDGES, index=False, encoding="utf-8-sig")
edges.head(30).to_csv(OUT_EDGES_TOP, index=False, encoding="utf-8-sig")


# =============================================================================
# 6. BUDOWA GRAFU
# =============================================================================

G = nx.Graph()

for _, row in nodes.iterrows():
    G.add_node(
        row["id"],
        label=row["label"],
        records=int(row["records"]),
        first_year=row["first_year"],
        last_year=row["last_year"],
        node_type=row["node_type"],
    )

for _, row in edges_export.iterrows():
    G.add_edge(
        row["source"],
        row["target"],
        weight=int(row["weight"]),
        first_year=row["first_year"],
        last_year=row["last_year"],
    )

# Metryki do Gephi / dalszej analizy.
degree = dict(G.degree())
weighted_degree = dict(G.degree(weight="weight"))
betweenness = nx.betweenness_centrality(G, weight="weight") if G.number_of_edges() > 0 else {}

nx.set_node_attributes(G, degree, "degree")
nx.set_node_attributes(G, weighted_degree, "weighted_degree")
nx.set_node_attributes(G, betweenness, "betweenness")

# Eksport do Gephi.
nx.write_gexf(G, OUT_GEXF)


# =============================================================================
# 7. PROSTY WYKRES PNG DO PREZENTACJI
# =============================================================================

# Do PNG wybieramy największe węzły i najsilniejsze krawędzie,
# żeby obraz był czytelny na slajdzie.
TOP_N_NODES_FOR_PNG = 18
MIN_EDGE_WEIGHT_FOR_PNG = 3

top_nodes = set(nodes.head(TOP_N_NODES_FOR_PNG)["id"].tolist())

G_png = nx.Graph()

for node in top_nodes:
    if node in G:
        G_png.add_node(node, **G.nodes[node])

for source, target, attrs in G.edges(data=True):
    if source in top_nodes and target in top_nodes and attrs.get("weight", 0) >= MIN_EDGE_WEIGHT_FOR_PNG:
        G_png.add_edge(source, target, **attrs)

plt.figure(figsize=(14, 10))

if G_png.number_of_nodes() > 0:
    pos = nx.spring_layout(G_png, seed=42, k=0.8, weight="weight")

    node_sizes = [
        300 + G_png.nodes[n].get("records", 1) * 35
        for n in G_png.nodes()
    ]

    edge_widths = [
        0.5 + G_png[u][v].get("weight", 1) * 0.25
        for u, v in G_png.edges()
    ]

    nx.draw_networkx_edges(
        G_png,
        pos,
        width=edge_widths,
        alpha=0.35
    )

    nx.draw_networkx_nodes(
        G_png,
        pos,
        node_size=node_sizes,
        alpha=0.85
    )

    nx.draw_networkx_labels(
        G_png,
        pos,
        font_size=9
    )

plt.title("Z czym łączy się pamięć? Mapa współwystępowania pojęć", fontsize=15)
plt.axis("off")
plt.tight_layout()
plt.savefig(OUT_FIG, dpi=300)
plt.close()


# =============================================================================
# 8. KOMENTARZ INTERPRETACYJNY
# =============================================================================

total_records = len(df)
records_with_any_concept = int((df["concepts_count"] >= 1).sum())
records_with_two_concepts = int((df["concepts_count"] >= 2).sum())

top_node_lines = []
for _, row in nodes.head(10).iterrows():
    top_node_lines.append(
        f"- {row['label']}: {int(row['records'])} rekordów"
    )

top_edge_lines = []
for _, row in edges.head(10).iterrows():
    top_edge_lines.append(
        f"- {row['source']} — {row['target']}: {int(row['weight'])} współwystąpień"
    )

commentary = f"""
SLAJD 6
Z czym łączy się pamięć?

Dane:
- liczba rekordów w korpusie: {total_records}
- rekordy z przynajmniej jednym rozpoznanym pojęciem: {records_with_any_concept}
- rekordy z przynajmniej dwoma rozpoznanymi pojęciami: {records_with_two_concepts}
- liczba węzłów pojęciowych: {G.number_of_nodes()}
- liczba krawędzi współwystępowania po filtracji wagowej: {G.number_of_edges()}

Najczęstsze pojęcia:
{chr(10).join(top_node_lines)}

Najsilniejsze połączenia pojęć:
{chr(10).join(top_edge_lines)}

Proponowany komentarz do slajdu:

Ten widok pokazuje, że „pamięć” nie funkcjonuje w korpusie jako samotne hasło, lecz jako węzeł wielu sąsiedztw pojęciowych. Łączy się z literaturą, historią, świadectwem, traumą, Zagładą, archiwum, tożsamością, dziedzictwem i polityką pamięci. Mapa nie jest jeszcze interpretacją gotowych nurtów. Pokazuje strukturę współwystępowania, którą badacz może następnie nazwać i zinterpretować.

Bezpieczna teza:
Algorytm wskazuje sąsiedztwa pojęć, ale nie nadaje im samodzielnie sensu. Nazwy klastrów — na przykład klaster świadectwa i Zagłady, klaster literacko-narracyjny albo klaster archiwalno-dziedzictwowy — są już pracą interpretacyjną badacza.

Sugestia do prezentacji:
Na slajdzie pokaż PNG jako mapę orientacyjną, a wersję GEXF zachowaj do Gephi, jeśli chcesz pokazać demonstrator interaktywnie.
""".strip()

OUT_TEXT.write_text(commentary, encoding="utf-8")


# =============================================================================
# 9. PODSUMOWANIE W KONSOLI
# =============================================================================

print("Gotowe: slajd 6")
print()
print("Zapisano:")
print("-", OUT_NODES)
print("-", OUT_EDGES)
print("-", OUT_EDGES_TOP)
print("-", OUT_GEXF)
print("-", OUT_FIG)
print("-", OUT_TEXT)
print()
print(commentary)

#%% slajd 7

# -*- coding: utf-8 -*-
"""
Slajd 7 demonstratora:
Które czasopisma stabilizują język pamięci?

Cel:
- pokazać czasopisma jako instytucje stabilizujące obieg pojęć,
- policzyć, które czasopisma najczęściej publikują teksty z korpusu pamięci,
- sprawdzić, jakie pojęcia są powiązane z poszczególnymi czasopismami,
- wskazać czasopisma-mosty, które łączą wiele kategorii pamięci.
"""

from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import networkx as nx


# =============================================================================
# 0. ŚCIEŻKI
# =============================================================================

INPUT = Path("data/bn_pamiec_fast/pamiec_outputs/pamiec_02_memory_studies_candidate.csv")

OUT_DIR = Path("data/bn_pamiec_fast/pamiec_demo_outputs/slide_07")
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_JOURNAL_RANKING = OUT_DIR / "slide_07_journal_ranking.csv"
OUT_JOURNAL_CONCEPT_EDGES = OUT_DIR / "slide_07_journal_concept_edges.csv"
OUT_JOURNAL_CONCEPT_TOP = OUT_DIR / "slide_07_journal_dominant_concepts.csv"
OUT_BRIDGE_JOURNALS = OUT_DIR / "slide_07_bridge_journals.csv"
OUT_GEXF = OUT_DIR / "slide_07_journal_concept_network.gexf"
OUT_FIG_TOP_JOURNALS = OUT_DIR / "slide_07_top_journals.png"
OUT_FIG_BRIDGE_JOURNALS = OUT_DIR / "slide_07_bridge_journals.png"
OUT_TEXT = OUT_DIR / "slide_07_commentary_for_slide.txt"


# =============================================================================
# 1. WCZYTANIE DANYCH
# =============================================================================

df = pd.read_csv(INPUT, dtype=str, encoding="utf-8-sig")

if "year_num" in df.columns:
    df["year"] = pd.to_numeric(df["year_num"], errors="coerce")
else:
    df["year"] = pd.to_numeric(df["year"], errors="coerce")

df = df.dropna(subset=["year"]).copy()
df["year"] = df["year"].astype(int)

df = df[(df["year"] >= 1900) & (df["year"] <= 2026)].copy()

for col in ["title", "description", "journal", "source", "publisher"]:
    if col not in df.columns:
        df[col] = ""

# Jeżeli kolumna journal jest pusta, próbujemy wydobyć czasopismo z source.
df["journal_clean"] = df["journal"].fillna("").str.strip()

if df["journal_clean"].eq("").all() and "source" in df.columns:
    df["journal_clean"] = (
        df["source"]
        .fillna("")
        .str.split(";")
        .str[0]
        .str.strip()
    )

df.loc[df["journal_clean"] == "", "journal_clean"] = "brak danych"

df["analysis_text"] = (
    df["title"].fillna("") + " " +
    df["description"].fillna("") + " " +
    df["journal_clean"].fillna("") + " " +
    df["source"].fillna("") + " " +
    df["publisher"].fillna("")
).str.lower()

df["analysis_text"] = (
    df["analysis_text"]
    .str.replace(r"\s+", " ", regex=True)
    .str.replace("–", "-", regex=False)
    .str.replace("—", "-", regex=False)
    .str.strip()
)


# =============================================================================
# 2. SŁOWNIK POJĘĆ — WERSJA Z FLEKSJĄ
# =============================================================================

concept_patterns = {
    "pamięć zbiorowa": (
        r"\b(pamięć|pamięci|pamięcią|pamięciach|pamięciami)"
        r"\s+zbiorow[a-ząćęłńóśźż]*\b"
        r"|\bcollective memor(?:y|ies)\b"
    ),

    "pamięć kulturowa": (
        r"\b(pamięć|pamięci|pamięcią|pamięciach|pamięciami)"
        r"\s+kulturow[a-ząćęłńóśźż]*\b"
        r"|\bcultural memor(?:y|ies)\b"
    ),

    "pamięć historyczna": (
        r"\b(pamięć|pamięci|pamięcią|pamięciach|pamięciami)"
        r"\s+historyczn[a-ząćęłńóśźż]*\b"
        r"|\bhistorical memor(?:y|ies)\b"
    ),

    "pamięć społeczna": (
        r"\b(pamięć|pamięci|pamięcią|pamięciach|pamięciami)"
        r"\s+społeczn[a-ząćęłńóśźż]*\b"
        r"|\bsocial memor(?:y|ies)\b"
    ),

    "postpamięć": (
        r"\bpostpamię[a-ząćęłńóśźż]*\b"
        r"|\bpost-memory\b"
        r"|\bpostmemory\b"
    ),

    "polityka pamięci": (
        r"\bpolityk[a-ząćęłńóśźż]*\s+"
        r"(pamięci|pamięcią|pamięciach|pamięciami)\b"
        r"|\bpolitics of memor(?:y|ies)\b"
        r"|\bmemory politics\b"
    ),

    "miejsce pamięci": (
        r"\bmiejsc[a-ząćęłńóśźż]*\s+"
        r"(pamięci|pamięcią|pamięciach|pamięciami)\b"
        r"|\bsite[s]?\s+of\s+memor(?:y|ies)\b"
        r"|\bmemory site[s]?\b"
        r"|\blieux de mémoire\b"
    ),

    "Zagłada / Holocaust": (
        r"\bzagład[a-ząćęłńóśźż]*\b"
        r"|\bholocaust\b"
        r"|\bshoah\b"
        r"|\bauschwitz\b"
    ),

    "trauma": (
        r"\btraum[a-ząćęłńóśźż]*\b"
        r"|\btrauma(?:s|tic|tized|tised|tization|tisation)?\b"
    ),

    "świadectwo": (
        r"\bświadectw[a-ząćęłńóśźż]*\b"
        r"|\bświadk[a-ząćęłńóśźż]*\b"
        r"|\btestimon(?:y|ies|ial|ials)\b"
        r"|\bwitness(?:es|ing)?\b"
    ),

    "tożsamość": (
        r"\btożsamoś[ćc][a-ząćęłńóśźż]*\b"
        r"|\bidentit(?:y|ies|arian)?\b"
    ),

    "dziedzictwo": (
        r"\bdziedzictw[a-ząćęłńóśźż]*\b"
        r"|\bheritage[s]?\b"
    ),

    "archiwum": (
        r"\barchiw[a-ząćęłńóśźż]*\b"
        r"|\barchive[s]?\b"
        r"|\barchival\b"
        r"|\barchiv(?:e|es|al|ing)\b"
    ),

    "muzeum": (
        r"\bmuze[a-ząćęłńóśźż]*\b"
        r"|\bmuseum[s]?\b"
        r"|\bmuseal\b"
        r"|\bmuseology\b"
    ),

    "komemoracja": (
        r"\bkomemoracj[a-ząćęłńóśźż]*\b"
        r"|\bupamiętni[a-ząćęłńóśźż]*\b"
        r"|\bcommemorat(?:e|es|ed|ing|ion|ions|ive)\b"
        r"|\bmemoriali[sz](?:e|es|ed|ing|ation)\b"
    ),

    "literatura": (
        r"\bliteratur[a-ząćęłńóśźż]*\b"
        r"|\bliterack[a-ząćęłńóśźż]*\b"
        r"|\bliteraturoznaw[a-ząćęłńóśźż]*\b"
        r"|\bliterary\b"
        r"|\bliterature[s]?\b"
        r"|\bliterary studies\b"
    ),

    "narracja": (
        r"\bnarracj[a-ząćęłńóśźż]*\b"
        r"|\bnarracyjn[a-ząćęłńóśźż]*\b"
        r"|\bnarrativ(?:e|es|ity|ization|isation)?\b"
        r"|\bnarration[s]?\b"
    ),

    "autobiografia / wspomnienia": (
        r"\bautobiografi[a-ząćęłńóśźż]*\b"
        r"|\bwspomnieni[a-ząćęłńóśźż]*\b"
        r"|\bpamiętnik[a-ząćęłńóśźż]*\b"
        r"|\bdziennik[a-ząćęłńóśźż]*\b"
        r"|\bautobiograph(?:y|ies|ical)\b"
        r"|\bmemoir[s]?\b"
        r"|\bdiar(?:y|ies|istic)\b"
    ),

    "poezja": (
        r"\bpoezj[a-ząćęłńóśźż]*\b"
        r"|\bpoetyck[a-ząćęłńóśźż]*\b"
        r"|\bwiersz[a-ząćęłńóśźż]*\b"
        r"|\bpoetr(?:y|ies)\b"
        r"|\bpoem[s]?\b"
        r"|\bpoetic[s]?\b"
    ),

    "powieść": (
        r"\bpowieś[ćc][a-ząćęłńóśźż]*\b"
        r"|\bproza\b"
        r"|\bprozatorsk[a-ząćęłńóśźż]*\b"
        r"|\bnovel[s]?\b"
        r"|\bfiction\b"
        r"|\bprose\b"
    ),

    "historia": (
        r"\bhistori[a-ząćęłńóśźż]*\b"
        r"|\bhistory\b"
        r"|\bhistorical\b"
        r"|\bhistoric(?:al)?\b"
    ),

    "historiografia": (
        r"\bhistoriografi[a-ząćęłńóśźż]*\b"
        r"|\bhistoriograficzn[a-ząćęłńóśźż]*\b"
        r"|\bhistoriograph(?:y|ies|ical)\b"
    ),

    "edukacja": (
        r"\bedukacj[a-ząćęłńóśźż]*\b"
        r"|\bedukacyjn[a-ząćęłńóśźż]*\b"
        r"|\bnauczani[a-ząćęłńóśźż]*\b"
        r"|\beducat(?:ion|ional|e|ed|ing)\b"
        r"|\bteaching\b"
    ),

    "media": (
        r"\bmedia\b"
        r"|\bmedialn[a-ząćęłńóśźż]*\b"
        r"|\bmedium\b"
        r"|\bmediati(?:on|ons|zed|sed)\b"
        r"|\bdigital media\b"
    ),

    "przestrzeń / miejsce": (
        r"\bprzestrze[ńn][a-ząćęłńóśźż]*\b"
        r"|\bprzestrzenn[a-ząćęłńóśźż]*\b"
        r"|\bmiejsc[a-ząćęłńóśźż]*\b"
        r"|\btopografi[a-ząćęłńóśźż]*\b"
        r"|\bspac(?:e|es|ial)\b"
        r"|\bplace[s]?\b"
        r"|\btopograph(?:y|ies|ical)\b"
    ),
}


# =============================================================================
# 3. WYKRYWANIE POJĘĆ
# =============================================================================

for concept, pattern in concept_patterns.items():
    df[f"concept__{concept}"] = df["analysis_text"].str.contains(
        pattern,
        regex=True,
        na=False,
        case=False
    )


def concepts_in_row(row):
    found = []
    for concept in concept_patterns:
        if row[f"concept__{concept}"]:
            found.append(concept)
    return sorted(set(found))


df["concepts_found"] = df.apply(concepts_in_row, axis=1)
df["concepts_count"] = df["concepts_found"].apply(len)


# =============================================================================
# 4. RANKING CZASOPISM W KORPUSIE
# =============================================================================

journal_ranking = (
    df.groupby("journal_clean")
    .agg(
        records=("bn_article_id", "nunique"),
        first_year=("year", "min"),
        last_year=("year", "max"),
        concepts_total=("concepts_count", "sum"),
        concepts_distinct=("concepts_found", lambda rows: len(set(sum(rows, [])))),
    )
    .reset_index()
    .sort_values(["records", "concepts_distinct"], ascending=False)
)

journal_ranking["first_year"] = journal_ranking["first_year"].astype("Int64")
journal_ranking["last_year"] = journal_ranking["last_year"].astype("Int64")

journal_ranking.to_csv(
    OUT_JOURNAL_RANKING,
    index=False,
    encoding="utf-8-sig"
)


# =============================================================================
# 5. TABELA CZASOPISMO–POJĘCIE
# =============================================================================

edge_rows = []

for _, row in df.iterrows():
    journal = row["journal_clean"]
    article_id = row.get("bn_article_id", "")
    year = row.get("year", None)

    for concept in row["concepts_found"]:
        edge_rows.append({
            "journal": journal,
            "concept": concept,
            "bn_article_id": article_id,
            "year": year,
            "title": row.get("title", ""),
        })

journal_concept_raw = pd.DataFrame(edge_rows)

if len(journal_concept_raw) > 0:
    journal_concept_edges = (
        journal_concept_raw
        .groupby(["journal", "concept"])
        .agg(
            weight=("bn_article_id", "nunique"),
            first_year=("year", "min"),
            last_year=("year", "max"),
            example_title=("title", "first"),
        )
        .reset_index()
        .sort_values(["weight", "journal"], ascending=[False, True])
    )
else:
    journal_concept_edges = pd.DataFrame(
        columns=["journal", "concept", "weight", "first_year", "last_year", "example_title"]
    )

journal_concept_edges.to_csv(
    OUT_JOURNAL_CONCEPT_EDGES,
    index=False,
    encoding="utf-8-sig"
)


# =============================================================================
# 6. DOMINUJĄCE POJĘCIA DLA KAŻDEGO CZASOPISMA
# =============================================================================

dominant_rows = []

for journal, group in journal_concept_edges.groupby("journal"):
    group_sorted = group.sort_values("weight", ascending=False)
    top_concepts = group_sorted.head(5)

    dominant_rows.append({
        "journal": journal,
        "journal_records": int(
            journal_ranking.loc[
                journal_ranking["journal_clean"] == journal,
                "records"
            ].iloc[0]
        ),
        "distinct_concepts": int(
            journal_ranking.loc[
                journal_ranking["journal_clean"] == journal,
                "concepts_distinct"
            ].iloc[0]
        ),
        "top_concepts": "; ".join(
            f"{row['concept']} ({int(row['weight'])})"
            for _, row in top_concepts.iterrows()
        )
    })

journal_dominant_concepts = (
    pd.DataFrame(dominant_rows)
    .sort_values(["journal_records", "distinct_concepts"], ascending=False)
)

journal_dominant_concepts.to_csv(
    OUT_JOURNAL_CONCEPT_TOP,
    index=False,
    encoding="utf-8-sig"
)


# =============================================================================
# 7. CZASOPISMA-MOSTY
# =============================================================================
# Prosta definicja robocza:
# czasopismo-most = ma stosunkowo dużo tekstów i łączy wiele różnych pojęć.
# bridge_score = liczba rekordów * liczba rozpoznanych różnych pojęć.

bridge_journals = journal_ranking.copy()
bridge_journals["bridge_score"] = (
    bridge_journals["records"] * bridge_journals["concepts_distinct"]
)

# Usuwamy „brak danych”, jeśli występuje.
bridge_journals = bridge_journals[
    bridge_journals["journal_clean"] != "brak danych"
].copy()

bridge_journals = bridge_journals.sort_values(
    ["bridge_score", "records", "concepts_distinct"],
    ascending=False
)

bridge_journals.to_csv(
    OUT_BRIDGE_JOURNALS,
    index=False,
    encoding="utf-8-sig"
)


# =============================================================================
# 8. GRAF DWUDZIELNY CZASOPISMO–POJĘCIE DO GEPHI
# =============================================================================

# Dla Gephi nie warto brać wszystkiego. Bierzemy:
# - top czasopisma według liczby rekordów,
# - relacje czasopismo–pojęcie o minimalnej wadze.

TOP_JOURNALS_FOR_GEPHI = 40
MIN_EDGE_WEIGHT_FOR_GEPHI = 2

top_journals_set = set(
    journal_ranking
    .query("journal_clean != 'brak danych'")
    .head(TOP_JOURNALS_FOR_GEPHI)["journal_clean"]
    .tolist()
)

jc_gephi = journal_concept_edges[
    (journal_concept_edges["journal"].isin(top_journals_set)) &
    (journal_concept_edges["weight"] >= MIN_EDGE_WEIGHT_FOR_GEPHI)
].copy()

G = nx.Graph()

# Węzły czasopism.
for _, row in journal_ranking.iterrows():
    journal = row["journal_clean"]

    if journal not in top_journals_set:
        continue

    G.add_node(
        journal,
        label=journal,
        node_type="journal",
        records=int(row["records"]),
        concepts_distinct=int(row["concepts_distinct"]),
        bridge_score=int(row["records"] * row["concepts_distinct"]),
        first_year=int(row["first_year"]) if pd.notna(row["first_year"]) else None,
        last_year=int(row["last_year"]) if pd.notna(row["last_year"]) else None,
    )

# Węzły pojęć.
used_concepts = sorted(jc_gephi["concept"].unique().tolist())

for concept in used_concepts:
    concept_records = int(
        journal_concept_edges[
            journal_concept_edges["concept"] == concept
        ]["weight"].sum()
    )

    G.add_node(
        concept,
        label=concept,
        node_type="concept",
        records=concept_records,
    )

# Krawędzie.
for _, row in jc_gephi.iterrows():
    G.add_edge(
        row["journal"],
        row["concept"],
        weight=int(row["weight"]),
        first_year=int(row["first_year"]) if pd.notna(row["first_year"]) else None,
        last_year=int(row["last_year"]) if pd.notna(row["last_year"]) else None,
    )

# Metryki pomocnicze.
degree = dict(G.degree())
weighted_degree = dict(G.degree(weight="weight"))
nx.set_node_attributes(G, degree, "degree")
nx.set_node_attributes(G, weighted_degree, "weighted_degree")

nx.write_gexf(G, OUT_GEXF)


# =============================================================================
# 9. WYKRES TOP CZASOPISM
# =============================================================================

TOP_N = 15

top_for_plot = (
    journal_ranking[journal_ranking["journal_clean"] != "brak danych"]
    .head(TOP_N)
    .sort_values("records", ascending=True)
)

plt.figure(figsize=(11, 8))
plt.barh(
    top_for_plot["journal_clean"],
    top_for_plot["records"]
)

plt.title("Czasopisma najczęściej obecne w korpusie pamięci", fontsize=14)
plt.xlabel("Liczba tekstów w korpusie")
plt.ylabel("Czasopismo")
plt.tight_layout()
plt.savefig(OUT_FIG_TOP_JOURNALS, dpi=300)
plt.close()


# =============================================================================
# 10. WYKRES CZASOPISM-MOSTÓW
# =============================================================================

bridge_for_plot = bridge_journals.head(TOP_N).sort_values(
    "bridge_score",
    ascending=True
)

plt.figure(figsize=(11, 8))
plt.barh(
    bridge_for_plot["journal_clean"],
    bridge_for_plot["bridge_score"]
)

plt.title("Czasopisma-mosty: liczba tekstów × różnorodność pojęć", fontsize=14)
plt.xlabel("Wynik mostu: records × distinct concepts")
plt.ylabel("Czasopismo")
plt.tight_layout()
plt.savefig(OUT_FIG_BRIDGE_JOURNALS, dpi=300)
plt.close()


# =============================================================================
# 11. KOMENTARZ DO SLAJDU
# =============================================================================

top_journal_lines = []
for _, row in journal_ranking[journal_ranking["journal_clean"] != "brak danych"].head(10).iterrows():
    top_journal_lines.append(
        f"- {row['journal_clean']}: {int(row['records'])} tekstów, "
        f"{int(row['concepts_distinct'])} różnych pojęć"
    )

bridge_lines = []
for _, row in bridge_journals.head(10).iterrows():
    bridge_lines.append(
        f"- {row['journal_clean']}: bridge_score={int(row['bridge_score'])}, "
        f"teksty={int(row['records'])}, pojęcia={int(row['concepts_distinct'])}"
    )

dominant_lines = []
for _, row in journal_dominant_concepts.head(10).iterrows():
    dominant_lines.append(
        f"- {row['journal']}: {row['top_concepts']}"
    )

commentary = f"""
SLAJD 7
Które czasopisma stabilizują język pamięci?

Co pokazuje slajd:
Ten slajd przesuwa uwagę z samych pojęć na instytucje ich obiegu. Czasopisma nie są tu traktowane jako neutralne kontenery publikacji, lecz jako miejsca, w których określone języki pamięci stają się widoczne, powtarzalne i rozpoznawalne.

Najczęstsze czasopisma w korpusie:
{chr(10).join(top_journal_lines)}

Czasopisma-mosty:
{chr(10).join(bridge_lines)}

Dominujące pojęcia w wybranych czasopismach:
{chr(10).join(dominant_lines)}

Proponowany komentarz do slajdu:

Ten widok pokazuje, że język pamięci stabilizuje się nie tylko przez pojedyncze teksty i autorów, ale także przez czasopisma. Czasopismo działa jako instytucja widzialności: powtarza pewne pojęcia, łączy je z określonymi problemami i tworzy środowisko, w którym kategorie takie jak pamięć kulturowa, postpamięć, świadectwo, Zagłada, archiwum czy tożsamość stają się częścią wspólnego języka humanistyki.

Bezpieczna teza:
Ranking nie mówi, które czasopismo jest „najważniejsze” w sensie jakościowym. Pokazuje, które czasopisma są najbardziej widoczne w tym korpusie oraz które łączą największą liczbę rozpoznanych pojęć pamięciologicznych.

Sugestia do prezentacji:
Na slajdzie użyj wykresu TOP czasopism albo tabeli dominujących pojęć. Graf GEXF warto pokazać w Gephi jako mapę czasopismo–pojęcie, zwłaszcza jeśli chcesz pokazać czasopisma-mosty między literaturą, historią, świadectwem, archiwum i polityką pamięci.
""".strip()

OUT_TEXT.write_text(commentary, encoding="utf-8")


# =============================================================================
# 12. PODSUMOWANIE W KONSOLI
# =============================================================================

print("Gotowe: slajd 7")
print()
print("Zapisano:")
print("-", OUT_JOURNAL_RANKING)
print("-", OUT_JOURNAL_CONCEPT_EDGES)
print("-", OUT_JOURNAL_CONCEPT_TOP)
print("-", OUT_BRIDGE_JOURNALS)
print("-", OUT_GEXF)
print("-", OUT_FIG_TOP_JOURNALS)
print("-", OUT_FIG_BRIDGE_JOURNALS)
print("-", OUT_TEXT)
print()
print(commentary)

#%% slajd 8

# -*- coding: utf-8 -*-
"""
Slajd 8 demonstratora:
Kogo czyta polska pamięciologia?

Cel:
- wykorzystać pełne teksty do rozpoznania kanonu teoretycznego memory studies,
- policzyć, którzy autorzy teoretyczni są najczęściej przywoływani,
- pokazać ich trajektorie w czasie,
- sprawdzić, z jakimi pojęciami i czasopismami są związani.
"""

from pathlib import Path
import re
import pandas as pd
import matplotlib.pyplot as plt
import networkx as nx
from tqdm import tqdm


# =============================================================================
# 0. ŚCIEŻKI
# =============================================================================

INPUT = Path("data/bn_pamiec_fast/pamiec_fulltext_outputs/pamiec_02_candidate_fulltext_clean.csv")

OUT_DIR = Path("data/bn_pamiec_fast/pamiec_demo_outputs/slide_08")
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_AUTHOR_COUNTS = OUT_DIR / "slide_08_theoretical_author_counts.csv"
OUT_AUTHOR_TRAJECTORIES_LONG = OUT_DIR / "slide_08_author_trajectories_long.csv"
OUT_AUTHOR_TRAJECTORIES_WIDE = OUT_DIR / "slide_08_author_trajectories_wide.csv"
OUT_AUTHOR_CONCEPT_EDGES = OUT_DIR / "slide_08_author_concept_edges.csv"
OUT_AUTHOR_JOURNAL_EDGES = OUT_DIR / "slide_08_author_journal_edges.csv"
OUT_AUTHOR_EXAMPLES = OUT_DIR / "slide_08_author_examples.csv"
OUT_GEXF = OUT_DIR / "slide_08_author_concept_network.gexf"
OUT_FIG_COUNTS = OUT_DIR / "slide_08_kogo_czyta_pamieciologia.png"
OUT_FIG_TRAJECTORIES = OUT_DIR / "slide_08_trajektorie_autorow_teoretycznych.png"
OUT_TEXT = OUT_DIR / "slide_08_commentary_for_slide.txt"


# =============================================================================
# 1. WCZYTANIE DANYCH
# =============================================================================

df = pd.read_csv(INPUT, dtype=str, encoding="utf-8-sig", low_memory=False)

if "year_num" in df.columns:
    df["year"] = pd.to_numeric(df["year_num"], errors="coerce")
else:
    df["year"] = pd.to_numeric(df["year"], errors="coerce")

df = df.dropna(subset=["year"]).copy()
df["year"] = df["year"].astype(int)
df = df[(df["year"] >= 1900) & (df["year"] <= 2026)].copy()

for col in ["title", "description", "journal", "source", "publisher", "creators"]:
    if col not in df.columns:
        df[col] = ""

df["journal_clean"] = df["journal"].fillna("").str.strip()

if df["journal_clean"].eq("").all() and "source" in df.columns:
    df["journal_clean"] = (
        df["source"]
        .fillna("")
        .str.split(";")
        .str[0]
        .str.strip()
    )

df.loc[df["journal_clean"] == "", "journal_clean"] = "brak danych"


# =============================================================================
# 2. WYBÓR KOLUMNY PEŁNOTEKSTOWEJ
# =============================================================================

TEXT_COLUMN_CANDIDATES = [
    "fulltext_clean",
    "fulltext",
    "full_text",
    "text",
    "plain_text",
    "content",
    "body",
    "article_text",
    "jats_text",
    "pdf_text",
]

available_text_columns = [
    col for col in TEXT_COLUMN_CANDIDATES
    if col in df.columns
]

if not available_text_columns:
    raise ValueError(
        "Nie znalazłem kolumny z pełnym tekstem. "
        "Dostępne kolumny to:\n"
        f"{df.columns.tolist()}\n\n"
        "Dopisz właściwą nazwę kolumny do TEXT_COLUMN_CANDIDATES."
    )

TEXT_COLUMN = available_text_columns[0]

print(f"Używam kolumny pełnotekstowej: {TEXT_COLUMN}")


# =============================================================================
# 3. TEKST DO ANALIZY
# =============================================================================
# Do wykrywania autorów bierzemy pełny tekst + opis + tytuł.
# Pełny tekst jest najważniejszy, ale metadane pomagają przy krótszych rekordach.

df["analysis_text"] = (
    df["title"].fillna("") + " " +
    df["description"].fillna("") + " " +
    df[TEXT_COLUMN].fillna("")
).str.lower()

df["analysis_text"] = (
    df["analysis_text"]
    .str.replace(r"\s+", " ", regex=True)
    .str.replace("–", "-", regex=False)
    .str.replace("—", "-", regex=False)
    .str.strip()
)

df["fulltext_words"] = df[TEXT_COLUMN].fillna("").str.split().str.len()
df["has_real_fulltext"] = df["fulltext_words"] >= 300


# =============================================================================
# 4. SŁOWNIK AUTORÓW TEORETYCZNYCH
# =============================================================================
# Uwaga:
# Część nazwisk, np. Nora, może generować szum, jeśli użyjemy samego nazwiska.
# Dlatego w najbezpieczniejszych przypadkach preferujemy pełne imię i nazwisko,
# a nazwisko zostawiamy tylko tam, gdzie jest silnie rozpoznawalne w kontekście.

author_patterns = {
    "Maurice Halbwachs": (
        r"\bmaurice\s+halbwachs\b"
        r"|\bhalbwachs(?:a|em|owi|ie|owsk[a-ząćęłńóśźż]*)?\b"
    ),

    "Jan Assmann": (
        r"\bjan\s+assmann\b"
        r"|\bassmann(?:a|em|owi|ie)?\b"
    ),

    "Aleida Assmann": (
        r"\baleida\s+assmann\b"
        r"|\baleidy\s+assmann\b"
        r"|\baleidą\s+assmann\b"
    ),

    "Pierre Nora": (
        r"\bpierre\s+nora\b"
        r"|\bpierre'a\s+nory\b"
        r"|\bpierre'a\s+nora\b"
        r"|\bnora,\s*pierre\b"
        r"|\bnory\s+lieux\b"
        r"|\blieux\s+de\s+mémoire\b"
    ),

    "Marianne Hirsch": (
        r"\bmarianne\s+hirsch\b"
        r"|\bhirsch(?:a|em|owi|ie)?\b"
        r"|\bpostmemory\b"
        r"|\bpost-memory\b"
    ),

    "Paul Ricoeur": (
        r"\bpaul\s+ricoeur\b"
        r"|\bricoeur(?:a|em|owi|ze)?\b"
    ),

    "Astrid Erll": (
        r"\bastrid\s+erll\b"
        r"|\berll(?:a|em|owi|u)?\b"
    ),

    "Michael Rothberg": (
        r"\bmichael\s+rothberg\b"
        r"|\brothberg(?:a|em|owi|u)?\b"
        r"|\bmultidirectional memory\b"
    ),

    "Dominick LaCapra": (
        r"\bdominick\s+lacapra\b"
        r"|\bla\s*capra\b"
        r"|\blacapra(?:y|ą|ze)?\b"
    ),

    "Andreas Huyssen": (
        r"\bandreas\s+huyssen\b"
        r"|\bhuyssen(?:a|em|owi|ie)?\b"
    ),

    "Jeffrey K. Olick": (
        r"\bjeffrey\s+k\.?\s+olick\b"
        r"|\bjeffrey\s+olick\b"
        r"|\bolick(?:a|iem|owi|u)?\b"
    ),

    "James E. Young": (
        r"\bjames\s+e\.?\s+young\b"
        r"|\bjames\s+young\b"
        r"|\byoung(?:a|iem|owi|u)?\b"
    ),

    "Aleksander / Alexander Etkind": (
        r"\baleksander\s+etkind\b"
        r"|\balexander\s+etkind\b"
        r"|\betkind(?:a|em|owi|zie)?\b"
    ),

    "Svetlana Boym": (
        r"\bsvetlana\s+boym\b"
        r"|\bświetlana\s+boym\b"
        r"|\bboym(?:a|em|owi|ie)?\b"
    ),

    "Jay Winter": (
        r"\bjay\s+winter\b"
        r"|\bwinter(?:a|em|owi|ze)?\b"
    ),

    "Aleksandra Ubertowska": (
        r"\baleksandra\s+ubertowska\b"
        r"|\bubertowsk[a-ząćęłńóśźż]*\b"
    ),

    "Bożena Shallcross": (
        r"\bbożena\s+shallcross\b"
        r"|\bbozena\s+shallcross\b"
        r"|\bshallcross\b"
    ),

    "Przemysław Czapliński": (
        r"\bprzemysław\s+czapliński\b"
        r"|\bprzemyslaw\s+czaplinski\b"
        r"|\bczaplińsk[a-ząćęłńóśźż]*\b"
        r"|\bczaplinsk[a-z]*\b"
    ),

    "Ryszard Nycz": (
        r"\bryszard\s+nycz\b"
        r"|\bnycz(?:a|em|owi|u)?\b"
    ),

    "Ewa Domańska": (
        r"\bewa\s+domańska\b"
        r"|\bewa\s+domanska\b"
        r"|\bdomańsk[a-ząćęłńóśźż]*\b"
        r"|\bdomansk[a-z]*\b"
    ),

    "Maria Janion": (
        r"\bmaria\s+janion\b"
        r"|\bjanion\b"
    ),
}


# =============================================================================
# 5. POJĘCIA DO POWIĄZANIA Z AUTORAMI
# =============================================================================

concept_patterns = {
    "pamięć zbiorowa": (
        r"\b(pamięć|pamięci|pamięcią|pamięciach|pamięciami)"
        r"\s+zbiorow[a-ząćęłńóśźż]*\b"
        r"|\bcollective memor(?:y|ies)\b"
    ),

    "pamięć kulturowa": (
        r"\b(pamięć|pamięci|pamięcią|pamięciach|pamięciami)"
        r"\s+kulturow[a-ząćęłńóśźż]*\b"
        r"|\bcultural memor(?:y|ies)\b"
    ),

    "pamięć historyczna": (
        r"\b(pamięć|pamięci|pamięcią|pamięciach|pamięciami)"
        r"\s+historyczn[a-ząćęłńóśźż]*\b"
        r"|\bhistorical memor(?:y|ies)\b"
    ),

    "postpamięć": (
        r"\bpostpamię[a-ząćęłńóśźż]*\b"
        r"|\bpost-memory\b"
        r"|\bpostmemory\b"
    ),

    "polityka pamięci": (
        r"\bpolityk[a-ząćęłńóśźż]*\s+"
        r"(pamięci|pamięcią|pamięciach|pamięciami)\b"
        r"|\bpolitics of memor(?:y|ies)\b"
        r"|\bmemory politics\b"
    ),

    "miejsce pamięci": (
        r"\bmiejsc[a-ząćęłńóśźż]*\s+"
        r"(pamięci|pamięcią|pamięciach|pamięciami)\b"
        r"|\bsite[s]?\s+of\s+memor(?:y|ies)\b"
        r"|\bmemory site[s]?\b"
        r"|\blieux de mémoire\b"
    ),

    "Zagłada / Holocaust": (
        r"\bzagład[a-ząćęłńóśźż]*\b"
        r"|\bholocaust\b"
        r"|\bshoah\b"
        r"|\bauschwitz\b"
    ),

    "trauma": (
        r"\btraum[a-ząćęłńóśźż]*\b"
        r"|\btrauma(?:s|tic|tized|tised|tization|tisation)?\b"
    ),

    "świadectwo": (
        r"\bświadectw[a-ząćęłńóśźż]*\b"
        r"|\bświadk[a-ząćęłńóśźż]*\b"
        r"|\btestimon(?:y|ies|ial|ials)\b"
        r"|\bwitness(?:es|ing)?\b"
    ),

    "tożsamość": (
        r"\btożsamoś[ćc][a-ząćęłńóśźż]*\b"
        r"|\bidentit(?:y|ies|arian)?\b"
    ),

    "archiwum": (
        r"\barchiw[a-ząćęłńóśźż]*\b"
        r"|\barchive[s]?\b"
        r"|\barchival\b"
    ),

    "literatura": (
        r"\bliteratur[a-ząćęłńóśźż]*\b"
        r"|\bliterack[a-ząćęłńóśźż]*\b"
        r"|\bliteraturoznaw[a-ząćęłńóśźż]*\b"
        r"|\bliterary\b"
        r"|\bliterature[s]?\b"
    ),

    "narracja": (
        r"\bnarracj[a-ząćęłńóśźż]*\b"
        r"|\bnarracyjn[a-ząćęłńóśźż]*\b"
        r"|\bnarrativ(?:e|es|ity|ization|isation)?\b"
        r"|\bnarration[s]?\b"
    ),
}


# =============================================================================
# 6. WYKRYWANIE AUTORÓW I POJĘĆ
# =============================================================================

for author, pattern in author_patterns.items():
    df[f"author__{author}"] = df["analysis_text"].str.contains(
        pattern,
        regex=True,
        na=False,
        case=False
    )

for concept, pattern in concept_patterns.items():
    df[f"concept__{concept}"] = df["analysis_text"].str.contains(
        pattern,
        regex=True,
        na=False,
        case=False
    )


def authors_in_row(row):
    return sorted([
        author for author in author_patterns
        if row[f"author__{author}"]
    ])


def concepts_in_row(row):
    return sorted([
        concept for concept in concept_patterns
        if row[f"concept__{concept}"]
    ])


df["theoretical_authors_found"] = df.apply(authors_in_row, axis=1)
df["concepts_found"] = df.apply(concepts_in_row, axis=1)
df["theoretical_authors_count"] = df["theoretical_authors_found"].apply(len)


# =============================================================================
# 7. RANKING AUTORÓW TEORETYCZNYCH
# =============================================================================

author_rows = []

for author in tqdm(author_patterns):
    col = f"author__{author}"
    subset = df[df[col]].copy()

    if len(subset) == 0:
        continue

    years = subset["year"].dropna()

    # Liczba wystąpień w pełnych tekstach, nie tylko liczba rekordów.
    pattern = author_patterns[author]
    occurrence_count = int(
        df["analysis_text"]
        .str.count(pattern, flags=re.IGNORECASE)
        .sum()
    )

    author_rows.append({
        "author": author,
        "records": int(len(subset)),
        "occurrences": occurrence_count,
        "share_of_corpus_percent": round(len(subset) / len(df) * 100, 2) if len(df) else 0,
        "first_year": int(years.min()) if len(years) else None,
        "last_year": int(years.max()) if len(years) else None,
        "peak_year": int(subset.groupby("year").size().idxmax()) if len(subset) else None,
        "peak_year_records": int(subset.groupby("year").size().max()) if len(subset) else 0,
        "journals": int(subset["journal_clean"].nunique()),
    })

author_counts = pd.DataFrame(author_rows).sort_values(
    ["records", "occurrences"],
    ascending=False
)

author_counts.to_csv(
    OUT_AUTHOR_COUNTS,
    index=False,
    encoding="utf-8-sig"
)


# =============================================================================
# 8. TRAJEKTORIE AUTORÓW
# =============================================================================

trajectory_rows = []

for author in tqdm(author_patterns):
    col = f"author__{author}"
    temp = (
        df[df[col]]
        .groupby("year")
        .size()
        .reset_index(name="records")
    )
    temp["author"] = author
    trajectory_rows.append(temp)

author_trajectories_long = pd.concat(trajectory_rows, ignore_index=True)

author_trajectories_long.to_csv(
    OUT_AUTHOR_TRAJECTORIES_LONG,
    index=False,
    encoding="utf-8-sig"
)

all_years = pd.DataFrame({
    "year": range(df["year"].min(), df["year"].max() + 1)
})

author_trajectories_wide = all_years.copy()

for author in tqdm(author_patterns):
    temp = author_trajectories_long[
        author_trajectories_long["author"] == author
    ][["year", "records"]]

    temp = temp.rename(columns={"records": author})
    author_trajectories_wide = author_trajectories_wide.merge(
        temp,
        on="year",
        how="left"
    )

for author in tqdm(author_patterns):
    author_trajectories_wide[author] = (
        author_trajectories_wide[author]
        .fillna(0)
        .astype(int)
    )

    author_trajectories_wide[f"{author}__ma3"] = (
        author_trajectories_wide[author]
        .rolling(window=3, center=True, min_periods=1)
        .mean()
    )

author_trajectories_wide.to_csv(
    OUT_AUTHOR_TRAJECTORIES_WIDE,
    index=False,
    encoding="utf-8-sig"
)


# =============================================================================
# 9. RELACJE AUTOR TEORETYCZNY–POJĘCIE
# =============================================================================

author_concept_rows = []

for _, row in tqdm(df.iterrows()):
    article_id = row.get("bn_article_id", "")
    year = row.get("year", None)

    for author in row["theoretical_authors_found"]:
        for concept in row["concepts_found"]:
            author_concept_rows.append({
                "author": author,
                "concept": concept,
                "bn_article_id": article_id,
                "year": year,
                "journal": row.get("journal_clean", ""),
                "title": row.get("title", ""),
            })

author_concept_raw = pd.DataFrame(author_concept_rows)

if len(author_concept_raw) > 0:
    author_concept_edges = (
        author_concept_raw
        .groupby(["author", "concept"])
        .agg(
            weight=("bn_article_id", "nunique"),
            first_year=("year", "min"),
            last_year=("year", "max"),
            example_title=("title", "first"),
        )
        .reset_index()
        .sort_values("weight", ascending=False)
    )
else:
    author_concept_edges = pd.DataFrame(
        columns=["author", "concept", "weight", "first_year", "last_year", "example_title"]
    )

author_concept_edges.to_csv(
    OUT_AUTHOR_CONCEPT_EDGES,
    index=False,
    encoding="utf-8-sig"
)


# =============================================================================
# 10. RELACJE AUTOR TEORETYCZNY–CZASOPISMO
# =============================================================================

author_journal_rows = []

for _, row in tqdm(df.iterrows()):
    article_id = row.get("bn_article_id", "")
    year = row.get("year", None)
    journal = row.get("journal_clean", "")

    for author in row["theoretical_authors_found"]:
        author_journal_rows.append({
            "author": author,
            "journal": journal,
            "bn_article_id": article_id,
            "year": year,
            "title": row.get("title", ""),
        })

author_journal_raw = pd.DataFrame(author_journal_rows)

if len(author_journal_raw) > 0:
    author_journal_edges = (
        author_journal_raw
        .groupby(["author", "journal"])
        .agg(
            weight=("bn_article_id", "nunique"),
            first_year=("year", "min"),
            last_year=("year", "max"),
            example_title=("title", "first"),
        )
        .reset_index()
        .sort_values("weight", ascending=False)
    )
else:
    author_journal_edges = pd.DataFrame(
        columns=["author", "journal", "weight", "first_year", "last_year", "example_title"]
    )

author_journal_edges.to_csv(
    OUT_AUTHOR_JOURNAL_EDGES,
    index=False,
    encoding="utf-8-sig"
)


# =============================================================================
# 11. PRZYKŁADOWE TEKSTY DLA AUTORÓW
# =============================================================================

examples = []

for author in author_counts["author"].head(12).tolist():
    subset = df[df[f"author__{author}"]].copy()
    subset = subset.sort_values("year")

    for _, row in subset.head(5).iterrows():
        examples.append({
            "author": author,
            "year": row.get("year", ""),
            "title": row.get("title", ""),
            "journal": row.get("journal_clean", ""),
            "creators": row.get("creators", ""),
            "bn_article_id": row.get("bn_article_id", ""),
            "article_url": row.get("article_url", ""),
        })

author_examples = pd.DataFrame(examples)
author_examples.to_csv(
    OUT_AUTHOR_EXAMPLES,
    index=False,
    encoding="utf-8-sig"
)


# =============================================================================
# 12. GRAF AUTOR–POJĘCIE DO GEPHI
# =============================================================================

MIN_EDGE_WEIGHT_FOR_GEPHI = 2
TOP_AUTHORS_FOR_GEPHI = 15

top_authors = set(author_counts.head(TOP_AUTHORS_FOR_GEPHI)["author"].tolist())

ac_gephi = author_concept_edges[
    (author_concept_edges["author"].isin(top_authors)) &
    (author_concept_edges["weight"] >= MIN_EDGE_WEIGHT_FOR_GEPHI)
].copy()

G = nx.Graph()

for _, row in author_counts.iterrows():
    author = row["author"]

    if author not in top_authors:
        continue

    G.add_node(
        author,
        label=author,
        node_type="theoretical_author",
        records=int(row["records"]),
        occurrences=int(row["occurrences"]),
        first_year=int(row["first_year"]) if pd.notna(row["first_year"]) else None,
        last_year=int(row["last_year"]) if pd.notna(row["last_year"]) else None,
    )

used_concepts = sorted(ac_gephi["concept"].unique().tolist())

for concept in used_concepts:
    concept_weight = int(
        ac_gephi[ac_gephi["concept"] == concept]["weight"].sum()
    )

    G.add_node(
        concept,
        label=concept,
        node_type="concept",
        records=concept_weight,
    )

for _, row in ac_gephi.iterrows():
    G.add_edge(
        row["author"],
        row["concept"],
        weight=int(row["weight"]),
        first_year=int(row["first_year"]) if pd.notna(row["first_year"]) else None,
        last_year=int(row["last_year"]) if pd.notna(row["last_year"]) else None,
    )

degree = dict(G.degree())
weighted_degree = dict(G.degree(weight="weight"))

nx.set_node_attributes(G, degree, "degree")
nx.set_node_attributes(G, weighted_degree, "weighted_degree")

nx.write_gexf(G, OUT_GEXF)


# =============================================================================
# 13. WYKRES: RANKING AUTORÓW
# =============================================================================

TOP_N = 15

top_authors_plot = author_counts.head(TOP_N).sort_values("records", ascending=True)

plt.figure(figsize=(11, 8))
plt.barh(
    top_authors_plot["author"],
    top_authors_plot["records"]
)

plt.title("Kogo czyta polska pamięciologia?", fontsize=15)
plt.xlabel("Liczba tekstów, w których rozpoznano autora")
plt.ylabel("Autor / autorka teoretyczna")
plt.tight_layout()
plt.savefig(OUT_FIG_COUNTS, dpi=300)
plt.close()


# =============================================================================
# 14. WYKRES: TRAJEKTORIE NAJCZĘSTSZYCH AUTORÓW
# =============================================================================

top_authors_for_trajectory = author_counts.head(8)["author"].tolist()

plt.figure(figsize=(13, 7))

for author in top_authors_for_trajectory:
    plt.plot(
        author_trajectories_wide["year"],
        author_trajectories_wide[f"{author}__ma3"],
        linewidth=2,
        marker="o",
        markersize=3,
        label=author
    )

plt.title("Trajektorie autorów teoretycznych w korpusie", fontsize=15)
plt.xlabel("Rok")
plt.ylabel("Liczba tekstów, średnia krocząca 3-letnia")
plt.grid(True, alpha=0.3)
plt.legend(loc="upper left", fontsize=9)
plt.tight_layout()
plt.savefig(OUT_FIG_TRAJECTORIES, dpi=300)
plt.close()


# =============================================================================
# 15. KOMENTARZ DO SLAJDU
# =============================================================================

top_author_lines = []
for _, row in author_counts.head(12).iterrows():
    top_author_lines.append(
        f"- {row['author']}: {int(row['records'])} tekstów, "
        f"{int(row['occurrences'])} wystąpień, "
        f"zakres {int(row['first_year']) if pd.notna(row['first_year']) else 'brak'}–"
        f"{int(row['last_year']) if pd.notna(row['last_year']) else 'brak'}"
    )

top_author_concept_lines = []
for _, row in author_concept_edges.head(12).iterrows():
    top_author_concept_lines.append(
        f"- {row['author']} — {row['concept']}: {int(row['weight'])} tekstów"
    )

top_author_journal_lines = []
for _, row in author_journal_edges.head(10).iterrows():
    top_author_journal_lines.append(
        f"- {row['author']} — {row['journal']}: {int(row['weight'])} tekstów"
    )

records_with_any_author = int((df["theoretical_authors_count"] >= 1).sum())
records_with_fulltext = int(df["has_real_fulltext"].sum())

commentary = f"""
SLAJD 8
Kogo czyta polska pamięciologia?

Dane:
- liczba rekordów w korpusie: {len(df)}
- liczba rekordów z pełnym tekstem >= 300 słów: {records_with_fulltext}
- liczba rekordów z przynajmniej jednym rozpoznanym autorem/autorką teoretyczną: {records_with_any_author}
- liczba rozpoznanych autorów/autorek teoretycznych: {len(author_counts)}

Najczęściej rozpoznani autorzy/autorki:
{chr(10).join(top_author_lines)}

Najsilniejsze relacje autor/autorka — pojęcie:
{chr(10).join(top_author_concept_lines)}

Najsilniejsze relacje autor/autorka — czasopismo:
{chr(10).join(top_author_journal_lines)}

Proponowany komentarz do slajdu:

Ten slajd wykorzystuje pełne teksty, a nie tylko metadane. Dzięki temu możemy zapytać, z jakiego zaplecza teoretycznego korzysta polska pamięciologia. Nie chodzi tu jeszcze o precyzyjną analizę cytowań bibliograficznych, lecz o rozpoznanie nazwisk i pojęć obecnych w języku artykułów. Widać, które tradycje teoretyczne — pamięć zbiorowa, pamięć kulturowa, miejsca pamięci, postpamięć, trauma, świadectwo czy Zagłada — są przywoływane jako wspólny aparat interpretacyjny.

Bezpieczna teza:
To nie jest jeszcze kanon ustalony na podstawie pełnej analizy bibliografii załącznikowych. To mapa rozpoznawalnych odniesień teoretycznych w pełnych tekstach. Pokazuje, kogo korpus przywołuje, ale nie rozstrzyga jeszcze, jak dokładnie ci autorzy są interpretowani.

Sugestia do prezentacji:
Na slajdzie pokaż ranking autorów teoretycznych. Jako drugi element można dodać 3–5 relacji autor–pojęcie, np. Halbwachs — pamięć zbiorowa, Assmann — pamięć kulturowa, Nora — miejsce pamięci, Hirsch — postpamięć.
""".strip()

OUT_TEXT.write_text(commentary, encoding="utf-8")


# =============================================================================
# 16. PODSUMOWANIE W KONSOLI
# =============================================================================

print("Gotowe: slajd 8")
print()
print("Użyta kolumna pełnotekstowa:", TEXT_COLUMN)
print()
print("Zapisano:")
print("-", OUT_AUTHOR_COUNTS)
print("-", OUT_AUTHOR_TRAJECTORIES_LONG)
print("-", OUT_AUTHOR_TRAJECTORIES_WIDE)
print("-", OUT_AUTHOR_CONCEPT_EDGES)
print("-", OUT_AUTHOR_JOURNAL_EDGES)
print("-", OUT_AUTHOR_EXAMPLES)
print("-", OUT_GEXF)
print("-", OUT_FIG_COUNTS)
print("-", OUT_FIG_TRAJECTORIES)
print("-", OUT_TEXT)
print()
print(commentary)

#%% slajd 9

# -*- coding: utf-8 -*-
"""
Slajd 9 demonstratora:
Gdzie w tej mapie znajduje się polonistyka?

Cel:
- wydzielić z korpusu pamięciologicznego podkorpus polonistyczny/literaturoznawczy,
- pokazać udział polonistyki w korpusie,
- wskazać pojęcia, czasopisma i sąsiedztwa, przez które polonistyka uczestniczy
  w języku pamięci.
"""

from pathlib import Path
import itertools
import pandas as pd
import matplotlib.pyplot as plt
import networkx as nx


# =============================================================================
# 0. ŚCIEŻKI
# =============================================================================

INPUT = Path("data/bn_pamiec_fast/pamiec_outputs/pamiec_02_memory_studies_candidate.csv")

OUT_DIR = Path("data/bn_pamiec_fast/pamiec_demo_outputs/slide_09")
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_SUMMARY = OUT_DIR / "slide_09_polonistyka_summary.csv"
OUT_POLISH_STUDIES_CORPUS = OUT_DIR / "slide_09_polonistyka_records.csv"
OUT_CONCEPT_COUNTS = OUT_DIR / "slide_09_polonistyka_concept_counts.csv"
OUT_JOURNAL_RANKING = OUT_DIR / "slide_09_polonistyka_journal_ranking.csv"
OUT_CONCEPT_EDGES = OUT_DIR / "slide_09_polonistyka_concept_edges.csv"
OUT_GEXF = OUT_DIR / "slide_09_polonistyka_concept_network.gexf"
OUT_FIG_SHARE = OUT_DIR / "slide_09_udzial_polonistyki.png"
OUT_FIG_CONCEPTS = OUT_DIR / "slide_09_pojecia_polonistyki.png"
OUT_FIG_JOURNALS = OUT_DIR / "slide_09_czasopisma_polonistyczne.png"
OUT_TEXT = OUT_DIR / "slide_09_commentary_for_slide.txt"


# =============================================================================
# 1. WCZYTANIE DANYCH
# =============================================================================

df = pd.read_csv(INPUT, dtype=str, encoding="utf-8-sig")

if "year_num" in df.columns:
    df["year"] = pd.to_numeric(df["year_num"], errors="coerce")
else:
    df["year"] = pd.to_numeric(df["year"], errors="coerce")

df = df.dropna(subset=["year"]).copy()
df["year"] = df["year"].astype(int)
df = df[(df["year"] >= 1900) & (df["year"] <= 2026)].copy()

for col in ["title", "description", "journal", "source", "publisher", "creators"]:
    if col not in df.columns:
        df[col] = ""

df["journal_clean"] = df["journal"].fillna("").str.strip()

if df["journal_clean"].eq("").all() and "source" in df.columns:
    df["journal_clean"] = (
        df["source"]
        .fillna("")
        .str.split(";")
        .str[0]
        .str.strip()
    )

df.loc[df["journal_clean"] == "", "journal_clean"] = "brak danych"

df["analysis_text"] = (
    df["title"].fillna("") + " " +
    df["description"].fillna("") + " " +
    df["journal_clean"].fillna("") + " " +
    df["source"].fillna("") + " " +
    df["publisher"].fillna("") + " " +
    df["creators"].fillna("")
).str.lower()

df["analysis_text"] = (
    df["analysis_text"]
    .str.replace(r"\s+", " ", regex=True)
    .str.replace("–", "-", regex=False)
    .str.replace("—", "-", regex=False)
    .str.strip()
)


# =============================================================================
# 2. REGUŁY IDENTYFIKACJI POLONISTYKI / LITERATUROZNAWSTWA
# =============================================================================
# Celowo rozdzielamy sygnały:
# - mocne sygnały polonistyczne,
# - sygnały literaturoznawcze,
# - sygnały tekstowo-gatunkowe,
# - sygnały czasopismowe.

polish_studies_patterns = {
    "strong_polish_studies": (
        r"\bpolonist[a-ząćęłńóśźż]*\b"
        r"|\bpolonistyk[a-ząćęłńóśźż]*\b"
        r"|\bfilologi[a-ząćęłńóśźż]*\s+polsk[a-ząćęłńóśźż]*\b"
        r"|\bliteratur[a-ząćęłńóśźż]*\s+polsk[a-ząćęłńóśźż]*\b"
        r"|\bpolish literature\b"
        r"|\bpolish literary studies\b"
    ),

    "literary_studies": (
        r"\bliteraturoznaw[a-ząćęłńóśźż]*\b"
        r"|\bliterack[a-ząćęłńóśźż]*\b"
        r"|\bliteratur[a-ząćęłńóśźż]*\b"
        r"|\bliterary\b"
        r"|\bliterature\b"
        r"|\bliterary studies\b"
    ),

    "genres_and_forms": (
        r"\bpoezj[a-ząćęłńóśźż]*\b"
        r"|\bwiersz[a-ząćęłńóśźż]*\b"
        r"|\bpowieś[ćc][a-ząćęłńóśźż]*\b"
        r"|\bproza\b"
        r"|\bprozatorsk[a-ząćęłńóśźż]*\b"
        r"|\bdramat[a-ząćęłńóśźż]*\b"
        r"|\besej[a-ząćęłńóśźż]*\b"
        r"|\breporta[żz][a-ząćęłńóśźż]*\b"
        r"|\bpoetr(?:y|ies)\b"
        r"|\bpoem[s]?\b"
        r"|\bnovel[s]?\b"
        r"|\bfiction\b"
        r"|\bprose\b"
        r"|\bdrama\b"
        r"|\bessay[s]?\b"
    ),

    "interpretive_categories": (
        r"\bnarracj[a-ząćęłńóśźż]*\b"
        r"|\bnarracyjn[a-ząćęłńóśźż]*\b"
        r"|\bautobiografi[a-ząćęłńóśźż]*\b"
        r"|\bwspomnieni[a-ząćęłńóśźż]*\b"
        r"|\bpamiętnik[a-ząćęłńóśźż]*\b"
        r"|\bdziennik[a-ząćęłńóśźż]*\b"
        r"|\bświadectw[a-ząćęłńóśźż]*\b"
        r"|\bnarrativ(?:e|es|ity)?\b"
        r"|\bautobiograph(?:y|ies|ical)\b"
        r"|\bmemoir[s]?\b"
        r"|\bdiar(?:y|ies|istic)\b"
        r"|\btestimon(?:y|ies|ial|ials)\b"
    ),

    "journal_title_signal": (
        r"\bpamiętnik literacki\b"
        r"|\bruch literacki\b"
        r"|\bteksty drugie\b"
        r"|\bwielogłos\b"
        r"|\bprzestrzenie teorii\b"
        r"|\bpostscriptum polonistyczne\b"
        r"|\bporównania\b"
        r"|\bfabrica litterarum\b"
        r"|\bforum poetyki\b"
        r"|\btekstualia\b"
        r"|\bslavia occidentalis\b"
        r"|\bacta universitatis lodziensis\. folia litteraria\b"
        r"|\bannales universitatis paedagogicae cracoviensis\. studia poetica\b"
    ),
}


for name, pattern in polish_studies_patterns.items():
    df[f"pol_signal__{name}"] = df["analysis_text"].str.contains(
        pattern,
        regex=True,
        na=False,
        case=False
    )


# Punktacja:
# mocne sygnały polonistyczne i tytuły czasopism są ważniejsze;
# same „literature/literatura” są szerokie, więc wymagają wsparcia innymi sygnałami.

df["polish_studies_score"] = (
    df["pol_signal__strong_polish_studies"].astype(int) * 4 +
    df["pol_signal__literary_studies"].astype(int) * 2 +
    df["pol_signal__genres_and_forms"].astype(int) * 2 +
    df["pol_signal__interpretive_categories"].astype(int) * 1 +
    df["pol_signal__journal_title_signal"].astype(int) * 4
)

df["is_polish_studies_candidate"] = (
    (df["polish_studies_score"] >= 4)
    |
    (
        df["pol_signal__literary_studies"]
        &
        df["pol_signal__interpretive_categories"]
    )
    |
    df["pol_signal__journal_title_signal"]
)

pol_df = df[df["is_polish_studies_candidate"]].copy()


# =============================================================================
# 3. SŁOWNIK POJĘĆ DLA PODKORPUSU POLONISTYCZNEGO
# =============================================================================

concept_patterns = {
    "literatura": (
        r"\bliteratur[a-ząćęłńóśźż]*\b"
        r"|\bliterack[a-ząćęłńóśźż]*\b"
        r"|\bliteraturoznaw[a-ząćęłńóśźż]*\b"
        r"|\bliterary\b"
        r"|\bliterature[s]?\b"
        r"|\bliterary studies\b"
    ),

    "narracja": (
        r"\bnarracj[a-ząćęłńóśźż]*\b"
        r"|\bnarracyjn[a-ząćęłńóśźż]*\b"
        r"|\bnarrativ(?:e|es|ity|ization|isation)?\b"
        r"|\bnarration[s]?\b"
    ),

    "świadectwo": (
        r"\bświadectw[a-ząćęłńóśźż]*\b"
        r"|\bświadk[a-ząćęłńóśźż]*\b"
        r"|\btestimon(?:y|ies|ial|ials)\b"
        r"|\bwitness(?:es|ing)?\b"
    ),

    "Zagłada / Holocaust": (
        r"\bzagład[a-ząćęłńóśźż]*\b"
        r"|\bholocaust\b"
        r"|\bshoah\b"
        r"|\bauschwitz\b"
    ),

    "trauma": (
        r"\btraum[a-ząćęłńóśźż]*\b"
        r"|\btrauma(?:s|tic|tized|tised|tization|tisation)?\b"
    ),

    "postpamięć": (
        r"\bpostpamię[a-ząćęłńóśźż]*\b"
        r"|\bpost-memory\b"
        r"|\bpostmemory\b"
    ),

    "pamięć kulturowa": (
        r"\b(pamięć|pamięci|pamięcią|pamięciach|pamięciami)"
        r"\s+kulturow[a-ząćęłńóśźż]*\b"
        r"|\bcultural memor(?:y|ies)\b"
    ),

    "pamięć zbiorowa": (
        r"\b(pamięć|pamięci|pamięcią|pamięciach|pamięciami)"
        r"\s+zbiorow[a-ząćęłńóśźż]*\b"
        r"|\bcollective memor(?:y|ies)\b"
    ),

    "pamięć historyczna": (
        r"\b(pamięć|pamięci|pamięcią|pamięciach|pamięciami)"
        r"\s+historyczn[a-ząćęłńóśźż]*\b"
        r"|\bhistorical memor(?:y|ies)\b"
    ),

    "polityka pamięci": (
        r"\bpolityk[a-ząćęłńóśźż]*\s+"
        r"(pamięci|pamięcią|pamięciach|pamięciami)\b"
        r"|\bpolitics of memor(?:y|ies)\b"
        r"|\bmemory politics\b"
    ),

    "miejsce pamięci": (
        r"\bmiejsc[a-ząćęłńóśźż]*\s+"
        r"(pamięci|pamięcią|pamięciach|pamięciami)\b"
        r"|\bsite[s]?\s+of\s+memor(?:y|ies)\b"
        r"|\bmemory site[s]?\b"
        r"|\blieux de mémoire\b"
    ),

    "archiwum": (
        r"\barchiw[a-ząćęłńóśźż]*\b"
        r"|\barchive[s]?\b"
        r"|\barchival\b"
    ),

    "autobiografia / wspomnienia": (
        r"\bautobiografi[a-ząćęłńóśźż]*\b"
        r"|\bwspomnieni[a-ząćęłńóśźż]*\b"
        r"|\bpamiętnik[a-ząćęłńóśźż]*\b"
        r"|\bdziennik[a-ząćęłńóśźż]*\b"
        r"|\bautobiograph(?:y|ies|ical)\b"
        r"|\bmemoir[s]?\b"
        r"|\bdiar(?:y|ies|istic)\b"
    ),

    "poezja": (
        r"\bpoezj[a-ząćęłńóśźż]*\b"
        r"|\bpoetyck[a-ząćęłńóśźż]*\b"
        r"|\bwiersz[a-ząćęłńóśźż]*\b"
        r"|\bpoetr(?:y|ies)\b"
        r"|\bpoem[s]?\b"
        r"|\bpoetic[s]?\b"
    ),

    "powieść": (
        r"\bpowieś[ćc][a-ząćęłńóśźż]*\b"
        r"|\bproza\b"
        r"|\bprozatorsk[a-ząćęłńóśźż]*\b"
        r"|\bnovel[s]?\b"
        r"|\bfiction\b"
        r"|\bprose\b"
    ),

    "tożsamość": (
        r"\btożsamoś[ćc][a-ząćęłńóśźż]*\b"
        r"|\bidentit(?:y|ies|arian)?\b"
    ),
}


for concept, pattern in concept_patterns.items():
    pol_df[f"concept__{concept}"] = pol_df["analysis_text"].str.contains(
        pattern,
        regex=True,
        na=False,
        case=False
    )


def concepts_in_row(row):
    return sorted([
        concept for concept in concept_patterns
        if row[f"concept__{concept}"]
    ])


pol_df["concepts_found"] = pol_df.apply(concepts_in_row, axis=1)
pol_df["concepts_count"] = pol_df["concepts_found"].apply(len)


# =============================================================================
# 4. PODSUMOWANIE: UDZIAŁ POLONISTYKI
# =============================================================================

summary = {
    "total_memory_studies_records": len(df),
    "polish_studies_records": len(pol_df),
    "polish_studies_share_percent": round(len(pol_df) / len(df) * 100, 2) if len(df) else 0,
    "total_journals": df["journal_clean"].nunique(),
    "polish_studies_journals": pol_df["journal_clean"].nunique(),
    "first_year_polish_studies": int(pol_df["year"].min()) if len(pol_df) else None,
    "last_year_polish_studies": int(pol_df["year"].max()) if len(pol_df) else None,
}

summary_df = pd.DataFrame([summary])
summary_df.to_csv(OUT_SUMMARY, index=False, encoding="utf-8-sig")

pol_df.to_csv(OUT_POLISH_STUDIES_CORPUS, index=False, encoding="utf-8-sig")


# =============================================================================
# 5. NAJWAŻNIEJSZE POJĘCIA W PODKORPUSIE POLONISTYCZNYM
# =============================================================================

concept_rows = []

for concept in concept_patterns:
    col = f"concept__{concept}"
    subset = pol_df[pol_df[col]].copy()

    if len(subset) == 0:
        continue

    years = subset["year"].dropna()

    concept_rows.append({
        "concept": concept,
        "records": int(len(subset)),
        "share_of_polish_studies_percent": round(len(subset) / len(pol_df) * 100, 2) if len(pol_df) else 0,
        "first_year": int(years.min()) if len(years) else None,
        "last_year": int(years.max()) if len(years) else None,
    })

concept_counts = (
    pd.DataFrame(concept_rows)
    .sort_values("records", ascending=False)
)

concept_counts.to_csv(
    OUT_CONCEPT_COUNTS,
    index=False,
    encoding="utf-8-sig"
)


# =============================================================================
# 6. CZASOPISMA POLONISTYCZNE / LITERATUROZNAWCZE W KORPUSIE
# =============================================================================

journal_ranking = (
    pol_df.groupby("journal_clean")
    .agg(
        records=("bn_article_id", "nunique"),
        first_year=("year", "min"),
        last_year=("year", "max"),
        avg_polish_studies_score=("polish_studies_score", "mean"),
        distinct_concepts=("concepts_found", lambda rows: len(set(sum(rows, [])))),
    )
    .reset_index()
    .sort_values(["records", "distinct_concepts"], ascending=False)
)

journal_ranking["first_year"] = journal_ranking["first_year"].astype("Int64")
journal_ranking["last_year"] = journal_ranking["last_year"].astype("Int64")

journal_ranking.to_csv(
    OUT_JOURNAL_RANKING,
    index=False,
    encoding="utf-8-sig"
)


# =============================================================================
# 7. WSPÓŁWYSTĘPOWANIE POJĘĆ W PODKORPUSIE POLONISTYCZNYM
# =============================================================================

edge_rows = []

for _, row in pol_df.iterrows():
    concepts = row["concepts_found"]

    if len(concepts) < 2:
        continue

    for source, target in itertools.combinations(concepts, 2):
        edge_rows.append({
            "source": source,
            "target": target,
            "bn_article_id": row.get("bn_article_id", ""),
            "year": row.get("year", None),
            "journal": row.get("journal_clean", ""),
            "title": row.get("title", ""),
        })

edges_raw = pd.DataFrame(edge_rows)

if len(edges_raw) > 0:
    concept_edges = (
        edges_raw
        .groupby(["source", "target"])
        .agg(
            weight=("bn_article_id", "nunique"),
            first_year=("year", "min"),
            last_year=("year", "max"),
            example_title=("title", "first"),
        )
        .reset_index()
        .sort_values("weight", ascending=False)
    )
else:
    concept_edges = pd.DataFrame(
        columns=["source", "target", "weight", "first_year", "last_year", "example_title"]
    )

concept_edges.to_csv(
    OUT_CONCEPT_EDGES,
    index=False,
    encoding="utf-8-sig"
)


# =============================================================================
# 8. GRAF POLONISTYCZNY DO GEPHI
# =============================================================================

MIN_EDGE_WEIGHT_FOR_GEPHI = 2

G = nx.Graph()

for _, row in concept_counts.iterrows():
    G.add_node(
        row["concept"],
        label=row["concept"],
        node_type="polish_studies_concept",
        records=int(row["records"]),
        first_year=int(row["first_year"]) if pd.notna(row["first_year"]) else None,
        last_year=int(row["last_year"]) if pd.notna(row["last_year"]) else None,
    )

for _, row in concept_edges.iterrows():
    if int(row["weight"]) < MIN_EDGE_WEIGHT_FOR_GEPHI:
        continue

    G.add_edge(
        row["source"],
        row["target"],
        weight=int(row["weight"]),
        first_year=int(row["first_year"]) if pd.notna(row["first_year"]) else None,
        last_year=int(row["last_year"]) if pd.notna(row["last_year"]) else None,
    )

degree = dict(G.degree())
weighted_degree = dict(G.degree(weight="weight"))

nx.set_node_attributes(G, degree, "degree")
nx.set_node_attributes(G, weighted_degree, "weighted_degree")

nx.write_gexf(G, OUT_GEXF)


# =============================================================================
# 9. WYKRES: UDZIAŁ POLONISTYKI
# =============================================================================

share_df = pd.DataFrame({
    "category": ["podkorpus polonistyczny", "pozostały korpus"],
    "records": [len(pol_df), len(df) - len(pol_df)]
})

plt.figure(figsize=(8, 8))
plt.pie(
    share_df["records"],
    labels=share_df["category"],
    autopct="%1.1f%%",
    startangle=90
)
plt.title("Udział podkorpusu polonistycznego w korpusie pamięci")
plt.tight_layout()
plt.savefig(OUT_FIG_SHARE, dpi=300)
plt.close()


# =============================================================================
# 10. WYKRES: POJĘCIA POLONISTYKI
# =============================================================================

TOP_N = 12

top_concepts_plot = concept_counts.head(TOP_N).sort_values("records", ascending=True)

plt.figure(figsize=(11, 8))
plt.barh(
    top_concepts_plot["concept"],
    top_concepts_plot["records"]
)

plt.title("Pojęcia pamięci w podkorpusie polonistycznym", fontsize=14)
plt.xlabel("Liczba tekstów")
plt.ylabel("Pojęcie")
plt.tight_layout()
plt.savefig(OUT_FIG_CONCEPTS, dpi=300)
plt.close()


# =============================================================================
# 11. WYKRES: CZASOPISMA
# =============================================================================

top_journals_plot = (
    journal_ranking[journal_ranking["journal_clean"] != "brak danych"]
    .head(TOP_N)
    .sort_values("records", ascending=True)
)

plt.figure(figsize=(11, 8))
plt.barh(
    top_journals_plot["journal_clean"],
    top_journals_plot["records"]
)

plt.title("Czasopisma polonistyczne/literaturoznawcze w korpusie pamięci", fontsize=14)
plt.xlabel("Liczba tekstów")
plt.ylabel("Czasopismo")
plt.tight_layout()
plt.savefig(OUT_FIG_JOURNALS, dpi=300)
plt.close()


# =============================================================================
# 12. KOMENTARZ DO SLAJDU
# =============================================================================

concept_lines = []
for _, row in concept_counts.head(10).iterrows():
    concept_lines.append(
        f"- {row['concept']}: {int(row['records'])} tekstów "
        f"({row['share_of_polish_studies_percent']}% podkorpusu)"
    )

journal_lines = []
for _, row in journal_ranking.head(10).iterrows():
    journal_lines.append(
        f"- {row['journal_clean']}: {int(row['records'])} tekstów, "
        f"{int(row['distinct_concepts'])} różnych pojęć"
    )

edge_lines = []
for _, row in concept_edges.head(10).iterrows():
    edge_lines.append(
        f"- {row['source']} — {row['target']}: {int(row['weight'])} współwystąpień"
    )

commentary = f"""
SLAJD 9
Gdzie w tej mapie znajduje się polonistyka?

Dane:
- liczba rekordów w całym korpusie pamięciologicznym: {len(df)}
- liczba rekordów w podkorpusie polonistycznym/literaturoznawczym: {len(pol_df)}
- udział podkorpusu polonistycznego: {summary['polish_studies_share_percent']}%
- liczba czasopism w podkorpusie polonistycznym: {summary['polish_studies_journals']}
- zakres lat podkorpusu: {summary['first_year_polish_studies']}–{summary['last_year_polish_studies']}

Najważniejsze pojęcia w podkorpusie polonistycznym:
{chr(10).join(concept_lines)}

Najbardziej widoczne czasopisma:
{chr(10).join(journal_lines)}

Najsilniejsze sąsiedztwa pojęciowe:
{chr(10).join(edge_lines)}

Proponowany komentarz do slajdu:

Ten slajd odpowiada na pytanie, gdzie w szerokiej mapie pamięci znajduje się polonistyka. Nie chodzi tylko o teksty, które wprost deklarują temat literatury. Interesują nas także te miejsca, w których problematyka pamięci łączy się z narracją, świadectwem, Zagładą, traumą, autobiografią, archiwum, poezją, powieścią i interpretacją literacką. W tym sensie polonistyka nie jest bocznym odbiorcą języka pamięci, lecz jednym z miejsc, w których pamięć uzyskuje materiał tekstowy, formy narracyjne i narzędzia interpretacji.

Bezpieczna teza:
To jest podkorpus wyznaczony regułowo, a nie ostateczna klasyfikacja dyscyplinarna. Pokazuje jednak, że polonistyka uczestniczy w obiegu pamięci przez określone sąsiedztwa pojęciowe: literatura–narracja–świadectwo–Zagłada–trauma–archiwum–tożsamość.

Sugestia do prezentacji:
Najlepiej pokazać jeden wykres udziału polonistyki oraz drugi wykres z najważniejszymi pojęciami podkorpusu. Graf GEXF można pokazać w Gephi jako literaturoznawczą mapę sąsiedztw pamięci.
""".strip()

OUT_TEXT.write_text(commentary, encoding="utf-8")


# =============================================================================
# 13. PODSUMOWANIE W KONSOLI
# =============================================================================

print("Gotowe: slajd 9")
print()
print("Zapisano:")
print("-", OUT_SUMMARY)
print("-", OUT_POLISH_STUDIES_CORPUS)
print("-", OUT_CONCEPT_COUNTS)
print("-", OUT_JOURNAL_RANKING)
print("-", OUT_CONCEPT_EDGES)
print("-", OUT_GEXF)
print("-", OUT_FIG_SHARE)
print("-", OUT_FIG_CONCEPTS)
print("-", OUT_FIG_JOURNALS)
print("-", OUT_TEXT)
print()
print(commentary)






#%%
# df = pd.read_csv(BASE + "pamiec_metadata_clean.csv")
df = pd.read_csv(BASE + "pamiec_fulltext_outputs/pamiec_02_candidate_fulltext_clean.csv")

#%%
year_counts = (
    df.dropna(subset=["year"])
    .groupby("year")
    .size()
    .reset_index(name="records")
    .sort_values("year")
)

plt.figure(figsize=(12, 6))
plt.plot(year_counts["year"], year_counts["records"], marker="o")
plt.title("Publikacje z korpusu „pamięć” według roku")
plt.xlabel("Rok")
plt.ylabel("Liczba rekordów")
plt.grid(True, alpha=0.3)
plt.tight_layout()
# plt.savefig(BASE + "pamiec_timeline_all.png", dpi=300)
plt.savefig(BASE + "pamiec_timeline_memory studies.png", dpi=300)
plt.show()






















#%%
BASE = 'data/bn_pamiec_fast/'

INPUT = Path(BASE + "pamiec_fulltext_outputs/pamiec_02_candidate_fulltext_clean.csv")

OUT_DIR = Path(BASE + "pamiec_fulltext_outputs")
OUT_AUTHOR_COUNTS = OUT_DIR / "pamiec_03_theoretical_author_counts.csv"
OUT_AUTHOR_YEAR = OUT_DIR / "pamiec_04_theoretical_author_by_year.csv"

df = pd.read_csv(INPUT, dtype=str, encoding="utf-8-sig", low_memory=False)

df["year_num"] = pd.to_numeric(df["year"], errors="coerce")

text_col = "fulltext_clean"

authors = {
    "Maurice Halbwachs": r"\bhalbwachs\b",
    "Jan Assmann": r"\bjan assmann\b|\bassmann\b",
    "Aleida Assmann": r"\baleida assmann\b",
    "Pierre Nora": r"\bpierre nora\b|\bnora\b",
    "Marianne Hirsch": r"\bmarianne hirsch\b|\bhirsch\b",
    "Paul Ricoeur": r"\bpaul ricoeur\b|\bricoeur\b",
    "Astrid Erll": r"\bastrid erll\b|\berll\b",
    "Michael Rothberg": r"\bmichael rothberg\b|\brothberg\b",
    "Dominick LaCapra": r"\bdominick lacapra\b|\blacapra\b",
    "Andreas Huyssen": r"\bandreas huyssen\b|\bhuyssen\b",
}

for author, pattern in authors.items():
    df[f"author__{author}"] = df[text_col].fillna("").str.lower().str.contains(
        pattern,
        regex=True
    )

rows = []

for author in authors:
    col = f"author__{author}"
    subset = df[df[col]]

    rows.append({
        "author": author,
        "records": len(subset),
        "first_year": subset["year_num"].min(),
        "last_year": subset["year_num"].max(),
    })

author_counts = pd.DataFrame(rows).sort_values("records", ascending=False)
author_counts.to_csv(OUT_AUTHOR_COUNTS, index=False, encoding="utf-8-sig")

year_rows = []

for author in authors:
    col = f"author__{author}"
    temp = (
        df[df[col]]
        .dropna(subset=["year_num"])
        .groupby("year_num")
        .size()
        .reset_index(name="records")
    )
    temp["author"] = author
    year_rows.append(temp)

author_year = pd.concat(year_rows, ignore_index=True)
author_year.to_csv(OUT_AUTHOR_YEAR, index=False, encoding="utf-8-sig")

print(author_counts)





















