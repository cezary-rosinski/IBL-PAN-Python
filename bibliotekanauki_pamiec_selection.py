import pandas as pd
import re
from pathlib import Path

#%%
INPUT = "data/bn_pamiec_fast/pamiec_metadata.csv"
OUTPUT = "data/bn_pamiec_fast/pamiec_metadata_clean.csv"

df = pd.read_csv(INPUT)

df["journal"] = (
    df["source"]
    .fillna("")
    .str.split(";")
    .str[0]
    .str.strip()
)

df["has_description"] = df["description"].notna() & (df["description"].astype(str).str.strip() != "")
df["has_pdf"] = df["pdf_url"].notna() & (df["pdf_url"].astype(str).str.strip() != "")

df["text_for_analysis"] = (
    df["title"].fillna("") + ". " +
    df["description"].fillna("")
)

df["year"] = pd.to_numeric(df["year"], errors="coerce")

df.to_csv(OUTPUT, index=False, encoding="utf-8")


#%%

df = pd.read_csv("data/bn_pamiec_fast/pamiec_metadata_clean.csv")

print("Liczba rekordów:", len(df))
print("Zakres lat:", int(df["year"].min()), "-", int(df["year"].max()))
print("Z abstraktem:", df["has_description"].sum())
print("Z PDF:", df["has_pdf"].sum())

print("\nJęzyki:")
print(df["language"].value_counts().head(15))

print("\nNajczęstsze czasopisma:")
print(df["journal"].value_counts().head(30))

print("\nPublikacje według roku:")
print(df["year"].value_counts().sort_index())

#%%

# =============================================================================
# 0. ŚCIEŻKI
# =============================================================================

INPUT_CSV = Path("data/bn_pamiec_fast/pamiec_metadata.csv")

OUTPUT_DIR = Path("data/bn_pamiec_fast/pamiec_outputs")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_ALL_SCORED = OUTPUT_DIR / "pamiec_01_all_scored.csv"
OUT_MEMORY_STUDIES = OUTPUT_DIR / "pamiec_02_memory_studies_candidate.csv"
OUT_EXCLUDED = OUTPUT_DIR / "pamiec_03_excluded_or_unclear.csv"
OUT_REVIEW = OUTPUT_DIR / "pamiec_04_manual_review_sample.csv"
OUT_REPORT = OUTPUT_DIR / "pamiec_00_filter_report.txt"
OUT_JOURNALS = OUTPUT_DIR / "pamiec_05_journal_counts.csv"
OUT_DOMAINS = OUTPUT_DIR / "pamiec_06_domain_counts.csv"


# =============================================================================
# 1. FUNKCJE POMOCNICZE
# =============================================================================

def normalize_text(value):
    if pd.isna(value):
        return ""
    value = str(value).lower()
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def contains_pattern(series, pattern):
    return series.str.contains(pattern, regex=True, na=False)


def first_part_of_source(source):
    if pd.isna(source):
        return ""
    return str(source).split(";")[0].strip()


# =============================================================================
# 2. WCZYTANIE I PODSTAWOWE CZYSZCZENIE
# =============================================================================

if not INPUT_CSV.exists():
    raise FileNotFoundError(
        f"Nie znaleziono pliku: {INPUT_CSV.resolve()}\n"
        "Umieść pamiec_metadata.csv w folderze roboczym albo zmień INPUT_CSV."
    )

df = pd.read_csv(INPUT_CSV, dtype=str, encoding="utf-8")

required_columns = [
    "bn_article_id", "title", "creators", "year", "source", "description",
    "language", "publisher", "article_url", "pdf_url", "dc_identifier"
]

missing = [col for col in required_columns if col not in df.columns]
if missing:
    raise ValueError(f"Brakuje wymaganych kolumn w CSV: {missing}")

df["year_num"] = pd.to_numeric(df["year"], errors="coerce")
df["journal"] = df["source"].apply(first_part_of_source)

df["has_description"] = df["description"].fillna("").str.strip().ne("")
df["has_pdf"] = df["pdf_url"].fillna("").str.strip().ne("")

df["filter_text"] = (
    df["title"].fillna("") + " " +
    df["description"].fillna("") + " " +
    df["source"].fillna("") + " " +
    df["journal"].fillna("") + " " +
    df["publisher"].fillna("")
).apply(normalize_text)


# =============================================================================
# 3. REGUŁY POZYTYWNE
# =============================================================================

positive_patterns = {
    "strong_memory_studies": r"\b(badania nad pamięcią|memory studies|pamięć zbiorow[aeyąę]*|collective memory|pamięć kulturow[aeyąę]*|cultural memory|postpamię[ćc]|postmemory|polityk[a-ząćęłńóśźż]* pamięci|politics of memory|memory politics|miejsc[a-ząćęłńóśźż]* pamięci|sites? of memory|lieux de mémoire)\b",

    "historical_social_memory": r"\b(pamięć historyczn[aeyąę]*|historical memory|pamięć społeczn[aeyąę]*|social memory|pamięć narodow[aeyąę]*|national memory|pamięć komunikacyjn[aeyąę]*|communicative memory|pamięć instytucjonaln[aeyąę]*|institutional memory)\b",

    "trauma_testimony_heritage": r"\b(zagład[aeyąę]*|holocaust|shoah|auschwitz|traum[aeyąę]*|trauma|świadectw[a-ząćęłńóśźż]*|testimony|świadek|witness|dziedzictw[a-ząćęłńóśźż]*|heritage|upamiętnian[a-ząćęłńóśźż]*|commemoration|commemorative)\b",

    "literary_context": r"\b(literatur[aeyąę]*|literary|literature|poezj[aeyąę]*|poetry|powieś[ćc]|novel|proza|prose|narracj[aeyąę]*|narrative|autobiografi[aeyąę]*|autobiography|wspomnieni[aeyąę]*|memoir|diary|dziennik|reportaż|reportage)\b",

    "history_archive_museum": r"\b(historiografi[aeyąę]*|historiography|archiw[a-ząćęłńóśźż]*|archive|archival|muze[a-ząćęłńóśźż]*|museum|oral history|historia mówiona|mikrohistori[aeyąę]*|microhistory)\b",

    "identity_context": r"\b(tożsamoś[ćc] zbiorow[aeyąę]*|tożsamoś[ćc] kulturow[aeyąę]*|tożsamoś[ćc] narodow[aeyąę]*|collective identity|cultural identity|national identity)\b",
}

positive_weights = {
    "strong_memory_studies": 5,
    "historical_social_memory": 4,
    "trauma_testimony_heritage": 3,
    "literary_context": 2,
    "history_archive_museum": 2,
    "identity_context": 2,
}

for name, pattern in positive_patterns.items():
    df[f"pos_{name}"] = contains_pattern(df["filter_text"], pattern)


# =============================================================================
# 4. REGUŁY NEGATYWNE
# =============================================================================

negative_patterns = {
    "psychology_health": r"\b(psycholog[a-ząćęłńóśźż]*|psychology|psychological|social psychology|respondents|participants|experiment|experimental|questionnaire|kwestionariusz|scale|skala|test pamięci|memory test|self-esteem|body-esteem|body image|mood adjective|attractiveness|mortality salience|fear of death|lęk przed śmiercią|obraz ciała|nauk o zdrowiu|health sciences|medical science|clinical|klinicz[a-ząćęłńóśźż]*)\b",

    "biomedical_neuro": r"\b(patient|patients|pacjent[a-ząćęłńóśźż]*|disease|chorob[a-ząćęłńóśźż]*|therapy|terapi[a-ząćęłńóśźż]*|treatment|diagnosis|diagnost[a-ząćęłńóśźż]*|brain|mózg|neuro|cognitive test|cognition|poznawcz[a-ząćęłńóśźż]*|hypoxic mice|mice|mouse|rat|rats|immunological memory|immune memory)\b",

    "technical_computer": r"\b(computer memory|pamięć komputerow[aeyąę]*|cache memory|memory allocation|ram|rom|flash memory|semiconductor memory|database memory|shape memory alloy|stopy z pamięcią kształtu|shape-memory|smart materials|materials science|engineering|inżynieri[aeyąę]*)\b",

    "education_testing": r"\b(test wyników|learning outcomes|uczniowie|students' memory|pamięć uczniów|achievement test|school test|didactic experiment|eksperyment dydaktyczny)\b",
}

negative_weights = {
    "psychology_health": 5,
    "biomedical_neuro": 5,
    "technical_computer": 6,
    "education_testing": 3,
}

for name, pattern in negative_patterns.items():
    df[f"neg_{name}"] = contains_pattern(df["filter_text"], pattern)


# =============================================================================
# 5. SCORING
# =============================================================================

df["positive_score"] = 0
for name, weight in positive_weights.items():
    df["positive_score"] += df[f"pos_{name}"].astype(int) * weight

df["negative_score"] = 0
for name, weight in negative_weights.items():
    df["negative_score"] += df[f"neg_{name}"].astype(int) * weight

df["memory_studies_score"] = df["positive_score"] - df["negative_score"]

df["has_strong_memory_studies_signal"] = (
    df["pos_strong_memory_studies"] |
    df["pos_historical_social_memory"]
)


# =============================================================================
# 6. KLASYFIKACJA WARSTW KORPUSU
# =============================================================================

conditions_memory = (
    ((df["positive_score"] >= 4) & (df["negative_score"] <= 3))
    |
    ((df["has_strong_memory_studies_signal"]) & (df["positive_score"] >= 5) & (df["negative_score"] <= 5))
    |
    ((df["positive_score"] >= 5) & (df["negative_score"] == 0))
)

conditions_manual = (
    (df["positive_score"] >= 2) &
    (df["positive_score"] < 4) &
    (df["negative_score"] <= 3)
)

conditions_excluded_non_hum = (
    (df["negative_score"] >= 5) &
    ~conditions_memory
)

df["corpus_layer"] = "excluded_or_unclear"
df.loc[conditions_manual, "corpus_layer"] = "manual_review"
df.loc[conditions_excluded_non_hum, "corpus_layer"] = "excluded_non_humanities"
df.loc[conditions_memory, "corpus_layer"] = "memory_studies_candidate"


# =============================================================================
# 7. ETYKIETY DOMENOWE
# =============================================================================

df["memory_domain"] = "unclear_or_weak_signal"

df.loc[df["neg_technical_computer"], "memory_domain"] = "technical_or_computer_memory"
df.loc[df["neg_biomedical_neuro"], "memory_domain"] = "biomedical_or_neuro_memory"
df.loc[df["neg_psychology_health"], "memory_domain"] = "psychology_or_health_memory"
df.loc[df["neg_education_testing"], "memory_domain"] = "education_or_testing_memory"

df.loc[
    df["pos_literary_context"] & (df["corpus_layer"] == "memory_studies_candidate"),
    "memory_domain"
] = "literary_memory"

df.loc[
    df["pos_history_archive_museum"] & (df["corpus_layer"] == "memory_studies_candidate"),
    "memory_domain"
] = "historical_archival_memory"

df.loc[
    df["pos_trauma_testimony_heritage"] & (df["corpus_layer"] == "memory_studies_candidate"),
    "memory_domain"
] = "trauma_testimony_heritage_memory"

df.loc[
    df["pos_historical_social_memory"] & (df["corpus_layer"] == "memory_studies_candidate"),
    "memory_domain"
] = "historical_social_memory"

df.loc[
    df["pos_strong_memory_studies"] & (df["corpus_layer"] == "memory_studies_candidate"),
    "memory_domain"
] = "memory_studies_core"


# =============================================================================
# 8. POWÓD WŁĄCZENIA / WYKLUCZENIA
# =============================================================================

def build_reason(row):
    pos_hits = [name for name in positive_patterns if row.get(f"pos_{name}", False)]
    neg_hits = [name for name in negative_patterns if row.get(f"neg_{name}", False)]

    parts = []
    if pos_hits:
        parts.append("positive=" + ",".join(pos_hits))
    if neg_hits:
        parts.append("negative=" + ",".join(neg_hits))
    parts.append(f"pos_score={row['positive_score']}")
    parts.append(f"neg_score={row['negative_score']}")
    parts.append(f"net={row['memory_studies_score']}")
    return " | ".join(parts)


df["filter_reason"] = df.apply(build_reason, axis=1)


# =============================================================================
# 9. EKSPORTY
# =============================================================================

front_cols = [
    "bn_article_id", "title", "creators", "year", "year_num", "journal", "source",
    "language", "publisher", "description", "article_url", "pdf_url", "dc_identifier",
    "corpus_layer", "memory_domain", "positive_score", "negative_score",
    "memory_studies_score", "filter_reason", "has_description", "has_pdf"
]

other_cols = [col for col in df.columns if col not in front_cols]
df = df[front_cols + other_cols]

df.to_csv(OUT_ALL_SCORED, index=False, encoding="utf-8-sig")

memory_df = df[df["corpus_layer"] == "memory_studies_candidate"].copy()
memory_df.to_csv(OUT_MEMORY_STUDIES, index=False, encoding="utf-8-sig")

excluded_df = df[
    df["corpus_layer"].isin(["excluded_non_humanities", "excluded_or_unclear"])
].copy()
excluded_df.to_csv(OUT_EXCLUDED, index=False, encoding="utf-8-sig")

review_parts = []
for layer, n in [
    ("manual_review", 200),
    ("memory_studies_candidate", 100),
    ("excluded_non_humanities", 100),
    ("excluded_or_unclear", 100),
]:
    part = df[df["corpus_layer"] == layer].copy()
    if len(part) > 0:
        review_parts.append(part.sample(n=min(n, len(part)), random_state=42))

if review_parts:
    review_df = pd.concat(review_parts, ignore_index=True)
else:
    review_df = pd.DataFrame()

review_df.to_csv(OUT_REVIEW, index=False, encoding="utf-8-sig")

journal_counts = (
    memory_df.groupby("journal", dropna=False)
    .agg(
        records=("bn_article_id", "nunique"),
        first_year=("year_num", "min"),
        last_year=("year_num", "max"),
        with_description=("has_description", "sum"),
        with_pdf=("has_pdf", "sum"),
    )
    .reset_index()
    .sort_values("records", ascending=False)
)
journal_counts.to_csv(OUT_JOURNALS, index=False, encoding="utf-8-sig")

domain_counts = (
    df.groupby(["corpus_layer", "memory_domain"], dropna=False)
    .size()
    .reset_index(name="records")
    .sort_values(["corpus_layer", "records"], ascending=[True, False])
)
domain_counts.to_csv(OUT_DOMAINS, index=False, encoding="utf-8-sig")


# =============================================================================
# 10. RAPORT TEKSTOWY
# =============================================================================

layer_counts = df["corpus_layer"].value_counts(dropna=False)
domain_counts_simple = df["memory_domain"].value_counts(dropna=False)
language_counts = df["language"].value_counts(dropna=False).head(20)

report_lines = []
report_lines.append("RAPORT FILTRACJI KORPUSU 'PAMIĘĆ / MEMORY'")
report_lines.append("=" * 70)
report_lines.append(f"Plik wejściowy: {INPUT_CSV}")
report_lines.append(f"Liczba rekordów wejściowych: {len(df)}")
report_lines.append("")

if df["year_num"].notna().any():
    report_lines.append(f"Zakres lat: {int(df['year_num'].min())}–{int(df['year_num'].max())}")
else:
    report_lines.append("Zakres lat: brak poprawnych danych")

report_lines.append(f"Rekordy z opisem/abstraktem: {int(df['has_description'].sum())}")
report_lines.append(f"Rekordy z PDF: {int(df['has_pdf'].sum())}")
report_lines.append("")

report_lines.append("WARSTWY KORPUSU:")
for layer, count in layer_counts.items():
    report_lines.append(f"- {layer}: {count}")
report_lines.append("")

report_lines.append("DOMENY:")
for domain, count in domain_counts_simple.items():
    report_lines.append(f"- {domain}: {count}")
report_lines.append("")

report_lines.append("JĘZYKI — TOP 20:")
for lang, count in language_counts.items():
    report_lines.append(f"- {lang}: {count}")
report_lines.append("")

report_lines.append("NAJCZĘSTSZE CZASOPISMA W KORPUSIE KANDYDACKIM:")
for _, row in journal_counts.head(30).iterrows():
    first_year = int(row["first_year"]) if pd.notna(row["first_year"]) else "brak"
    last_year = int(row["last_year"]) if pd.notna(row["last_year"]) else "brak"
    report_lines.append(
        f"- {row['journal']}: {row['records']} ({first_year}–{last_year})"
    )
report_lines.append("")

report_lines.append("UWAGA METODOLOGICZNA:")
report_lines.append(
    "Plik pamiec_02_memory_studies_candidate.csv nie jest jeszcze finalnym korpusem "
    "pamięciologicznym. To korpus kandydacki po filtracji regułowej. "
    "Należy sprawdzić próbę pamiec_04_manual_review_sample.csv i na tej podstawie "
    "dostroić wzorce pozytywne oraz negatywne."
)

OUT_REPORT.write_text("\n".join(report_lines), encoding="utf-8")


# =============================================================================
# 11. PODSUMOWANIE W KONSOLI
# =============================================================================

print("Gotowe. Zapisano pliki w folderze:", OUTPUT_DIR.resolve())
print("\nWarstwy korpusu:")
print(layer_counts)
print("\nNajważniejsze pliki:")
print("-", OUT_ALL_SCORED)
print("-", OUT_MEMORY_STUDIES)
print("-", OUT_REVIEW)
print("-", OUT_REPORT)


#%%

# =============================================================================
# ŚCIEŻKI
# =============================================================================

CANDIDATE_CSV = Path("data/bn_pamiec_fast/pamiec_outputs/pamiec_02_memory_studies_candidate.csv")
FULLTEXT_CSV = Path("data/bn_pamiec_fast/pamiec_metadata_fulltext.csv")

OUT_DIR = Path("data/bn_pamiec_fast/pamiec_fulltext_outputs")
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_CANDIDATE_FULLTEXT = OUT_DIR / "pamiec_01_candidate_fulltext.csv"
OUT_REPORT = OUT_DIR / "pamiec_00_fulltext_extraction_report.txt"

CHUNKSIZE = 50_000


# =============================================================================
# 1. WCZYTANIE ID KORPUSU KANDYDACKIEGO
# =============================================================================

candidate = pd.read_csv(CANDIDATE_CSV, dtype=str, encoding="utf-8-sig")

if "bn_article_id" not in candidate.columns:
    raise ValueError("W pliku kandydackim brakuje kolumny bn_article_id.")

candidate_ids = set(candidate["bn_article_id"].dropna().astype(str))

print(f"Liczba ID w korpusie kandydackim: {len(candidate_ids)}")


# =============================================================================
# 2. WCZYTANIE DUŻEGO PLIKU PEŁNOTEKSTOWEGO W CHUNKACH
# =============================================================================

matched_chunks = []
total_rows = 0
matched_rows = 0

reader = pd.read_csv(
    FULLTEXT_CSV,
    dtype=str,
    encoding="utf-8",
    chunksize=CHUNKSIZE,
    low_memory=False
)

for i, chunk in enumerate(reader, start=1):
    if "bn_article_id" not in chunk.columns:
        raise ValueError("W pliku pełnotekstowym brakuje kolumny bn_article_id.")

    total_rows += len(chunk)

    chunk["bn_article_id"] = chunk["bn_article_id"].astype(str)

    matched = chunk[chunk["bn_article_id"].isin(candidate_ids)].copy()

    if len(matched) > 0:
        matched_chunks.append(matched)
        matched_rows += len(matched)

    print(
        f"Chunk {i}: przeczytano {len(chunk)} rekordów, "
        f"dopasowano {len(matched)}"
    )


# =============================================================================
# 3. EKSPORT
# =============================================================================

if matched_chunks:
    fulltext_candidate = pd.concat(matched_chunks, ignore_index=True)
else:
    fulltext_candidate = pd.DataFrame()

fulltext_candidate.to_csv(
    OUT_CANDIDATE_FULLTEXT,
    index=False,
    encoding="utf-8-sig"
)

report = []
report.append("RAPORT EKSTRAKCJI PEŁNYCH TEKSTÓW")
report.append("=" * 70)
report.append(f"Plik kandydacki: {CANDIDATE_CSV}")
report.append(f"Plik pełnotekstowy: {FULLTEXT_CSV}")
report.append(f"Liczba ID w korpusie kandydackim: {len(candidate_ids)}")
report.append(f"Liczba rekordów przeczytanych z pliku pełnotekstowego: {total_rows}")
report.append(f"Liczba dopasowanych rekordów pełnotekstowych: {matched_rows}")
report.append(f"Plik wynikowy: {OUT_CANDIDATE_FULLTEXT}")

OUT_REPORT.write_text("\n".join(report), encoding="utf-8")

print("\nGotowe.")
print("Zapisano:", OUT_CANDIDATE_FULLTEXT)
print("Dopasowane rekordy:", matched_rows)


#%%
INPUT = Path("data/bn_pamiec_fast/pamiec_fulltext_outputs/pamiec_01_candidate_fulltext.csv")

OUT_DIR = Path("data/bn_pamiec_fast/pamiec_fulltext_outputs")
OUT_CLEAN = OUT_DIR / "pamiec_02_candidate_fulltext_clean.csv"
OUT_REPORT = OUT_DIR / "pamiec_02_fulltext_clean_report.txt"

TEXT_COLUMN = "full_text"  # zmień, jeśli kolumna nazywa się inaczej


def clean_text(text):
    if pd.isna(text):
        return ""

    text = str(text)

    # Usunięcie nadmiarowych białych znaków
    text = re.sub(r"\s+", " ", text)

    # Usunięcie bardzo częstych artefaktów technicznych
    text = re.sub(r"Downloaded from.*?(?=\.|\n)", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"Copyright.*?(?=\.|\n)", " ", text, flags=re.IGNORECASE)

    # Porządkowanie spacji
    text = text.strip()

    return text


df = pd.read_csv(INPUT, dtype=str, encoding="utf-8-sig", low_memory=False)

if TEXT_COLUMN not in df.columns:
    raise ValueError(
        f"Nie znaleziono kolumny {TEXT_COLUMN}. "
        f"Dostępne kolumny: {df.columns.tolist()}"
    )

df["fulltext_clean"] = df[TEXT_COLUMN].apply(clean_text)
df["fulltext_chars"] = df["fulltext_clean"].str.len()
df["fulltext_words"] = df["fulltext_clean"].str.split().str.len()

# Odrzucamy rekordy bez realnego tekstu.
df["has_real_fulltext"] = df["fulltext_words"] >= 300

df.to_csv(OUT_CLEAN, index=False, encoding="utf-8-sig")

report = []
report.append("RAPORT CZYSZCZENIA PEŁNYCH TEKSTÓW")
report.append("=" * 70)
report.append(f"Liczba rekordów: {len(df)}")
report.append(f"Z tekstem >= 300 słów: {int(df['has_real_fulltext'].sum())}")
report.append("")
report.append("Długość tekstów — liczba słów:")
report.append(str(df["fulltext_words"].describe()))

OUT_REPORT.write_text("\n".join(report), encoding="utf-8")

print("Gotowe.")
print("Zapisano:", OUT_CLEAN)
print(df["fulltext_words"].describe())
























