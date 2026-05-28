import re
import time
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Dict, List

import requests
import pandas as pd
from lxml import etree
from tqdm import tqdm
import fitz  # PyMuPDF


# ============================================================
# KONFIGURACJA
# ============================================================

BASE_OAI_URL = "https://bibliotekanauki.pl/api/oai/articles"
ARTICLE_BASE_URL = "https://bibliotekanauki.pl/articles"

OUTPUT_DIR = Path("data/bn_pamiec_fast")
OUTPUT_DIR.mkdir(exist_ok=True)

METADATA_CSV = OUTPUT_DIR / "pamiec_metadata.csv"
FULLTEXT_CSV = OUTPUT_DIR / "pamiec_metadata_fulltext.csv"
LOG_FILE = OUTPUT_DIR / "bn_pamiec_fast.log"

# Do testu ustaw np. 10.
# Do pełnego harvestingu ustaw None.
MAX_OAI_PAGES = None

# Równoległe pobieranie PDF-ów.
# Rozsądny zakres: 4–10.
MAX_WORKERS = 8

OAI_TIMEOUT = 60
PDF_TIMEOUT = 90

# Minimalna pauza między stronami OAI.
OAI_SLEEP = 0.05

# Czy zapisywać PDF-y lokalnie?
# Do eksperymentu zwykle nie trzeba.
SAVE_PDFS = False
PDF_DIR = OUTPUT_DIR / "pdf"

if SAVE_PDFS:
    PDF_DIR.mkdir(exist_ok=True)

HEADERS = {
    "User-Agent": (
        "IBL-PAN-memory-studies-harvester/0.3 "
        "(research use; contact: wpisz_tu_swoj_email@example.org)"
    )
}

# Wzorce wyszukiwania w metadanych oai_dc.
# To jest korpus kandydacki, nie finalna selekcja dyscyplinarna.
MEMORY_PATTERNS = [
    r"\bpamięć\b",
    r"\bpamięci\b",
    r"\bpamięcią\b",
    r"\bpamięciami\b",
    r"\bpamięciowy\b",
    r"\bpamięciowa\b",
    r"\bpamięciowe\b",
    r"\bpamięciowego\b",
    r"\bpamięciowej\b",
    r"\bpamięciowych\b",
    r"\bpamiętanie\b",
    r"\bpamiętania\b",
    r"\bpamiętaniu\b",
    r"\bpamiętać\b",
    r"\bpamięta\b",
    r"\bpamiętają\b",
    r"\bpostpamięć\b",
    r"\bpostpamięci\b",
    r"\bmiejsce pamięci\b",
    r"\bmiejsca pamięci\b",
    r"\bmiejsc pamięci\b",
    r"\bpolityka pamięci\b",
    r"\bpolityki pamięci\b",
    r"\bpamięć zbiorowa\b",
    r"\bpamięci zbiorowej\b",
    r"\bpamięć kulturowa\b",
    r"\bpamięci kulturowej\b",
    r"\bmemory\b",
    r"\bmemories\b",
    r"\bremembering\b",
    r"\bremembrance\b",
]

MEMORY_REGEX = re.compile("|".join(MEMORY_PATTERNS), flags=re.IGNORECASE)


logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)


# ============================================================
# SESJA HTTP
# ============================================================

def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)

    adapter = requests.adapters.HTTPAdapter(
        pool_connections=MAX_WORKERS + 4,
        pool_maxsize=MAX_WORKERS + 4,
        max_retries=2
    )

    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session


SESSION = make_session()


# ============================================================
# FUNKCJE POMOCNICZE
# ============================================================

def request_get(
    url: str,
    params: Optional[dict] = None,
    timeout: int = 60
) -> Optional[requests.Response]:
    """
    GET z prostym retry/backoff.
    """
    for attempt in range(1, 4):
        try:
            response = SESSION.get(url, params=params, timeout=timeout)

            if response.status_code == 200:
                return response

            logging.warning(
                "HTTP %s for %s params=%s attempt=%s",
                response.status_code,
                url,
                params,
                attempt
            )

            if response.status_code in [429, 500, 502, 503, 504]:
                time.sleep(2 * attempt)
                continue

            return response

        except requests.RequestException as exc:
            logging.warning("Request error: %s attempt=%s", exc, attempt)
            time.sleep(2 * attempt)

    return None


def parse_xml(content: bytes):
    parser = etree.XMLParser(recover=True, huge_tree=True)
    return etree.fromstring(content, parser=parser)


def clean_text(text: Optional[str]) -> Optional[str]:
    if text is None:
        return None

    text = str(text)
    text = re.sub(r"\s+", " ", text).strip()

    return text or None


def join_values(values: List[Optional[str]]) -> Optional[str]:
    cleaned = []

    for value in values:
        value = clean_text(value)
        if value:
            cleaned.append(value)

    return " | ".join(cleaned) if cleaned else None


def split_keywords(value: Optional[str]) -> Optional[str]:
    """
    Normalizuje dc:subject do postaci słów kluczowych rozdzielonych separatorem ' | '.

    W oai_dc słowa kluczowe zwykle siedzą w dc:subject.
    Nie próbujemy tu rozstrzygać, czy subject jest hasłem tematycznym,
    słowem kluczowym czy klasyfikacją. Zachowujemy pełną informację.
    """
    if not value:
        return None

    parts = re.split(r"\s*\|\s*|\s*;\s*", str(value))
    parts = [clean_text(p) for p in parts]
    parts = [p for p in parts if p]

    # usunięcie duplikatów z zachowaniem kolejności
    seen = set()
    unique = []
    for p in parts:
        key = p.lower()
        if key not in seen:
            seen.add(key)
            unique.append(p)

    return " | ".join(unique) if unique else None


def extract_bn_article_id(identifier: Optional[str]) -> Optional[str]:
    """
    Z identyfikatora typu:
    oai:bibliotekanauki.pl:1968869
    wyciąga 1968869.
    """
    if not identifier:
        return None

    match = re.search(r"bibliotekanauki\.pl:(\d+)", identifier)
    if match:
        return match.group(1)

    match = re.search(r":(\d+)$", identifier)
    if match:
        return match.group(1)

    return None


def year_from_text(value: Optional[str]) -> Optional[int]:
    """
    Próbuje wydobyć rok z pola date/source/description.
    """
    if not value:
        return None

    match = re.search(r"\b(19|20)\d{2}\b", str(value))
    if match:
        return int(match.group(0))

    return None


def pdf_url_from_id(article_id: Optional[str]) -> Optional[str]:
    if not article_id:
        return None

    return f"{ARTICLE_BASE_URL}/{article_id}.pdf"


def contains_memory_terms(record: Dict) -> bool:
    """
    Filtr pamięciowy na podstawie metadanych oai_dc.
    """
    fields = [
        record.get("title"),
        record.get("creators"),
        record.get("subject"),
        record.get("keywords"),
        record.get("description"),
        record.get("source"),
    ]

    haystack = " ".join(str(x) for x in fields if x)

    return bool(MEMORY_REGEX.search(haystack))


def safe_filename(value: Optional[str], max_len: int = 120) -> str:
    value = clean_text(value) or "unknown"
    value = re.sub(r"[^\w\-.]+", "_", value, flags=re.UNICODE)
    value = value.strip("_")
    return value[:max_len] or "unknown"


# ============================================================
# PARSOWANIE OAI_DC
# ============================================================

def parse_oai_dc_record(record_node) -> Optional[Dict]:
    ns = {
        "oai": "http://www.openarchives.org/OAI/2.0/",
        "dc": "http://purl.org/dc/elements/1.1/",
    }

    header = record_node.find("oai:header", namespaces=ns)
    metadata = record_node.find("oai:metadata", namespaces=ns)

    if header is None:
        return None

    identifier_node = header.find("oai:identifier", namespaces=ns)
    datestamp_node = header.find("oai:datestamp", namespaces=ns)

    oai_identifier = (
        clean_text(" ".join(identifier_node.itertext()))
        if identifier_node is not None
        else None
    )

    datestamp = (
        clean_text(" ".join(datestamp_node.itertext()))
        if datestamp_node is not None
        else None
    )

    if header.get("status") == "deleted" or metadata is None:
        return None

    def dc_values(tag: str) -> List[str]:
        values = []

        for el in metadata.findall(f".//dc:{tag}", namespaces=ns):
            txt = clean_text(" ".join(el.itertext()))
            if txt:
                values.append(txt)

        return values

    article_id = extract_bn_article_id(oai_identifier)

    title = join_values(dc_values("title"))
    creators = join_values(dc_values("creator"))
    subject = join_values(dc_values("subject"))

    # Ważne: osobna kolumna keywords.
    # W oai_dc traktujemy dc:subject jako źródło keywords.
    keywords = split_keywords(subject)

    description = join_values(dc_values("description"))
    date = join_values(dc_values("date"))
    source = join_values(dc_values("source"))
    publisher = join_values(dc_values("publisher"))
    language = join_values(dc_values("language"))
    dc_identifier = join_values(dc_values("identifier"))
    rights = join_values(dc_values("rights"))
    relation = join_values(dc_values("relation"))
    type_ = join_values(dc_values("type"))
    format_ = join_values(dc_values("format"))
    coverage = join_values(dc_values("coverage"))
    contributor = join_values(dc_values("contributor"))

    year = (
        year_from_text(date)
        or year_from_text(source)
        or year_from_text(description)
    )

    return {
        "bn_article_id": article_id,
        "oai_identifier": oai_identifier,
        "datestamp": datestamp,

        "title": title,
        "creators": creators,
        "contributor": contributor,

        "subject": subject,
        "keywords": keywords,
        "description": description,

        "date": date,
        "year": year,
        "source": source,
        "publisher": publisher,
        "language": language,

        "type": type_,
        "format": format_,
        "coverage": coverage,
        "rights": rights,

        "dc_identifier": dc_identifier,
        "relation": relation,

        "article_url": f"{ARTICLE_BASE_URL}/{article_id}" if article_id else None,
        "pdf_url": pdf_url_from_id(article_id),
    }


# ============================================================
# HARVEST METADANYCH TYLKO DLA „PAMIĘCI”
# ============================================================

def harvest_memory_metadata() -> pd.DataFrame:
    """
    Przechodzi po OAI-PMH, ale zapisuje tylko rekordy pasujące
    do korpusu pamięci. Nie zapisuje wszystkich rekordów i nie
    zapisuje surowych XML-i.
    """
    memory_records = []

    params = {
        "verb": "ListRecords",
        "metadataPrefix": "oai_dc",
    }

    resumption_token = None
    page_no = 0

    pbar = tqdm(desc="OAI-PMH strony", unit="strona")

    while True:
        page_no += 1

        if MAX_OAI_PAGES is not None and page_no > MAX_OAI_PAGES:
            break

        if resumption_token:
            params = {
                "verb": "ListRecords",
                "resumptionToken": resumption_token,
            }

        response = request_get(
            BASE_OAI_URL,
            params=params,
            timeout=OAI_TIMEOUT
        )

        if response is None:
            raise RuntimeError("Nie udało się pobrać odpowiedzi z OAI-PMH.")

        if response.status_code != 200:
            raise RuntimeError(
                f"OAI-PMH HTTP {response.status_code}: {response.text[:500]}"
            )

        root = parse_xml(response.content)

        ns = {
            "oai": "http://www.openarchives.org/OAI/2.0/",
        }

        errors = root.findall(".//oai:error", namespaces=ns)
        if errors:
            error_text = " | ".join(
                clean_text(" ".join(e.itertext())) or ""
                for e in errors
            )
            raise RuntimeError(f"OAI-PMH error: {error_text}")

        record_nodes = root.findall(".//oai:record", namespaces=ns)

        new_hits = 0

        for record_node in record_nodes:
            record = parse_oai_dc_record(record_node)

            if not record:
                continue

            if contains_memory_terms(record):
                memory_records.append(record)
                new_hits += 1

        token_node = root.find(".//oai:resumptionToken", namespaces=ns)
        resumption_token = (
            clean_text(" ".join(token_node.itertext()))
            if token_node is not None
            else None
        )

        pbar.set_postfix(
            hits=len(memory_records),
            last_page_hits=new_hits
        )
        pbar.update(1)

        if not resumption_token:
            break

        if OAI_SLEEP:
            time.sleep(OAI_SLEEP)

    pbar.close()

    df = pd.DataFrame(memory_records)

    if not df.empty:
        df = df.drop_duplicates(subset=["bn_article_id", "oai_identifier"])

        preferred_columns = [
            "bn_article_id",
            "title",
            "creators",
            "contributor",
            "year",
            "date",
            "source",
            "subject",
            "keywords",
            "description",
            "language",
            "publisher",
            "type",
            "format",
            "coverage",
            "article_url",
            "pdf_url",
            "rights",
            "dc_identifier",
            "relation",
            "oai_identifier",
            "datestamp",
        ]

        existing = [c for c in preferred_columns if c in df.columns]
        remaining = [c for c in df.columns if c not in existing]
        df = df[existing + remaining]

        df.to_csv(METADATA_CSV, index=False, encoding="utf-8-sig")

    print(f"Znaleziono rekordów pamięciowych: {len(df)}")
    print(f"Zapisano metadane: {METADATA_CSV}")

    return df


# ============================================================
# POBIERANIE PDF I EKSTRAKCJA TEKSTU
# ============================================================

def extract_text_from_pdf_bytes(pdf_bytes: bytes) -> Dict:
    """
    Zwraca tekst z PDF-a oraz proste statystyki.
    """
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")

        pages_text = []

        for page in doc:
            text = page.get_text("text")
            if text:
                pages_text.append(text)

        full_text = "\n\n".join(pages_text)
        full_text = re.sub(r"[ \t]+", " ", full_text)
        full_text = re.sub(r"\n{3,}", "\n\n", full_text).strip()

        return {
            "full_text": full_text,
            "pdf_pages": doc.page_count,
            "full_text_chars": len(full_text),
            "full_text_words_approx": len(
                re.findall(r"\w+", full_text, flags=re.UNICODE)
            ),
            "pdf_status": "ok",
            "pdf_error": None,
        }

    except Exception as exc:
        return {
            "full_text": None,
            "pdf_pages": None,
            "full_text_chars": 0,
            "full_text_words_approx": 0,
            "pdf_status": "extract_error",
            "pdf_error": str(exc),
        }


def download_and_extract_pdf(row: Dict) -> Dict:
    """
    Pobiera PDF dla jednego rekordu i wyciąga tekst.
    """
    article_id = row.get("bn_article_id")
    pdf_url = row.get("pdf_url")

    result = dict(row)

    if not article_id or not pdf_url:
        result.update({
            "full_text": None,
            "pdf_pages": None,
            "full_text_chars": 0,
            "full_text_words_approx": 0,
            "pdf_status": "missing_pdf_url",
            "pdf_http_status": None,
            "pdf_error": None,
            "local_pdf_path": None,
        })
        return result

    response = request_get(pdf_url, timeout=PDF_TIMEOUT)

    if response is None:
        result.update({
            "full_text": None,
            "pdf_pages": None,
            "full_text_chars": 0,
            "full_text_words_approx": 0,
            "pdf_status": "request_failed",
            "pdf_http_status": None,
            "pdf_error": None,
            "local_pdf_path": None,
        })
        return result

    if response.status_code != 200:
        result.update({
            "full_text": None,
            "pdf_pages": None,
            "full_text_chars": 0,
            "full_text_words_approx": 0,
            "pdf_status": "http_error",
            "pdf_http_status": response.status_code,
            "pdf_error": None,
            "local_pdf_path": None,
        })
        return result

    pdf_bytes = response.content
    local_pdf_path = None

    if SAVE_PDFS:
        title_part = safe_filename(row.get("title"), max_len=80)
        pdf_path = PDF_DIR / f"{article_id}_{title_part}.pdf"
        pdf_path.write_bytes(pdf_bytes)
        local_pdf_path = str(pdf_path)

    extracted = extract_text_from_pdf_bytes(pdf_bytes)

    result.update(extracted)
    result["pdf_http_status"] = response.status_code
    result["local_pdf_path"] = local_pdf_path

    return result


def build_fulltext_corpus(metadata_df: pd.DataFrame) -> pd.DataFrame:
    """
    Równolegle pobiera PDF-y i buduje jeden plik z metadanymi oraz tekstem.
    """
    if metadata_df.empty:
        print("Brak metadanych do pobrania pełnych tekstów.")
        return pd.DataFrame()

    records = metadata_df.to_dict(orient="records")
    results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(download_and_extract_pdf, row)
            for row in records
        ]

        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="PDF + tekst"
        ):
            try:
                results.append(future.result())
            except Exception as exc:
                logging.exception("Unexpected error in worker: %s", exc)

    df = pd.DataFrame(results)

    preferred_columns = [
        "bn_article_id",
        "title",
        "creators",
        "contributor",
        "year",
        "date",
        "source",

        "subject",
        "keywords",
        "description",

        "language",
        "publisher",
        "type",
        "format",
        "coverage",

        "article_url",
        "pdf_url",
        "pdf_status",
        "pdf_http_status",
        "pdf_pages",
        "full_text_chars",
        "full_text_words_approx",
        "full_text",

        "rights",
        "dc_identifier",
        "relation",
        "oai_identifier",
        "datestamp",
        "local_pdf_path",
        "pdf_error",
    ]

    existing = [c for c in preferred_columns if c in df.columns]
    remaining = [c for c in df.columns if c not in existing]
    df = df[existing + remaining]

    df.to_csv(FULLTEXT_CSV, index=False, encoding="utf-8-sig")

    print(f"Zapisano korpus z pełnymi tekstami: {FULLTEXT_CSV}")

    return df


# ============================================================
# PODSUMOWANIE
# ============================================================

def print_summary(df: pd.DataFrame):
    if df.empty:
        print("Brak wyników.")
        return

    print("\n=== PODSUMOWANIE ===")
    print(f"Liczba rekordów: {len(df)}")

    if "pdf_status" in df.columns:
        print("\nStatus PDF:")
        print(df["pdf_status"].value_counts(dropna=False).to_string())

    if "full_text_chars" in df.columns:
        with_text = df[df["full_text_chars"].fillna(0) > 1000]
        print(f"\nRekordy z tekstem > 1000 znaków: {len(with_text)}")

    if "year" in df.columns:
        years = pd.to_numeric(df["year"], errors="coerce").dropna()
        if not years.empty:
            print(f"Zakres lat: {int(years.min())}–{int(years.max())}")

    if "source" in df.columns:
        print("\nTop 15 źródeł:")
        print(df["source"].dropna().value_counts().head(15).to_string())

    if "keywords" in df.columns:
        all_keywords = []

        for value in df["keywords"].dropna():
            parts = re.split(r"\s*\|\s*", str(value))
            all_keywords.extend([p.strip() for p in parts if p.strip()])

        if all_keywords:
            kw_counts = pd.Series(all_keywords).value_counts().head(20)

            print("\nTop 20 keywords / dc:subject:")
            print(kw_counts.to_string())


# ============================================================
# MAIN
# ============================================================

def main():
    print("Start: Biblioteka Nauki — szybki korpus 'pamięć'")

    if METADATA_CSV.exists():
        print(f"Wczytuję istniejące metadane: {METADATA_CSV}")
        metadata_df = pd.read_csv(METADATA_CSV)

        # Naprawa kompatybilności dla starszej wersji pliku.
        if "keywords" not in metadata_df.columns and "subject" in metadata_df.columns:
            metadata_df["keywords"] = metadata_df["subject"].apply(split_keywords)
            metadata_df.to_csv(METADATA_CSV, index=False, encoding="utf-8-sig")
            print("Dodano brakującą kolumnę keywords do istniejącego pliku metadanych.")

    else:
        metadata_df = harvest_memory_metadata()

    fulltext_df = build_fulltext_corpus(metadata_df)
    print_summary(fulltext_df)

    print("\nGotowe.")


if __name__ == "__main__":
    main()


























