"""FASCA / Neus Crous Costa: harvest or enrich a GoTriple corpus.

Python >= 3.10; pandas >= 2.1 and openpyxl are needed for table I/O.
Run --help. Importing this module never initiates network requests.
Publication affiliations and current author profiles are kept separate.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import os
import re
import signal
import sqlite3
import sys
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from itertools import product
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

BASE_API_URL = "https://api.gotriple.eu/api/"
keywords_1 = ['government', 'state', 'public policy', 'public funding', 'ministry of culture', 'municipality', 'public administration']
keywords_2 = ['art', 'culture', 'heritage', 'museum', 'memorial', 'cinema', 'music', 'literature', 'festival', 'creative']
keywords_3 = ['value', 'identity', 'ideology', 'propaganda', 'soft power', 'social cohesion', 'patriotism', 'social change']
LOG = logging.getLogger("fasca")


class APIError(RuntimeError):
    """Transport/contract failure, never equivalent to zero search results."""


class APIWindowError(APIError):
    """The service cannot return the requested search window."""


def now():
    return datetime.now(timezone.utc).isoformat()


def as_list(value):
    return value if isinstance(value, list) else ([] if value is None or value == "" else [value])


def labels(value, language=None):
    out = []
    for item in as_list(value):
        if isinstance(item, dict):
            lang = item.get("lang", item.get("language", item.get("@language", "")))
            if language and not set(as_list(lang)).intersection({language, "eng" if language == "en" else language}):
                continue
            text = item.get("text", item.get("@value", item.get("name", item.get("label", ""))))
        else:
            if language:
                continue
            text = item
        if isinstance(text, (str, int, float)) and str(text).strip():
            out.append(str(text).strip())
    return list(dict.fromkeys(out))


def joined(value):
    return "; ".join(labels(value))


def first(rec, *keys):
    return next((rec[k] for k in keys if rec.get(k) not in (None, "", [])), None)


def build_api_url(params):
    cleaned = {}
    for key, value in params.items():
        if value is None or value == "" or value == {}:
            continue
        if isinstance(value, dict):
            value = ";".join(f"{k}={v}" for k, v in value.items())
        if isinstance(value, bool):
            value = str(value).lower()
        cleaned[key] = value
    return urlencode(cleaned)


def queries(quote_phrases=False):
    def term(s):
        return '"' + s + '"' if quote_phrases and ' ' in s else s
    return [" AND ".join(map(term, combo)) for combo in product(keywords_1, keywords_2, keywords_3)]


class Checkpoint:
    """Transactional state: successful pages and completed operations survive a stop."""
    def __init__(self, path, configuration):
        self.path = Path(path)
        self.db = sqlite3.connect(self.path)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS records (
                kind TEXT, id TEXT, payload TEXT NOT NULL, PRIMARY KEY(kind, id));
            CREATE TABLE IF NOT EXISTS jobs (
                key TEXT PRIMARY KEY, kind TEXT, query TEXT, status TEXT,
                next_page INTEGER, count INTEGER, total INTEGER);
            CREATE TABLE IF NOT EXISTS links (
                kind TEXT, id TEXT, job_key TEXT, PRIMARY KEY(kind, id, job_key));
            CREATE TABLE IF NOT EXISTS refreshed (kind TEXT, id TEXT, PRIMARY KEY(kind, id));
            CREATE TABLE IF NOT EXISTS normalized (
                kind TEXT, id TEXT, payload TEXT NOT NULL, PRIMARY KEY(kind, id));
            CREATE INDEX IF NOT EXISTS links_job ON links(job_key);
        """)
        columns = {r[1] for r in self.db.execute("PRAGMA table_info(jobs)")}
        with self.db:
            if "sort" not in columns:
                self.db.execute("ALTER TABLE jobs ADD COLUMN sort TEXT NOT NULL DEFAULT 'name:desc'")
            if "error" not in columns:
                self.db.execute("ALTER TABLE jobs ADD COLUMN error TEXT NOT NULL DEFAULT ''")
        config = json.dumps(configuration, sort_keys=True, ensure_ascii=False)
        previous = self.db.execute("SELECT value FROM settings WHERE key='configuration'").fetchone()
        if previous and previous[0] != config:
            self.db.close()
            raise ValueError("Checkpoint configuration differs. Use the original settings or a new --output directory.")
        with self.db:
            self.db.execute("INSERT OR IGNORE INTO settings VALUES ('configuration', ?)", (config,))

    @staticmethod
    def job_key(kind, query):
        return hashlib.sha256(json.dumps([kind, query]).encode()).hexdigest()

    def query_state(self, kind, query):
        key = self.job_key(kind, query)
        with self.db:
            self.db.execute("INSERT OR IGNORE INTO jobs (key,kind,query,status,next_page,count,total) VALUES (?, ?, ?, 'pending', 1, 0, NULL)", (key, kind, query))
        row = dict(self.db.execute("SELECT * FROM jobs WHERE key=?", (key,)).fetchone())
        seen = {r[0] for r in self.db.execute("SELECT id FROM links WHERE job_key=?", (key,))}
        return row, seen

    def save_page(self, state, batch, next_page, count, total, completed):
        # Page data, query links and the restart cursor are one durable transaction.
        with self.db:
            for rec in batch:
                rid = record_id(rec)
                raw = {**rec, "id": rid, "resource_kind": state["kind"]}
                self.db.execute("INSERT OR IGNORE INTO records VALUES (?, ?, ?)",
                                (state["kind"], rid, json.dumps(raw, ensure_ascii=False)))
                self.db.execute("INSERT OR IGNORE INTO links VALUES (?, ?, ?)", (state["kind"], rid, state["key"]))
            self.db.execute("UPDATE jobs SET status=?, next_page=?, count=?, total=?, sort=?, error='' WHERE key=?",
                            ("completed" if completed else "in_progress", next_page, count, total, state.get("sort", "name:desc"), state["key"]))

    def reverse_query(self, state):
        with self.db:
            self.db.execute("UPDATE jobs SET sort='name:asc', next_page=1, status='in_progress', error='' WHERE key=?", (state["key"],))
        state["sort"] = "name:asc"

    def mark_partial(self, state, reason, total):
        with self.db:
            self.db.execute("UPDATE jobs SET status='partial', error=?, total=COALESCE(?,total) WHERE key=?",
                            (str(reason), total, state["key"]))

    def was_refreshed(self, kind, rid):
        return self.db.execute("SELECT 1 FROM refreshed WHERE kind=? AND id=?", (kind, rid)).fetchone() is not None

    def get_record(self, kind, rid):
        row = self.db.execute("SELECT payload FROM records WHERE kind=? AND id=?", (kind, rid)).fetchone()
        return json.loads(row[0]) if row else None

    def save_record(self, rec):
        kind, rid = rec["resource_kind"], record_id(rec)
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO records VALUES (?, ?, ?)", (kind, rid, json.dumps(rec, ensure_ascii=False)))
            self.db.execute("INSERT OR IGNORE INTO refreshed VALUES (?, ?)", (kind, rid))

    def records(self):
        query_map = {}
        for r in self.db.execute("SELECT l.kind, l.id, j.query FROM links l JOIN jobs j ON j.key=l.job_key ORDER BY j.rowid"):
            query_map.setdefault((r[0], r[1]), []).append(r[2])
        result = []
        for r in self.db.execute("SELECT kind, id, payload FROM records ORDER BY rowid"):
            rec = json.loads(r[2])
            rec["_queries"] = list(dict.fromkeys(as_list(rec.get("_queries")) + query_map.get((r[0], r[1]), [])))
            result.append(rec)
        return result

    def saved_tables(self, kind, rid):
        row = self.db.execute("SELECT payload FROM normalized WHERE kind=? AND id=?", (kind, rid)).fetchone()
        return json.loads(row[0]) if row else None

    def save_tables(self, rec, tables):
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO normalized VALUES (?, ?, ?)",
                            (rec["resource_kind"], record_id(rec), json.dumps(tables, ensure_ascii=False)))

    def progress(self):
        return [dict(r) for r in self.db.execute("""
            SELECT j.kind, j.query, j.status, j.next_page, j.count, j.total,
                   COUNT(l.id) AS unique_count,
                   MAX(j.count - COUNT(l.id), 0) AS duplicate_hits, j.sort, j.error
            FROM jobs j LEFT JOIN links l ON l.job_key=j.key
            GROUP BY j.key ORDER BY j.rowid
        """)]

    def counts(self):
        return {"queries_completed": self.db.execute("SELECT COUNT(*) FROM jobs WHERE status='completed'").fetchone()[0],
                "queries_partial": self.db.execute("SELECT COUNT(*) FROM jobs WHERE status='partial'").fetchone()[0],
                "queries_finished": self.db.execute("SELECT COUNT(*) FROM jobs WHERE status IN ('completed','partial')").fetchone()[0],
                "queries_in_progress": self.db.execute("SELECT COUNT(*) FROM jobs WHERE status='in_progress'").fetchone()[0],
                "records_refreshed": self.db.execute("SELECT COUNT(*) FROM refreshed").fetchone()[0],
                "records_normalized": self.db.execute("SELECT COUNT(*) FROM normalized").fetchone()[0]}

    def close(self):
        self.db.close()


class Client:
    """Bounded retries, request spacing, successful-response cache and audit."""
    def __init__(self, cache, *, timeout=30, attempts=3, delay=0.3, refresh=False, mailto=""):
        self.cache = Path(cache)
        self.cache.mkdir(parents=True, exist_ok=True)
        self.timeout, self.attempts, self.delay, self.refresh = timeout, attempts, delay, refresh
        self.user_agent = "IBL-FASCA-CrousCosta/2.0" + (f" (mailto:{mailto})" if mailto else "")
        self.audit, self.last_request = [], 0.0

    def get(self, url, *, use_cache=True):
        path = self.cache / (hashlib.sha256(url.encode()).hexdigest() + ".json")
        if use_cache and not self.refresh and path.exists():
            saved = json.loads(path.read_text(encoding="utf-8"))
            if saved.get("url") != url:
                raise APIError("Cache URL mismatch")
            self.audit.append({"url": url, "time": now(), "cached": True, "retrieved_at": saved["retrieved_at"]})
            return saved["payload"]
        for attempt in range(self.attempts):
            time.sleep(max(0, self.delay - (time.monotonic() - self.last_request)))
            self.last_request = time.monotonic()
            retry_after = None
            try:
                req = Request(url, headers={"Accept": "application/json", "User-Agent": self.user_agent})
                with urlopen(req, timeout=self.timeout) as response:
                    content_type = response.headers.get("Content-Type", "")
                    body = response.read()
                    if "json" not in content_type.lower():
                        raise APIError(f"Non-JSON response ({content_type}) from {url}; possible service/protection page")
                    try:
                        payload = json.loads(body)
                    except (ValueError, UnicodeError) as exc:
                        raise APIError(f"Invalid JSON from {url}") from exc
                    if isinstance(payload, dict) and (payload.get("error") or payload.get("@type") == "hydra:Error"):
                        raise APIError(f"API error envelope from {url}")
                saved = {"url": url, "retrieved_at": now(), "payload": payload}
                tmp = path.with_suffix(".tmp")
                tmp.write_text(json.dumps(saved, ensure_ascii=False), encoding="utf-8")
                tmp.replace(path)
                self.audit.append({"url": url, "time": saved["retrieved_at"], "status": 200, "cached": False})
                return payload
            except HTTPError as exc:
                self.audit.append({"url": url, "time": now(), "status": exc.code, "attempt": attempt + 1})
                message = f"HTTP {exc.code} from {url}"
                if exc.code == 404:
                    return None
                if exc.code not in {408, 429, 500, 502, 503, 504}:
                    raise APIError(message) from exc
                raw = exc.headers.get("Retry-After")
                if raw:
                    try:
                        retry_after = float(raw)
                    except ValueError:
                        try:
                            retry_after = (parsedate_to_datetime(raw) - datetime.now(timezone.utc)).total_seconds()
                        except (ValueError, TypeError):
                            pass
            except (URLError, TimeoutError, OSError) as exc:
                message = f"Network failure from {url}: {exc}"
                self.audit.append({"url": url, "time": now(), "error": str(exc), "attempt": attempt + 1})
            if attempt + 1 < self.attempts:
                wait = max(0, retry_after) if retry_after is not None else min(2 ** attempt, 10)
                if wait > 60:
                    raise APIError(f"{message}; Retry-After={wait:g}s. Retry later.")
                LOG.warning("%s; retry in %.1fs", message, wait)
                time.sleep(wait)
        raise APIError(message)


def unpack_search(payload):
    if not isinstance(payload, dict):
        raise APIError("Search response is not an object")
    total = None
    if isinstance(payload.get("data"), list):
        records = payload["data"]
        meta = payload.get("meta") or payload.get("pagination") or {}
        for source in (meta, meta.get("pagination", {}) if isinstance(meta, dict) else {}, payload):
            if isinstance(source, dict):
                total = first(source, "total", "totalItems", "total_items", "total_results")
                if total is not None:
                    break
    elif isinstance(payload.get("hydra:member"), list):
        records, total = payload["hydra:member"], payload.get("hydra:totalItems")
    elif isinstance(payload.get("hits"), dict) and isinstance(payload["hits"].get("hits"), list):
        records = [hit.get("_source") for hit in payload["hits"]["hits"]]
        total = payload["hits"].get("total")
        if isinstance(total, dict):
            total = total.get("value") if total.get("relation", "eq") == "eq" else None
    else:
        raise APIError(f"Unknown GoTriple search envelope: {list(payload)[:15]}")
    if any(not isinstance(r, dict) for r in records):
        raise APIError("Search records must be objects")
    return records, int(total) if total is not None else None


def record_id(rec):
    value = first(rec, "id", "@id")
    if value is None:
        raise APIError("A GoTriple record has no ID; refusing unsafe deduplication")
    value = str(value)
    for prefix in ("/documents/", "/projects/", "/authors/", "/api/documents/", "/api/projects/", "/api/authors/"):
        if value.startswith(prefix):
            return value[len(prefix):]
    return value


def unpack_detail(payload):
    if payload is None:
        return None
    if not isinstance(payload, dict):
        raise APIError("Detail response is not an object")
    if "data" in payload:
        data = payload["data"]
        if isinstance(data, list) and len(data) == 1:
            data = data[0]
        if not isinstance(data, dict):
            raise APIError("Unexpected detail data envelope")
        payload = data
    record_id(payload)
    return payload


def search_page(client, url, page, *, use_cache=True):
    payload = client.get(url) if use_cache else client.get(url, use_cache=False)
    meta = payload.get("meta", {}) if isinstance(payload, dict) else {}
    returned = meta.get("current_page") if isinstance(meta, dict) else None
    if returned is not None and int(returned) != page:
        raise APIWindowError(f"Requested page {page}, API returned page {returned} (per_page={meta.get('per_page')})")
    return unpack_search(payload)


def harvest(client, base, query_list, *, endpoints=("documents",), page_size=100, max_pages=1000, article=True,
            checkpoint=None, on_query_complete=None):
    records = {}
    for endpoint in endpoints:
        for n, query in enumerate(query_list, 1):
            LOG.info("%s query %d/%d: %s", endpoint, n, len(query_list), query)
            seen, count, total = set(), 0, None
            start_page, state, sort = 1, None, "name:desc"
            if checkpoint:
                state, seen = checkpoint.query_state(endpoint, query)
                if state["status"] in ("completed", "partial"):
                    LOG.info("Already handled (%s); skipping query", state["status"])
                    continue
                start_page, count, total = state["next_page"], state["count"], state["total"]
                sort = state["sort"]
                LOG.info("Resuming at page %d (%s); %d unique IDs already saved", start_page, sort, len(seen))
            try:
                while True:
                    try:
                        for page in range(start_page, max_pages + 1):
                            # The current service resets requests at this window to page 1.
                            if page * page_size >= 10000:
                                raise APIWindowError("GoTriple search window reached (page * size >= 10,000)")
                            params = {"q": query, "include_duplicates": False, "page": page, "size": page_size, "sort": sort}
                            if endpoint == "documents" and article:
                                params["fq"] = {"type": "typ_article"}
                            url = base.rstrip("/") + "/" + endpoint + "?" + build_api_url(params)
                            batch, reported = search_page(client, url, page)
                            ids = [record_id(r) for r in batch]
                            if ids and (len(set(ids)) != len(ids) or seen.intersection(ids)):
                                LOG.warning("Overlapping IDs on page %d; retrying without HTTP cache", page)
                                batch, reported = search_page(client, url, page, use_cache=False)
                                ids = [record_id(r) for r in batch]
                            total = reported if reported is not None else total
                            if not batch:
                                if total is not None and (len(seen) if sort == "name:asc" else count) < total:
                                    raise APIError(f"Early empty page: {endpoint} / {query}; {len(seen)} unique IDs of {total}")
                                if checkpoint:
                                    checkpoint.save_page(state, [], page + 1, count, total, True)
                                break
                            new_ids, hit_count = set(ids) - seen, len(batch)
                            if not new_ids and sort == "name:desc" and (total is None or len(seen) < total):
                                raise APIError(f"Page {page} contains only previously saved IDs after retry; pagination did not advance")
                            duplicates = len(ids) - len(new_ids)
                            if duplicates:
                                LOG.warning("Page %d: %d duplicate hits, %d new IDs", page, duplicates, len(new_ids))
                                if page > 1:
                                    previous_url = base.rstrip("/") + "/" + endpoint + "?" + build_api_url({**params, "page": page - 1})
                                    try:
                                        previous, _ = search_page(client, previous_url, page - 1, use_cache=False)
                                        known = seen | set(ids)
                                        recovered = {record_id(r): r for r in previous if record_id(r) not in known}
                                        batch += list(recovered.values())
                                        ids += list(recovered)
                                        LOG.info("Recovered %d IDs by rechecking page %d", len(recovered), page - 1)
                                    except APIError as exc:
                                        LOG.warning("Boundary recheck failed: %s; retaining the received page", exc)
                            seen.update(ids)
                            count += hit_count
                            if not checkpoint:
                                for rec, rid in zip(batch, ids):
                                    key = (endpoint, rid)
                                    records.setdefault(key, {**rec, "id": rid, "resource_kind": endpoint, "_queries": []})
                                    if query not in records[key]["_queries"]:
                                        records[key]["_queries"].append(query)
                            complete = total is not None and (len(seen) >= total or (sort == "name:desc" and count >= total))
                            if checkpoint:
                                checkpoint.save_page(state, batch, page + 1, count, total, complete)
                            LOG.info("Saved page %d (%s): %d unique IDs, API total=%s", page, sort, len(seen), total)
                            if complete:
                                break
                        else:
                            raise APIError(f"Page safety limit reached for {query}; result is incomplete")
                        break
                    except APIWindowError as exc:
                        if sort == "name:asc":
                            raise
                        LOG.warning("%s; continuing the same query from the other end", exc)
                        sort, start_page = "name:asc", 1
                        if checkpoint:
                            checkpoint.reverse_query(state)
            except APIError as exc:
                if not checkpoint:
                    raise
                checkpoint.mark_partial(state, exc, total)
                LOG.warning("Query incomplete: %s. Saved IDs retained; continuing to the next query", exc)
            if on_query_complete:
                on_query_complete()
    return checkpoint.records() if checkpoint else list(records.values())


def extract_year(rec):
    fields = [k for k in ("date_published", "datePublished", "publication_date", "publicationdate") if rec.get(k) not in (None, "", [])]
    field = fields[0] if fields else ""
    value, years = rec.get(field), set()
    for k in fields:
        for item in as_list(rec[k]):
            if isinstance(item, dict):
                item = first(item, "@value", "value", "text", "date")
            s = str(item or "").strip()
            if not re.fullmatch(r"\d{4}(?:-\d{2}(?:-\d{2}(?:[T ].*)?)?)?", s):
                continue
            try:
                if len(s) == 7:
                    datetime.strptime(s, "%Y-%m")
                elif len(s) > 7:
                    datetime.fromisoformat(s.replace("Z", "+00:00"))
            except ValueError:
                continue
            y = int(s[:4])
            if 1000 <= y <= datetime.now().year + 1:
                years.add(y)
    status = "ok" if len(years) == 1 else ("conflicting" if len(years) > 1 else "missing_or_invalid")
    return value, next(iter(years)) if len(years) == 1 else None, field, status


def doi_list(rec):
    out = []
    for key in ("doi", "identifier"):
        for value in as_list(rec.get(key)):
            if isinstance(value, dict):
                value = first(value, "value", "text", "id", "@id")
            for part in str(value or "").split(";"):
                part = re.sub(r"^(?:https?://(?:dx\.)?doi\.org/|doi:\s*)", "", part.strip(), flags=re.I)
                if re.fullmatch(r"10\.\d{4,9}/\S+", part, flags=re.I):
                    out.append(part.lower())
    return list(dict.fromkeys(out))


def ror_id(value):
    m = re.search(r"https?://ror\.org/(0[a-z0-9]{8})(?![a-z0-9])", json.dumps(value, ensure_ascii=False))
    return "https://ror.org/" + m[1] if m else ""


def country(aff):
    if not isinstance(aff, dict):
        return "", ""
    val = first(aff, "country", "addressCountry", "country_name")
    if not val and isinstance(aff.get("address"), dict):
        val = first(aff["address"], "addressCountry", "country")
    code = first(aff, "country_code", "countryCode") or ""
    if isinstance(val, dict):
        code = first(val, "country_code", "code", "identifier") or code
        val = first(val, "name", "text", "label")
    if isinstance(val, str) and re.fullmatch("[A-Z]{2}", val):
        code, val = val, ""
    return str(val or ""), str(code or "")


def author_rows(rec, authors, source):
    result = []
    for pos, author in enumerate(as_list(authors), 1):
        author = author if isinstance(author, dict) else {"fullname": str(author)}
        name = first(author, "fullname", "name") or " ".join(str(author.get(k) or "") for k in ("given", "family")).strip()
        affiliations = as_list(first(author, "affiliation", "affiliations")) or [None]
        for aff in affiliations:
            aff_name = joined(first(aff, "name", "label", "text", "fullname")) if isinstance(aff, dict) else str(aff or "")
            c_name, c_code = country(aff)
            result.append({"id": rec["id"], "resource_kind": rec.get("resource_kind", "documents"), "author_position": pos,
                           "author": name, "author_id": str(first(author, "goTripleId", "id", "@id") or ""),
                           "orcid": str(first(author, "ORCID", "orcid") or ""), "affiliation": aff_name,
                           "country": c_name, "country_code": c_code, "ror_id": ror_id(aff), "source": source,
                           "scope": "publication_metadata", "country_source": source if c_name or c_code else ""})
    return result


def crossref_year(work):
    for k in ("published", "published-print", "published-online", "issued"):
        parts = (work.get(k) or {}).get("date-parts", [])
        if parts and parts[0] and isinstance(parts[0][0], int):
            y = parts[0][0]
            if 1000 <= y <= datetime.now().year + 1:
                return y, k
    return None, ""


def optional_get(client, url, errors):
    try:
        return client.get(url)
    except APIError as exc:
        errors.append({"url": url, "error": str(exc)})
        LOG.warning("Optional metadata unavailable: %s", exc)
        return None


def openalex_authors(work):
    result = []
    for authorship in work.get("authorships", []):
        author = authorship.get("author") or {}
        affiliations = [{"name": i.get("display_name", ""), "country_code": i.get("country_code", ""), "id": i.get("ror", "")}
                        for i in authorship.get("institutions", [])]
        if not affiliations:
            affiliations = [{"name": "; ".join(authorship.get("raw_affiliation_strings", [])),
                             "country_code": "; ".join(authorship.get("countries", []))}]
        result.append({"id": author.get("id", ""), "fullname": authorship.get("raw_author_name") or author.get("display_name", ""),
                       "orcid": authorship.get("raw_orcid") or author.get("orcid", ""), "affiliation": affiliations,
                       "raw_affiliation_strings": authorship.get("raw_affiliation_strings", [])})
    return result


def normalize_records(records, client=None, *, crossref=False, openalex=False, ror=False, mailto="", author_profiles=False, base=BASE_API_URL):
    rows, authors, profiles, errors = [], [], [], []
    for rec in records:
        rec = {**rec, "id": record_id(rec)}
        kind = rec.get("resource_kind", "documents")
        date, year, field, status = extract_year(rec)
        ar = author_rows(rec, first(rec, "author", "authors_raw"), "gotriple")
        cr_year, cr_field, cr_status = None, "", "not_requested"
        dois = doi_list(rec)
        if crossref and client and kind == "documents":
            cr_status = "no_doi" if not dois else "not_found"
            if len(dois) > 1:
                cr_status = "multiple_dois_review_required"
            elif dois:
                url = "https://api.crossref.org/works/" + quote(dois[0], safe="")
                if mailto:
                    url += "?" + urlencode({"mailto": mailto})
                before = len(errors)
                payload = optional_get(client, url, errors)
                if payload:
                    work = payload.get("message")
                    if not isinstance(work, dict) or str(work.get("DOI", "")).lower() != dois[0]:
                        raise APIError("Crossref returned an unexpected DOI or envelope")
                    cr_status = "ok"
                    cr_year, cr_field = crossref_year(work)
                    ar.extend(author_rows(rec, work.get("author"), "crossref:" + dois[0]))
                elif len(errors) > before:
                    cr_status = "api_error"
        oa_year, oa_status = None, "not_requested"
        if openalex and client and kind == "documents":
            oa_status = "no_doi" if not dois else "not_found"
            if len(dois) > 1:
                oa_status = "multiple_dois_review_required"
            elif dois:
                url = "https://api.openalex.org/works/" + quote("https://doi.org/" + dois[0], safe="")
                before = len(errors)
                work = optional_get(client, url, errors)
                if work:
                    if doi_list({"doi": work.get("doi")}) != dois:
                        raise APIError("OpenAlex returned an unexpected DOI")
                    oa_status = "ok"
                    oa_year = work.get("publication_year")
                    oa_authors = openalex_authors(work)
                    oa_rows = author_rows(rec, oa_authors, "openalex:" + str(work.get("id", "")))
                    for a in oa_rows:
                        a["country_source"] = "openalex_affiliation_resolution" if a["country_code"] else ""
                        a["raw_affiliation_strings"] = oa_authors[a["author_position"] - 1]["raw_affiliation_strings"]
                    ar.extend(oa_rows)
                elif len(errors) > before:
                    oa_status = "api_error"
        year_source = "gotriple:" + field if year is not None else ""
        if year is None and status != "conflicting" and cr_year is not None:
            year, year_source, status = cr_year, "crossref:" + cr_field, "crossref_fallback"
        if year is None and status != "conflicting" and isinstance(oa_year, int) and 1000 <= oa_year <= datetime.now().year + 1:
            year, year_source, status = oa_year, "openalex:publication_year", "openalex_fallback"
        if ror and client:
            for a in ar:
                if a["ror_id"] and not (a["country"] or a["country_code"]):
                    data = optional_get(client, "https://api.ror.org/v2/organizations/" + a["ror_id"].rsplit("/", 1)[-1], errors)
                    if data:
                        if data.get("id") != a["ror_id"]:
                            raise APIError("ROR returned an unexpected organization ID")
                        locations = data.get("locations", [])
                        names = sorted({l.get("geonames_details", {}).get("country_name", "") for l in locations} - {""})
                        codes = sorted({l.get("geonames_details", {}).get("country_code", "") for l in locations} - {""})
                        a.update(country="; ".join(names), country_code="; ".join(codes), country_source="ror_exact_id")
        if author_profiles and client:
            for a in ar:
                aid = a["author_id"]
                if a["source"] != "gotriple" or not aid or aid.startswith("_:"):
                    continue
                if aid.startswith("/authors/"):
                    aid = aid[len("/authors/"):]
                data = optional_get(client, base.rstrip("/") + "/authors/" + quote(aid, safe=""), errors)
                if data:
                    data = unpack_detail(data)
                    if record_id(data) != aid:
                        raise APIError(f"Author profile ID mismatch for {aid}")
                    if not first(data, "affiliation", "affiliations") and data.get("current_organization"):
                        data["affiliation"] = data["current_organization"]
                    for p in author_rows(rec, [data], "gotriple_author_profile"):
                        p["scope"] = "current_profile_not_publication_affiliation"
                        profiles.append(p)
        authors.extend(ar)
        q = rec.get("_queries") or as_list(rec.get("query"))
        abstract = first(rec, "abstract", "description")
        row = {k: v for k, v in rec.items() if k in rec.get("_original_columns", [])}
        row.update({"id": rec["id"], "resource_kind": kind, "doi": "; ".join(dois), "provider": joined(rec.get("provider")),
                    "query": "; ".join(sorted(set(q))), "title": " ".join(labels(first(rec, "headline", "title", "name"))),
                    "abstract_text": " ".join(labels(abstract)), "abstract_en": " ".join(labels(abstract, "en")),
                    "keywords": joined(rec.get("keywords")), "authors": "; ".join(dict.fromkeys(a["author"] for a in ar if a["author"])),
                    "datePublished": date, "publication_date_field": field, "original_date_published": rec.get("original_date_published"),
                    "publication_year": year, "year_source": year_source, "year_status": status,
                    "crossref_year": cr_year, "openalex_year": oa_year,
                    "year_conflict": status == "conflicting" or bool(year is not None and any(y is not None and y != year for y in (cr_year, oa_year))),
                    "crossref_status": cr_status, "openalex_status": oa_status,
                    "affiliations": "; ".join(sorted({a["affiliation"] for a in ar if a["affiliation"]})),
                    "affiliation_countries": "; ".join(sorted({a["country"] for a in ar if a["country"]})),
                    "affiliation_country_codes": "; ".join(sorted({code.strip() for a in ar for code in a["country_code"].split(";") if code.strip()})),
                    "affiliation_status": "available" if any(a["affiliation"] for a in ar) else "not_available",
                    "metadata_status": rec.get("_metadata_status", "available"),
                    "project_start_date": first(rec, "start_date", "startDate") if kind == "projects" else None,
                    "project_end_date": first(rec, "end_date", "endDate") if kind == "projects" else None,
                    "project_organizations": rec.get("organization") if kind == "projects" else None})
        for k in ("title", "abstract_text", "keywords", "authors", "doi", "provider", "query"):
            if not row[k] and rec.get(k):
                row[k] = rec[k]
        rows.append(row)
    return rows, authors, profiles, errors


def load_input(path):
    path = Path(path)
    if path.suffix.lower() in {".xlsx", ".csv"}:
        import pandas as pd
        frame = pd.read_excel(path, dtype=str, keep_default_na=False) if path.suffix.lower() == ".xlsx" else pd.read_csv(path, dtype=str, keep_default_na=False)
        if "id" not in frame:
            raise ValueError("Input table must contain the original GoTriple 'id' column")
        records = frame.to_dict("records")
        for rec in records:
            rec["_original_columns"] = list(frame.columns)
    elif path.suffix.lower() == ".jsonl":
        records = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    else:
        data = json.loads(path.read_text(encoding="utf-8"))
        records = data if isinstance(data, list) else unpack_search(data)[0]
    for rec in records:
        record_id(rec)
    return records


def refresh_records(records, client, base, offline=False, checkpoint=None, on_record_complete=None):
    result = {}
    for rec in records:
        rid = record_id(rec)
        kind = rec.get("resource_kind") or "documents"
        if kind not in {"documents", "projects"}:
            raise ValueError(f"Invalid resource_kind: {kind}")
        key = (kind, rid)
        q = rec.get("_queries") or [q.strip() for q in str(rec.get("query", "")).split(";") if q.strip()]
        if key in result:
            result[key]["_queries"] = sorted(set(result[key]["_queries"] + q))
            if checkpoint:
                checkpoint.save_record(result[key])
            continue
        if checkpoint and checkpoint.was_refreshed(kind, rid):
            result[key] = checkpoint.get_record(kind, rid)
            LOG.info("Already refreshed; skipping %s", rid)
            continue
        detail = None if offline else unpack_detail(client.get(base.rstrip("/") + "/" + kind + "/" + quote(rid, safe="")))
        if detail and record_id(detail) != rid:
            raise APIError(f"Detail ID mismatch for {rid}")
        original = dict(rec)
        if detail:
            for field in ("datePublished", "date_published", "publication_date", "publicationdate"):
                original.pop(field, None)  # A previous normalized date is not fresh API metadata.
        result[key] = {**original, **(detail or {}), "id": rid, "resource_kind": kind, "_queries": q,
                       "_metadata_status": "offline_input" if offline else ("refreshed" if detail else "not_found")}
        if checkpoint:
            checkpoint.save_record(result[key])
        if on_record_complete:
            on_record_complete(result[key])
    return list(result.values())


@contextmanager
def atomic_file(path):
    """An interrupted write leaves the preceding complete file intact."""
    path = Path(path)
    tmp = path.with_name(f".{path.stem}.tmp-{uuid.uuid4().hex}{path.suffix}")
    try:
        yield tmp
        # Windows _commit/FlushFileBuffers requires a writable handle.
        # r+b preserves the completed temporary file without truncating it.
        with tmp.open("r+b") as handle:
            os.fsync(handle.fileno())
        try:
            tmp.replace(path)
        except PermissionError:
            # Excel may lock an open workbook on Windows. Continue harvesting.
            fallback = path.with_name(f"{path.stem}_latest{path.suffix}")
            try:
                tmp.replace(fallback)
            except PermissionError:
                fallback = path.with_name(f"{path.stem}_snapshot_{uuid.uuid4().hex[:8]}{path.suffix}")
                tmp.replace(fallback)
            LOG.warning("%s is locked; saved current snapshot to %s", path.name, fallback)
    finally:
        tmp.unlink(missing_ok=True)


def write_json(path, value):
    with atomic_file(path) as tmp:
        tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")


def export(output, records, tables, manifest, write_excel=True):
    import pandas as pd
    from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    rows, authors, profiles, errors = tables
    with atomic_file(output / "raw_records.jsonl") as tmp:
        with tmp.open("w", encoding="utf-8") as f:
            for rec in records:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    def cell(v):
        if isinstance(v, (list, dict)):
            v = json.dumps(v, ensure_ascii=False)
        return ILLEGAL_CHARACTERS_RE.sub("", v) if isinstance(v, str) else v
    record_cols = list(dict.fromkeys(k for r in rows for k in r)) if rows else ["id", "resource_kind", "publication_year", "year_source", "affiliations", "query"]
    author_cols = list(dict.fromkeys(k for r in authors + profiles for k in r)) if authors or profiles else ["id", "resource_kind", "author", "affiliation", "country", "source", "scope"]
    frames = {"records": pd.DataFrame(rows, columns=record_cols), "authors": pd.DataFrame(authors, columns=author_cols),
              "author_profiles": pd.DataFrame(profiles, columns=author_cols), "errors": pd.DataFrame(errors, columns=["url", "error"])}
    truncated = 0
    clean_frames = {name: frame.map(cell) for name, frame in frames.items()}
    for name, clean in clean_frames.items():
        with atomic_file(output / (name + ".csv")) as tmp:
            clean.to_csv(tmp, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_ALL)
        truncated += sum(isinstance(v, str) and len(v) > 32767 for row in clean.itertuples(index=False, name=None) for v in row)
    if write_excel:
        with atomic_file(output / "cc_fasca_gotriple_enriched.xlsx") as tmp, pd.ExcelWriter(tmp, engine="openpyxl") as writer:
            for name, clean in clean_frames.items():
                clean.to_excel(writer, sheet_name=name, index=False)
                sheet = writer.sheets[name]
                sheet.freeze_panes = "A2"
                sheet.auto_filter.ref = sheet.dimensions
                for excel_row in sheet.iter_rows(min_row=2):
                    for c in excel_row:
                        if isinstance(c.value, str):
                            c.data_type = "s"
    manifest.update(records=len(rows), author_affiliation_rows=len(authors), profile_rows=len(profiles), optional_errors=len(errors),
                    records_with_year=sum(r["publication_year"] is not None for r in rows),
                    records_with_affiliation=sum(bool(r["affiliations"]) for r in rows),
                    records_with_country=sum(bool(r["affiliation_countries"] or r["affiliation_country_codes"]) for r in rows),
                    excel_truncated_cells=truncated, last_saved_at=now())
    write_json(output / "run_manifest.json", manifest)


def save_progress(checkpoint, output, manifest, client):
    """Small progress files; no loading or rewriting the corpus."""
    progress = checkpoint.progress()
    gaps = [p for p in progress if p["status"] == "completed" and p["total"] is not None and p["unique_count"] < p["total"]]
    partial = [p for p in progress if p["status"] == "partial"]
    manifest.update(checkpoint.counts(), requests=client.audit, progress_saved_at=now(),
                    records_saved=checkpoint.db.execute("SELECT COUNT(*) FROM records").fetchone()[0],
                    partial_queries=partial, queries_with_coverage_gaps=gaps,
                    duplicate_hits=sum(p["duplicate_hits"] for p in progress))
    if gaps or partial:
        manifest.update(corpus_complete=False, acquisition_complete=False)
    with atomic_file(Path(output) / "query_progress.csv") as tmp:
        with tmp.open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["kind", "query", "status", "next_page", "count", "total", "unique_count", "duplicate_hits", "sort", "error"])
            writer.writeheader()
            writer.writerows(progress)
    write_json(Path(output) / "run_manifest.json", manifest)


def save_snapshot(checkpoint, output, manifest, client, *, write_excel=True, english_only=False):
    """Export a usable preview without waiting for external metadata requests."""
    save_progress(checkpoint, output, manifest, client)
    records = checkpoint.records()
    tables = ([], [], [], [])
    flags = manifest["configuration"]
    enrichment_requested = any(flags.get(k) for k in ("crossref", "openalex", "ror", "author_profiles"))
    for rec in records:
        part = checkpoint.saved_tables(rec["resource_kind"], rec["id"])
        if part is None:
            part = normalize_records([rec])
            part[0][0]["enrichment_status"] = "pending" if enrichment_requested else "not_requested"
            for key in ("crossref", "openalex"):
                if flags.get(key):
                    part[0][0][key + "_status"] = "pending"
        else:
            part[0][0]["query"] = "; ".join(sorted(set(rec.get("_queries", []))))
            part[0][0]["enrichment_status"] = "completed_with_optional_errors" if part[3] else "completed"
        for destination, source in zip(tables, part):
            destination.extend(source)
    if english_only:
        keep = {(r["resource_kind"], r["id"]) for r in tables[0] if r["abstract_en"]}
        records = [r for r in records if (r["resource_kind"], r["id"]) in keep]
        tables = tuple([r for r in t if (r["resource_kind"], r["id"]) in keep] if i < 3 else t for i, t in enumerate(tables))
    export(output, records, tables, manifest, write_excel=write_excel)
    LOG.info("Saved snapshot: %d records, %d completed queries, %d normalized records",
             len(records), manifest["queries_completed"], manifest["records_normalized"])
    return tables


def check_api(client, base, output):
    result = {"checked_at": now(), "base": base, "checks": [], "live_verified": False}
    for endpoint in ("documents", "authors", "projects"):
        try:
            params = {"q": "government AND art AND identity", "size": 2, "page": 1}
            if endpoint == "documents":
                params.update(fq={"type": "typ_article"}, include_duplicates=False, sort="name:desc")
            batch, total = unpack_search(client.get(base.rstrip("/") + "/" + endpoint + "?" + build_api_url(params), use_cache=False))
            check = {"endpoint": endpoint, "status": "ok", "count": len(batch), "total": total, "fields": sorted({k for r in batch for k in r})}
            if batch:
                detail = unpack_detail(client.get(base.rstrip("/") + "/" + endpoint + "/" + quote(record_id(batch[0]), safe=""), use_cache=False))
                if detail is None or record_id(detail) != record_id(batch[0]):
                    raise APIError("Search result detail missing or ID mismatch")
                check["detail"] = "ok"
            if endpoint == "documents":
                result["live_verified"] = bool(batch)
                check["date_fields"] = {k: batch[0].get(k) for k in ("datePublished", "date_published") if batch and k in batch[0]}
            result["checks"].append(check)
        except (APIError, ValueError) as exc:
            result["checks"].append({"endpoint": endpoint, "status": "failed", "error": str(exc)})
    result["requests"] = client.audit
    write_json(output, result)
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["harvest", "enrich", "check-api"], nargs="?", default="harvest")
    parser.add_argument("--input", help="Original XLSX/CSV with id, or raw JSON/JSONL")
    parser.add_argument("--output", default="data/fasca_crous_costa")
    parser.add_argument("--base-url", default=BASE_API_URL)
    parser.add_argument("--cache", default="data/fasca_cache")
    parser.add_argument("--refresh", action="store_true", help="Ignore HTTP cache for unfinished operations; completed checkpoint operations remain saved")
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("--attempts", type=int, default=3)
    parser.add_argument("--delay", type=float, default=0.3)
    parser.add_argument("--mailto", default="", help="Real contact email for the Crossref polite pool")
    parser.add_argument("--crossref", action="store_true", help="DOI-linked publication year and author affiliations")
    parser.add_argument("--openalex", action="store_true", help="DOI-linked publication affiliations and institution countries from OpenAlex")
    parser.add_argument("--ror", action="store_true", help="Countries from existing exact ROR IDs only")
    parser.add_argument("--author-profiles", action="store_true", help="Separate current-profile affiliations")
    parser.add_argument("--offline-input", action="store_true", help="Skip refreshing GoTriple input; optional Crossref/ROR can still use the network")
    parser.add_argument("--include-projects", action="store_true")
    parser.add_argument("--all-document-types", action="store_true")
    parser.add_argument("--english-abstract-only", action="store_true", help="Filter tagged English abstracts locally; changes the corpus")
    parser.add_argument("--quote-phrases", action="store_true", help="Phrase queries instead of original syntax; changes the corpus")
    parser.add_argument("--query", action="append", help="Override the 560 original combinations; repeatable")
    parser.add_argument("--max-queries", type=int, help="Subset smoke test; manifest marks corpus incomplete")
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--max-pages", type=int, default=1000)
    parser.add_argument("--save-every-records", type=int, default=20, help="Export every N refreshed/enriched records; first record is always exported")
    parser.add_argument("--save-every-queries", type=int, default=10, help="Full CSV/JSONL/XLSX snapshots every N handled queries; first query, stop and end always save; 0 means stop/end only")
    parser.add_argument("--excel-every-queries", type=int, default=1, help="Additional XLSX frequency within scheduled full snapshots; 0 means XLSX only at stop/end")
    parser.add_argument("--retry-partial", action="store_true", help="Retry queries previously marked partial; completed queries remain skipped")
    args = parser.parse_args(argv)
    if not (1 <= args.page_size <= 100) or min(args.attempts, args.max_pages) < 1 or args.timeout <= 0 or args.delay < 0:
        parser.error("Invalid page size, timeout, attempts, max pages or delay")
    if args.max_queries is not None and args.max_queries < 1:
        parser.error("--max-queries must be positive")
    if args.mode == "enrich" and not args.input:
        parser.error("enrich requires --input")
    if args.offline_input and args.mode != "enrich":
        parser.error("--offline-input is available only with enrich")
    if args.save_every_records < 1 or min(args.excel_every_queries, args.save_every_queries) < 0:
        parser.error("Invalid snapshot frequency")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    client = Client(args.cache, timeout=args.timeout, attempts=args.attempts, delay=args.delay, refresh=args.refresh, mailto=args.mailto)
    manifest = {"started_at": now(), "configuration": vars(args), "status": "running", "corpus_complete": False,
                "acquisition_complete": False, "enrichment_complete": False, "requests_scope": "current_process"}
    checkpoint = None
    try:
        if args.mode == "check-api":
            result = check_api(client, args.base_url, output / "api_check.json")
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return 0 if result["live_verified"] else 1
        full_queries = list(dict.fromkeys(args.query or queries(args.quote_phrases)))
        qs = full_queries[:args.max_queries] if args.max_queries else full_queries
        full_count = len(full_queries)
        endpoints = ("documents", "projects") if args.include_projects else ("documents",)
        configuration = {"checkpoint_version": 1, "mode": args.mode, "base_url": args.base_url.rstrip("/"),
                         "page_size": args.page_size, "article": not args.all_document_types,
                         "queries": full_queries if args.mode == "harvest" else [], "endpoints": endpoints,
                         "crossref": args.crossref, "openalex": args.openalex, "ror": args.ror,
                         "author_profiles": args.author_profiles, "english_only": args.english_abstract_only,
                         "offline_input": args.offline_input}
        if args.mode == "enrich":
            digest = hashlib.sha256()
            with Path(args.input).open("rb") as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    digest.update(chunk)
            configuration["input_sha256"] = digest.hexdigest()
        checkpoint = Checkpoint(output / "progress.sqlite", configuration)
        if args.retry_partial:
            with checkpoint.db:
                checkpoint.db.execute("UPDATE jobs SET status='in_progress', error='' WHERE status='partial'")

        def snapshot(write_excel=True):
            return save_snapshot(checkpoint, output, manifest, client, write_excel=write_excel,
                                 english_only=args.english_abstract_only)

        enrichment_requested = any((args.crossref, args.openalex, args.ror, args.author_profiles))

        def enrich_pending(retry_errors=False, write_excel=True, records=None):
            if not enrichment_requested:
                return  # Raw previews already contain normalized GoTriple metadata.
            manifest["phase"] = "enriching"
            for rec in checkpoint.records() if records is None else records:
                saved = checkpoint.saved_tables(rec["resource_kind"], record_id(rec))
                if saved is not None and (not retry_errors or not saved[3]):
                    continue
                tables = normalize_records([rec], client, crossref=args.crossref, openalex=args.openalex, ror=args.ror,
                                           mailto=args.mailto, author_profiles=args.author_profiles, base=args.base_url)
                checkpoint.save_tables(rec, tables)
                count = checkpoint.counts()["records_normalized"]
                if args.mode == "enrich" and (count == 1 or count % args.save_every_records == 0):
                    snapshot(write_excel=write_excel)

        def initial_snapshot():
            phase = manifest["phase"]
            save_progress(checkpoint, output, manifest, client)
            # A stopped query may already be harvested but only partly enriched.
            if enrichment_requested:
                enrich_pending(retry_errors=True, write_excel=args.excel_every_queries > 0)
            manifest["phase"] = phase

        if args.mode == "harvest":
            with checkpoint.db:
                checkpoint.db.executemany("INSERT OR IGNORE INTO jobs (key,kind,query,status,next_page,count,total) VALUES (?, ?, ?, 'pending', 1, 0, NULL)",
                                         [(checkpoint.job_key(e, q), e, q) for e in endpoints for q in qs])
            manifest.update(query_count=len(qs), configured_query_count=full_count, queries=qs,
                            queries_target=len(qs) * len(endpoints), phase="harvesting",
                            corpus_basis="custom_queries" if args.query else "original_560_queries")
            initial_snapshot()

            def after_query():
                handled = checkpoint.counts()["queries_finished"]
                frequency = args.save_every_queries
                due = frequency > 0 and (handled == 1 or handled % frequency == 0)
                write_excel = args.excel_every_queries > 0 and (handled == 1 or handled % args.excel_every_queries == 0)
                manifest["phase"] = "harvesting"
                if due:
                    snapshot(write_excel=write_excel)
                else:
                    save_progress(checkpoint, output, manifest, client)
                # A readable raw preview exists before any slower external requests.
                if enrichment_requested:
                    enrich_pending(write_excel=write_excel)
                    manifest["phase"] = "harvesting"
                    if due:
                        snapshot(write_excel=write_excel)
                    else:
                        save_progress(checkpoint, output, manifest, client)

            harvest(client, args.base_url, qs, endpoints=endpoints, page_size=args.page_size,
                              max_pages=args.max_pages, article=not args.all_document_types,
                              checkpoint=checkpoint, on_query_complete=after_query)
            manifest["corpus_complete"] = len(qs) == full_count
        else:
            input_records = load_input(args.input)
            manifest.update(phase="refreshing", corpus_basis="input_ids", input_records=len(input_records))
            initial_snapshot()

            def after_refresh(rec):
                count = checkpoint.counts()["records_refreshed"]
                if count == 1 or count % args.save_every_records == 0:
                    snapshot(write_excel=args.excel_every_queries > 0)
                if enrichment_requested:
                    enrich_pending(write_excel=args.excel_every_queries > 0, records=[rec])
                    manifest["phase"] = "refreshing"

            records = refresh_records(input_records, client, args.base_url, args.offline_input,
                                      checkpoint=checkpoint, on_record_complete=after_refresh)
            manifest.update(corpus_complete=True, corpus_basis="input_ids", input_records=len(records))
        manifest.update(acquisition_complete=True, phase="enriching")
        enrich_pending(write_excel=args.excel_every_queries > 0)
        manifest.update(enrichment_complete=True, phase="completed", status="completed", finished_at=now())
        tables = snapshot()
        if manifest["partial_queries"]:
            manifest["status"] = "completed_with_incomplete_queries"
            write_json(output / "run_manifest.json", manifest)
        elif tables[3]:
            manifest["status"] = "completed_with_optional_errors"
            write_json(output / "run_manifest.json", manifest)
        elif manifest["queries_with_coverage_gaps"]:
            manifest["status"] = "completed_with_pagination_warnings"
            write_json(output / "run_manifest.json", manifest)
        if manifest["queries_with_coverage_gaps"]:
            LOG.warning("%d queries have fewer unique IDs than the API total; inspect query_progress.csv",
                        len(manifest["queries_with_coverage_gaps"]))
        LOG.info("Saved %d records to %s", len(tables[0]), output)
        if manifest["partial_queries"]:
            LOG.warning("%d queries remain partial; all planned queries have been handled. See query_progress.csv", len(manifest["partial_queries"]))
        return 2 if tables[3] or manifest["queries_with_coverage_gaps"] or manifest["partial_queries"] else 0
    except KeyboardInterrupt:
        manifest.update(status="interrupted", finished_at=now())
        if checkpoint:
            try:
                save_snapshot(checkpoint, output, manifest, client, english_only=args.english_abstract_only)
            except Exception as save_error:
                LOG.error("Preview export failed: %s. Committed data remain in progress.sqlite.", save_error)
        LOG.warning("Stopped. Resume with the same command and --output directory.")
        return 130
    except Exception as exc:
        manifest.update(status="failed", error=str(exc), finished_at=now(), requests=client.audit)
        if checkpoint:
            try:
                save_snapshot(checkpoint, output, manifest, client, english_only=args.english_abstract_only)
            except Exception as save_error:
                LOG.error("Preview export failed: %s. Committed data remain in progress.sqlite.", save_error)
        else:
            write_json(output / "last_error.json", manifest)
        LOG.exception("%s. Saved progress is retained; rerun the same command to resume.", exc)
        return 1
    finally:
        if checkpoint:
            checkpoint.close()


if __name__ == "__main__":
    def terminate(signum, frame):
        raise KeyboardInterrupt("Termination signal")

    signal.signal(signal.SIGTERM, terminate)
    sys.exit(main())
