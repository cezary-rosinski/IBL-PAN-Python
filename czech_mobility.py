import requests
from core_api_key import core_api_key


#%%

def get_metadata_for_doi(doi: str) -> dict:
    """
    Pobiera metadane artykułu z Crossref dla pojedynczego DOI.
    
    Args:
        doi: pełny DOI lub DOI bez prefiksu 'https://doi.org/' (np. '10.22148/001c.68341')
    Returns:
        dict: parsowany JSON z odpowiedzi Crossref, albo pusty dict przy błędzie.
    """
    # Upewniamy się, że mamy tylko komponent DOI
    doi = doi.strip()
    
    doi = '10.22148/001c.68341'
    if doi.startswith("http"):
        doi = doi.split("doi.org/")[-1]
    
    url = f"https://api.crossref.org/works/{doi}"
    headers = {
        "User-Agent": "MyCrossrefClient/1.0 (mailto:twoj_email@domena.pl)"
    }
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        
        
        # struktura: {'status': 'ok', 'message-type': 'work', 'message-version': '1.0.0', 'message': { … }}
        return data.get("message", {})
    except requests.exceptions.RequestException as e:
        print(f"Error fetching DOI {doi}: {e}")
        return {}

def get_metadata_for_doi_list(dois: list[str]) -> dict[str, dict]:
    """
    Iteruje po liście DOI i pobiera metadane dla każdego.
    
    Args:
        dois: lista DOI (pełne lub w formacie '10.xxxx/xxxx')
    Returns:
        dict mapping doi -> metadata dict
    """
    results = {}
    for doi in dois:
        meta = get_metadata_for_doi(doi)
        if meta:
            results[doi] = meta
        else:
            results[doi] = {}
    return results

if __name__ == "__main__":
    # przykładowe DOI
    dois = [
        "10.22148/001c.68341",
        # tutaj możesz dodać kolejne DOI
    ]
    all_meta = get_metadata_for_doi_list(dois)
    # wyświetlamy w ładnej formie
    import json
    print(json.dumps(all_meta, indent=2, ensure_ascii=False))



test = 'https://api.core.ac.uk/v3/journals/issn:1453-1305?apiKey=xxx'

doi = '10.22148/001c.68341'
test = f'https://api.core.ac.uk/v3/works/{doi}?apiKey={core_api_key}'

resp = requests.get(test, timeout=10)
data = resp.json()




















