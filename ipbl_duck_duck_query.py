import requests
import csv
from time import sleep
from tqdm import tqdm

# ===== KONFIGURACJA =====
TIMEOUT = 10
DELAY = 0.3  # opóźnienie między requestami (sekundy)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (URL Validator for research purposes)"
}

#%%
urls = [
    # --- wcześniejsze propozycje: portale / krytyka ---
    "https://www.dwutygodnik.com/literatura",
    "https://www.dwutygodnik.com/recenzje",
    "https://czaskultury.pl/",
    "https://mintmagazine.pl/",
    "https://booklips.pl/",
    "https://kwartalnikwyspa.pl/",
    "https://tworczosc.com.pl/",
    "https://www.biuroliterackie.pl/biblioteka/",
    "https://www.biuroliterackie.pl/biblioteka/debaty/",
    "https://przeczytane.net/",
    "https://czytampierwotnie.pl/",
    "https://literacka.pl/",
    "https://www.magiel-literacki.pl/",
    "https://zadruga-literacka.pl/",
    "https://www.tekstualia.pl/",
    "https://ksiazki.wp.pl/",
    "https://www.polityka.pl/tygodnikpolityka/kultura",
    "https://www.newsweek.pl/kultura/",
    "https://wyborcza.pl/0,128956.html",
    "https://nofluffculture.pl/",

    # --- teatr ---
    "https://www.e-teatr.pl/",
    "https://e-teatr.pl/recenzje",
    "https://www.teatralny.pl/",
    "https://www.teatralny.pl/rodzaje/recenzje",
    "https://teatr-pismo.pl/",
    "https://teatr-pismo.pl/category/recenzje/",
    "https://www.terazteatr.pl/",
    "https://scenaonline.org/",
    "https://teatrologia.org/",
    "https://teksty.org.pl/teatr",
    "https://www.teatrnn.pl/",
    "https://www.teatrpolski.krakow.pl/",
    "https://teatr-galaktyczny.pl/",
    "https://teatr-szekspirowski.pl/",
    "https://www.teatrzimowy.pl/",

    # --- film ---
    "https://pelnasala.pl/",
    "https://mediakrytyk.pl/",
    "https://www.filmawka.pl/",
    "https://www.filmawka.pl/category/recenzje/filmy/festiwale/",
    "https://kameraakcja.com.pl/",
    "https://kameraakcja.com.pl/katalog-krytykow-filmowych/",
    "https://film.org.pl/",
    "https://www.filmweb.pl/powiekszenie",
    "https://shortfestival.pl/",
    "https://www.cgm.pl/",
    "https://filmpolski.pl/fp/index.php",
    "https://stopklatka.pl/",
    "https://www.kinomuzeum.pl/",
    "https://warsawfilmart.pl/",
    "https://www.tarnowskiefestiwale.pl/",
    "https://www.pisf.pl/",
    "https://festivale-online.pl/",
    "https://pl.konfrontacje.pl/",

    # --- VOD / audio-wideo ---
    "https://ninateka.pl/",
    "https://www.naninateka.pl/przeglad/teatr",
    "https://vod.tvp.pl/",
    "https://vod.tvp.pl/teatr",
    "https://teatrtv.vod.tvp.pl/",
    "https://sport.tvp.pl/53989994/kultura",
    "https://vod.polskieradio.pl/",

    # --- radio / podcasty ---
    "https://www.polskieradio.pl/8/3347",
    "https://24polska.pl/kultura",
    "https://audycje.kulturaonline.pl/",
    "https://echo-kultury.pl/",
    "https://seriale-i-filmy-podcast.pl/",
    "https://cdc.pl/",
    "https://piatek.pl/",
    "https://swiatelokacji.pl/",
    "https://glosliteratury.pl/",

    # --- festiwale ---
    "https://poznanskiefestiwale.pl/",
    "https://festiwaltetrowwarszawa.pl/",
    "https://shortwaves.pl/",
    "https://miastomiasta.org/",
    "https://openeer.com/",
    "https://audioriver.pl/",
    "https://integracje.eu/",

    # --- humanistyka cyfrowa / archiwa ---
    "https://humanistyka.dev/",
    "https://humanistyka.dev/lessons/",
    "https://delab.uw.edu.pl/",
    "https://archiwa.net/",
    "https://www.archiwa.net/index.php?option=com_content&view=section&layout=blog&id=27&Itemid=42",
    "https://digitalhumanities.pl/",
    "https://dhacademy.org/",
    "https://dariah.eu/",
    "https://repozytorium.icm.edu.pl/",

    # --- biblioteki cyfrowe ---
    "https://polona.pl/",
    "https://dlibra.bg.pal.pl/",
    "https://www.ebuw.uw.edu.pl/",
    "https://www.wbc.poznan.pl/",
    "https://mbc.cyfrowemazowsze.pl/",
    "https://fbc.pionier.net.pl/",

    # --- archiwa społeczne ---
    "https://cas.org.pl/",
    "https://cas.org.pl/wydarzenia/",
    "https://archiwa.org/",
    "https://archiwa.gov.pl/poznaj/wystawy-i-prezentacje-online/",
    "https://docuweb.eu/",

    # --- self-publishing / newslettery ---
    "https://mariuszjagora.substack.com/",
    "https://kluska.substack.com/",
    "https://publica.pl/",
    "https://booknode.pl/",
    "https://bezszycia.pl/",
    "https://ziniol.pl/",

    # --- YouTube: instytucje ---
    "https://www.youtube.com/c/FilmPolskiOfficial",
    "https://www.youtube.com/c/CentrumSztukiWspolczesnejZachęta",
    "https://www.youtube.com/c/TeatrNarodowy/videos",

    # --- social media ---
    "https://www.instagram.com/ninateka/",
    "https://www.facebook.com/e.teatr/",
    "https://www.facebook.com/ShortWavesFestival/",
    "https://www.facebook.com/FilmPolski/",
    "https://www.facebook.com/TeatrNarodowy/",
    "https://www.facebook.com/PolskiInstytutSztukiFilmowej/",

    # --- BookTube: zweryfikowane kanały ---
    "https://www.youtube.com/user/bookreviewsbyanita",
    "https://www.youtube.com/@zaksiazkowane8428",
    "https://www.youtube.com/@lookforbookss",
    "https://www.youtube.com/channel/UCeN4s9FDFqftfTfQAYTy_LQ",
    "https://www.youtube.com/channel/UCrpg11zJlFeuRChYYccLVTA",
    "https://www.youtube.com/@SezonLiteracki",
    "https://www.youtube.com/@literatunek",
    "https://www.youtube.com/@kursywa",
    "https://www.youtube.com/@oksiazkach",
    "https://www.youtube.com/@getbooky",
    "https://www.youtube.com/@shubiektywnie",
    "https://www.youtube.com/@Bestselerki",
]

# ===== FUNKCJA WALIDUJĄCA =====
def check_url(url):
    try:
        # najpierw HEAD
        r = requests.head(
            url,
            headers=HEADERS,
            allow_redirects=True,
            timeout=TIMEOUT
        )
        return r.status_code, r.url
    except requests.exceptions.RequestException:
        try:
            # fallback: GET
            r = requests.get(
                url,
                headers=HEADERS,
                allow_redirects=True,
                timeout=TIMEOUT
            )
            return r.status_code, r.url
        except requests.exceptions.RequestException as e:
            return None, str(e)

# ===== WALIDACJA =====
results = []

for url in tqdm(urls):
    status, final_url = check_url(url)
    results.append({
        "original_url": url,
        "status_code": status,
        "final_url": final_url
    })
    sleep(DELAY)

# ===== ZAPIS DO CSV =====
with open("data/url_validation_results.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=["original_url", "status_code", "final_url"]
    )
    writer.writeheader()
    writer.writerows(results)

print("✔ Walidacja zakończona. Wyniki zapisane do url_validation_results.csv")


#%%

from ddgs import DDGS
from tqdm import tqdm
import pandas as pd

QUERIES = [
    # --- oryginalne kwerendy ---
    "polski booktube",
    "booktube polska lista",
    "kanały o książkach youtube",
    "książki youtube blog",
    "recenzje książek youtube",
    "książki vlog",
    "czytam książki blog",
    "bookhaul książki",
    "wrap up książki",
    "książki literatura youtube",
    "książki youtuberzy",
    "najlepsze kanały książkowe",
    "książki w social mediach",
    "literacki Youtube Polska",
    "booktok polska",
    "bookstagram polska",
    "instagram książki",
    "tiktok książki",
    "facebook książki",
    "recenzje literackie Youtube Polska",
    "podcasty literackie Youtube Polska",
    "kanały teatrów Youtube Polska",
    "literatura yt polska",
    "Bookstagram Polska",

    # --- YouTube / BookTube – warianty ---
    "booktube polska kanały",
    "polscy booktuberzy",
    "kanały literackie youtube",
    "youtube o książkach po polsku",
    "książkowy youtube polska",
    "literatura na youtube polska",
    "youtube recenzje literatury",
    "youtube o czytaniu książek",
    "książki na yt po polsku",
    "polskie kanały o czytaniu",

    # --- formaty BookTube ---
    "bookhaul polska youtube",
    "wrap up książkowy youtube",
    "tbr książki youtube",
    "reading vlog polska",
    "czytelniczy vlog youtube",
    "książkowy vlog polski",
    "vlog o czytaniu książek",
    "miesięczne podsumowanie książek youtube",

    # --- BookTok ---
    "booktok polska",
    "tiktok książki polska",
    "tiktok literatura polska",
    "książki na tiktoku",
    "booktok polskie polecenia",
    "booktok recenzje książek",
    "czytanie książek tiktok",
    "polski booktok lista",

    # --- Bookstagram / Instagram ---
    "bookstagram polska",
    "instagram książki polska",
    "instagram literatura polska",
    "książki na instagramie",
    "profile książkowe instagram",
    "recenzje książek instagram",
    "czytelnicze konta instagram",
    "bookstagramerzy polska",

    # --- Facebook / społeczności ---
    "książki facebook polska",
    "literatura facebook grupy",
    "grupy czytelnicze facebook",
    "recenzje książek facebook",
    "społeczności czytelnicze online",
    "media społecznościowe literatura polska",

    # --- blogi i portale książkowe ---
    "blogi książkowe polska",
    "blog o książkach po polsku",
    "recenzje książek blog",
    "czytelniczy blog polska",
    "literacki blog polski",
    "książki blogerzy polska",

    # --- podcasty / audio ---
    "podcasty o książkach polska",
    "podcast literacki polski",
    "książki podcast polska",
    "rozmowy o literaturze podcast",
    "bookcast polska",

    # --- krytyka literacka online ---
    "recenzje literackie online polska",
    "krytyka literacka internet",
    "literatura w internecie polska",
    "współczesna krytyka literacka online",
    "pisma literackie online",

    # --- ujęcia meta ---
    "literatura w mediach społecznościowych",
    "czytelnictwo social media polska",
    "książki a media społecznościowe",
    "promocja książek w social mediach",
    "obieg literatury online",

    # --- warianty anglojęzyczne ---
    "polish booktube",
    "polish booktok",
    "polish bookstagram",
    "polish book bloggers",
    "polish literary youtube",
    "books youtube poland",
]


articles = {}

with DDGS() as ddgs:
    for q in tqdm(QUERIES):
        for r in ddgs.text(q, region="pl-pl"):
            link = r.get("href")
            if not link:
                continue

            articles[link] = {
                "title": r.get("title"),
                "link": link,
                "snippet": r.get("body"),
                "query": q,
            }


print(f"Zebrano {len(articles)} artykułów/list.")


df = pd.DataFrame(articles.values())


#%%

queries = [
    # --- Uniwersytet Mikołaja Kopernika ---
    "Uniwersytet Mikołaja Kopernika wydział filologii polskiej",
    "Uniwersytet Mikołaja Kopernika instytut polonistyki",
    "UMK instytut literaturoznawstwa",
    "UMK instytut języka polskiego",

    # --- Uniwersytet Śląski ---
    "Uniwersytet Śląski wydział humanistyczny polonistyka",
    "Uniwersytet Śląski instytut literaturoznawstwa",
    "Uniwersytet Śląski instytut języka polskiego",
    "UŚ filologia polska instytut",

    # --- Uniwersytet Jana Kochanowskiego w Kielcach ---
    "Uniwersytet Jana Kochanowskiego filologia polska",
    "UJK Kielce instytut literaturoznawstwa",
    "UJK Kielce instytut języka polskiego",

    # --- Instytut Języka Polskiego PAN ---
    "Instytut Języka Polskiego PAN",
    "IJP PAN strona instytutu",
    "IJP PAN badania języka polskiego",

    # --- Uniwersytet Warmińsko-Mazurski ---
    "Uniwersytet Warmińsko-Mazurski filologia polska",
    "UWM instytut literaturoznawstwa",
    "UWM instytut języka polskiego",

    # --- Uniwersytet Wrocławski ---
    "Uniwersytet Wrocławski instytut filologii polskiej",
    "UWr instytut literaturoznawstwa",
    "UWr instytut języka polskiego",

    # --- Uniwersytet Szczeciński ---
    "Uniwersytet Szczeciński filologia polska",
    "Uniwersytet Szczeciński instytut literaturoznawstwa",

    # --- UMCS ---
    "Uniwersytet Marii Curie-Skłodowskiej filologia polska",
    "UMCS instytut literaturoznawstwa",
    "UMCS instytut języka polskiego",

    # --- Uniwersytet w Białymstoku ---
    "Uniwersytet w Białymstoku filologia polska",
    "UwB instytut literaturoznawstwa",

    # --- IBL PAN ---
    "Instytut Badań Literackich PAN",
    "IBL PAN literaturoznawstwo",
    "IBL PAN strona instytutu",

    # --- Uniwersytet Opolski ---
    "Uniwersytet Opolski filologia polska",
    "Uniwersytet Opolski instytut literaturoznawstwa",

    # --- Uniwersytet Łódzki ---
    "Uniwersytet Łódzki instytut filologii polskiej",
    "UŁ literaturoznawstwo",
    "UŁ język polski instytut",

    # --- Uniwersytet Warszawski ---
    "Uniwersytet Warszawski instytut polonistyki",
    "UW instytut literatury polskiej",
    "UW instytut języka polskiego",

    # --- UAM ---
    "Uniwersytet im. Adama Mickiewicza filologia polska",
    "UAM instytut literaturoznawstwa",
    "UAM instytut języka polskiego",

    # --- Uniwersytet Jagielloński ---
    "Uniwersytet Jagielloński wydział polonistyki",
    "UJ instytut literatury polskiej",
    "UJ instytut języka polskiego",

    # --- UPH Siedlce ---
    "Uniwersytet Przyrodniczo-Humanistyczny Siedlce filologia polska",
    "UPH Siedlce literaturoznawstwo",

    # --- ATH Bielsko-Biała ---
    "Akademia Techniczno-Humanistyczna Bielsko-Biała filologia polska",

    # --- KUL ---
    "Katolicki Uniwersytet Lubelski filologia polska",
    "KUL instytut literaturoznawstwa",
    "KUL instytut języka polskiego",

    # --- UKW ---
    "Uniwersytet Kazimierza Wielkiego filologia polska",
    "UKW instytut literaturoznawstwa",

    # --- UJD Częstochowa ---
    "Uniwersytet Jana Długosza filologia polska",
    "UJD Częstochowa literaturoznawstwo",

    # --- UP Kraków ---
    "Uniwersytet Pedagogiczny Kraków filologia polska",
    "UP Kraków instytut literaturoznawstwa",
    "UP Kraków instytut języka polskiego",

    # --- Uniwersytet Rzeszowski ---
    "Uniwersytet Rzeszowski filologia polska",
    "Uniwersytet Rzeszowski literaturoznawstwo",

    # --- UKSW ---
    "Uniwersytet Kardynała Stefana Wyszyńskiego filologia polska",
    "UKSW instytut literaturoznawstwa",

    # --- Uniwersytet Pomorski ---
    "Uniwersytet Pomorski filologia polska",

    # --- Uniwersytet Zielonogórski ---
    "Uniwersytet Zielonogórski filologia polska",
    "UZ literaturoznawstwo",

    # --- Uczelnie współdzielone / mniejsze ---
    "Tarnowska Szkoła Wyższa filologia polska",
    "Akademia im. Jakuba z Paradyża filologia polska",
    "Politechnika Radomska filologia polska",
]

from ddgs import DDGS
from tqdm import tqdm
import time
import traceback

links = {}
errors = []

MAX_RETRIES = 3
SLEEP_ON_ERROR = 2  # sekundy

with DDGS() as ddgs:
    for q in tqdm(queries):
        retries = 0

        while retries < MAX_RETRIES:
            try:
                for r in ddgs.text(q, region="pl-pl"):
                    link = r.get("href")
                    if not link:
                        continue

                    links[link] = {
                        "title": r.get("title"),
                        "link": link,
                        "snippet": r.get("body"),
                        "query": q,
                    }

                # jeśli zapytanie się udało — wychodzimy z retry loop
                break

            except Exception as e:
                retries += 1

                errors.append({
                    "query": q,
                    "attempt": retries,
                    "error_type": type(e).__name__,
                    "error": str(e),
                })

                print(f"[WARN] Błąd dla zapytania: {q}")
                print(f"       Próba {retries}/{MAX_RETRIES}: {e}")

                # krótka przerwa, żeby nie dobijać backendu
                time.sleep(SLEEP_ON_ERROR)

                if retries >= MAX_RETRIES:
                    print(f"[SKIP] Pomijam zapytanie po {MAX_RETRIES} nieudanych próbach: {q}")

print(f"\nZebrano {len(links)} artykułów/list.")
print(f"Błędy: {len(errors)}")


df = pd.DataFrame(links.values())

















