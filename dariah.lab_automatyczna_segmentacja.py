pliki hocr artykułów tutaj: https://drive.google.com/file/d/1HtJdNdfXn4Ih_CRAy0CfCqze_43THEzq/view?usp=drive_link
przygotowanie kodu w Pythonie, który pracuje na usłudze BB i pozyskuje abstrakty dla tekstów → https://converter-hocr.services.clarin-pl.eu/docs
spakowane abstrakty w formacie .zip przesłać do usługi https://services.clarin-pl.eu/login i pobrać JSON z wynikami

from glob import glob
import requests


path = r"D:\IBL\Biblioteka Nauki\Dariah.lab hOCR/"
files = [f for f in glob(f"{path}*", recursive=True)]


url = 'https://converter-hocr.services.clarin-pl.eu/docs'

url = 'https://converter-hocr.services.clarin-pl.eu/convert/'

def get_radon_info_for_person(person_id):
    url = 'https://radon.nauka.gov.pl/opendata/scientist/search'
    
    body={
      "resultNumbers": 1,
      "token": None,
      "body": {
        "uid": person_id,
        "firstName": None,
        "lastName": None,
        "employmentMarker": None,
        "employmentStatusMarker": None,
        "activePenaltyMarker": "No",
        "calculatedEduLevel": None,
        "academicDegreeMarker": None,
        "academicTitleMarker": None,
        "dataSources": None,
        "lastRefresh": None
      }}
    response=requests.post(url,  json=body).json()
    radon_response.update({person_id: response.get('results')})