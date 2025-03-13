from requests import get

doi = '10.14746/fp.2016.3.26703' #0
doi = '10.14746/fp.2020.20.24906'
oc_token = 'c18c68a7-9b88-47a6-b189-e8638002b0f2'

API_CALL = f"https://opencitations.net/index/api/v2/references/doi:{doi}"
API_CALL = "https://opencitations.net/index/api/v2/citations/doi:{doi}"
API_CALL = "https://opencitations.net/index/api/v2/reference-count/doi:{doi}"
API_CALL = "https://w3id.org/oc/meta/api/v1/metadata/doi:{doi}"
HTTP_HEADERS = {"authorization": oc_token}

test = get(API_CALL, headers=HTTP_HEADERS).json()




from requests import get

API_CALL = "https://opencitations.net/index/api/v2/references/doi:10.1186/1756-8722-6-59"
API_CALL = "https://opencitations.net/index/api/v2/citations/doi:10.1186/1756-8722-6-59"
API_CALL = "https://opencitations.net/index/api/v2/reference-count/doi:10.1186/1756-8722-6-59"
API_CALL = "https://opencitations.net/index/api/v2/metadata/doi:10.1186/1756-8722-6-59"

API_CALL = "https://w3id.org/oc/meta/api/v1/metadata/doi:10.1007/978-1-4020-9632-7"
HTTP_HEADERS = {"authorization": oc_token}

get(API_CALL, headers=HTTP_HEADERS)

test = get(API_CALL, headers=HTTP_HEADERS).json()
