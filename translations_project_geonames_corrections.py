#%% import
import pickle
import regex as re
import requests
import pandas as pd
from collections import Counter
from my_functions import marc_parser_dict_for_field, create_google_worksheet
import numpy as np
from unidecode import unidecode
import sys
sys.path.insert(1, 'C:/Users/Cezary/Documents/miasto-wies')
from geonames_accounts import geonames_users
import random
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from collections import defaultdict
from ast import literal_eval
from itertools import groupby
import gspread as gs
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

#%% def

def simplify_place_name(x):
    return ''.join([unidecode(e).lower() for e in re.findall('[\p{L}- ]',  x)]).strip()

def all_equal(iterable):
    g = groupby(iterable)
    return next(g, True) and not next(g, False)

#%% przygotowanie danych gonames

# with open('translations_places.pickle', 'rb') as f:
#     places_geonames = pickle.load(f)
# with open('translations_places_2.pickle', 'rb') as f:
#     places_geonames_2 = pickle.load(f)

# places_geonames_extra = {}
# places_geonames_extra.update({'v praze':places_geonames['praha']})
# places_geonames_extra.update({'reinbek bei hamburg':places_geonames['reinbek']})
# places_geonames_extra.update({'v ljubljani':places_geonames['ljubljana']})
# places_geonames_extra.update({'v brne':places_geonames['brno']})
# places_geonames_extra.update({'sofiia':places_geonames['sofia']})
# places_geonames_extra.update({'v bratislave':places_geonames['bratislava']})
# places_geonames_extra.update({'v gorici':places_geonames['gorizia']})
# places_geonames_extra.update({'u zagrebu':places_geonames['zagreb']})
# places_geonames_extra.update({'w budysinje':places_geonames['budisin']})
# places_geonames_extra.update({'turciansky sv martin':places_geonames['martin']})
# places_geonames_extra.update({'s-peterburg':places_geonames['st petersburg']})
# places_geonames_extra.update({'berlin ua':places_geonames['berlin']})
# places_geonames_extra.update({'nakladatelstvi ceskoslovenske akademie ved':places_geonames['praha']})
# places_geonames_extra.update({'nayi dilli':places_geonames['new delhi']})
# places_geonames_extra.update({'paul hamlyn':places_geonames['london']})
# places_geonames_extra.update({'unwin':places_geonames['london']})
# places_geonames_extra.update({'soul tukpyolsi':places_geonames['seoul']})
# places_geonames_extra.update({'v ostrave':places_geonames['ostrava']})
# places_geonames_extra.update({'ottensheim an der donau':places_geonames['ottensheim']})
# places_geonames_extra.update({'mor ostrava':places_geonames['ostrava']})
# places_geonames_extra.update({'troppau':places_geonames['opava']})
# places_geonames_extra.update({'g allen':places_geonames['london']})
# places_geonames_extra.update({'frankfurt a m':places_geonames['frankfurt am main']})
# places_geonames_extra.update({'v kosiciach':places_geonames['kosice']})
# places_geonames_extra.update({'olmutz':places_geonames['olomouc']})
# places_geonames_extra.update({'helsingissa':places_geonames['helsinki']})
# places_geonames_extra.update({'mahr-ostrau':places_geonames['ostrava']})
# places_geonames_extra.update({'v ziline':places_geonames['zilina']})
# places_geonames_extra.update({'v plzni':places_geonames['plzen']})
# places_geonames_extra.update({'artia':places_geonames['praha']})
# places_geonames_extra.update({'praha in-':places_geonames['praha']})
# places_geonames_extra.update({'klagenfurt am worthersee':places_geonames['klagenfurt']})
# places_geonames_extra.update({'prjasiv':places_geonames['presov']})
# places_geonames_extra.update({'esplugas de llobregat':places_geonames['esplugues de llobregat']})
# places_geonames_extra.update({'v celovcu':places_geonames['klagenfurt']})
# places_geonames_extra.update({'london printed in czechoslovakia':places_geonames['london']})
# places_geonames_extra.update({'warzsawa':places_geonames['warszawa']})
# places_geonames_extra.update({'tai bei xian xin dian shi':places_geonames['taiwan']})
# places_geonames_extra.update({'ciudad de mexico':places_geonames['mexico']})
# places_geonames_extra.update({'poszony':places_geonames['bratislava']})
# places_geonames_extra.update({'budysyn':places_geonames['bautzen']})
# places_geonames_extra.update({'spolek ceskych bibliofilu':places_geonames['praha']})
# places_geonames_extra.update({'v londyne':places_geonames['london']})
# places_geonames_extra.update({'korea':places_geonames['seoul']})
# places_geonames_extra.update({'madarsko':places_geonames['budapest']})
# places_geonames_extra.update({'na smichove':places_geonames['praha']})
# places_geonames_extra.update({'wien ua':places_geonames['wien']})
# places_geonames_extra.update({'hki':places_geonames['helsinki']})
# places_geonames_extra.update({'prag ii':places_geonames['praha']})
# places_geonames_extra.update({'sv praha':places_geonames['praha']})
# places_geonames_extra.update({'kassel-wilhelmshoehe':places_geonames['kassel']})
# places_geonames_extra.update({'matica slovenska':places_geonames['martin']})
# places_geonames_extra.update({'basil blackwell':places_geonames['oxford']})
# places_geonames_extra.update({'amsterodam':places_geonames['amsterdam']})
# places_geonames_extra.update({'boosey':places_geonames['london']})
# places_geonames_extra.update({'bratislave':places_geonames['bratislava']})
# places_geonames_extra.update({'evans bros':places_geonames['london']})
# places_geonames_extra.update({'fore publications':places_geonames['london']})
# places_geonames_extra.update({'g allen':places_geonames['london']})
# places_geonames_extra.update({'george sheppard':places_geonames['oxford']})
# places_geonames_extra.update({'hamburg wegner':places_geonames['hamburg']})
# places_geonames_extra.update({'heinemann':places_geonames['london']})
# places_geonames_extra.update({'hogarth press':places_geonames['london']})
# places_geonames_extra.update({'hutchinson':places_geonames['london']})
# places_geonames_extra.update({'i nicholson':places_geonames['london']})
# places_geonames_extra.update({'john lane':places_geonames['london']})
# places_geonames_extra.update({'jonathan cape':places_geonames['london']})
# places_geonames_extra.update({'kattowitz':places_geonames['katowice']})
# places_geonames_extra.update({'methuen':places_geonames['london']})
# places_geonames_extra.update({'moderschan':places_geonames['praha']})
# places_geonames_extra.update({'new english library':places_geonames['london']})
# places_geonames_extra.update({'orbis pub co':places_geonames['praha']})
# places_geonames_extra.update({'pp ix':places_geonames['london']})
# places_geonames_extra.update({'praha prag':places_geonames['praha']})
# places_geonames_extra.update({'robert anscombe':places_geonames['london']})
# places_geonames_extra.update({'spck':places_geonames['london']})
# places_geonames_extra.update({'spring books':places_geonames['london']})
# places_geonames_extra.update({'supraphon':places_geonames['praha']})
# places_geonames_extra.update({'u sisku':places_geonames['praha']})
# places_geonames_extra.update({'unwin':places_geonames['london']})
# places_geonames_extra.update({'v cheshskoi pragie':places_geonames['praha']})
# places_geonames_extra.update({'v prahe':places_geonames['praha']})
# places_geonames_extra.update({'w prazy':places_geonames['praha']})
# places_geonames_extra.update({'watson':places_geonames['london']})
# places_geonames_extra.update({'william heinemann':places_geonames['london']})
# places_geonames_extra.update({'xv paris':places_geonames['paris']})           

# places_geonames_extra.update({'aarau':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'basel':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'kopenhagen':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'hradec kralove':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'presov':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'nadlac':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'monchaltorf':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'kyonggi-do paju-si':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'hong kong':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'prishtine':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'chester springs':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'gardena':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'kisineu':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'w chosebuzu':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'bombay':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'calcutta':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'tuzla':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'prjasiv':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'hradec kralove':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'koniggratz':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'weitra':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'cairo':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'al-qahirat':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'hildesheim':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'taipei':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'tubingen':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'sibiu':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'kbh':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'aarau':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'altenmedingen':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'am heiligen berge bei olmutz':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'avon':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'bad aibling':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'bad goisern':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'bad homburg':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'bakingampeta vijayavada':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'basingstoke':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'bassac':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'bjelovar':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'boucherville':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'brandys nad labem':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'brazilio':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'chapeco':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'cormons':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'crows nest':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'csorna':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'daun':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'dinslaken':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'doran':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'east rutherford':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'englewood cliffs':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'galati':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'grenoble':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'gweru':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'haida':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'harmonds-worth':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'hoboken':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'hof':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'hof a d saal':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'horn':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'idstein':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'kbh':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'kirchseeon':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'kissingen':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'kremsier':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'la tour-daigues':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'la vergne':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'leinfelden bei stuttgart':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'leitmeritz':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'ljouvert':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'ludewig':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'melbourne':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'melhus':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'mem martins':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'mensk':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'mestecko':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'mouton':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'na prevaljah':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'neuotting am inn':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'neustadt in holstein':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'newburyport':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'nimes':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'nokis':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'novomesto':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'oradea':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'oud-gastel':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'oude-god':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'paiania':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'penguin books':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'petrovec':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'pozega':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'prace':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'purley':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'reinbek bei hamburg':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'reinbek hbg':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'remschied':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'riga':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'rotmanka':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'salzburg':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'sibiu':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'stanislawow':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'starnberg am see':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'szentendre':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'the haugue':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'tonder':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'treben':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'usti nad labem':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'v sevljusi':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'v uzgorode':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'vila do conde':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'vila nova de famalicao':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'villeneuve dascq':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'vrsac':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'vrutky':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'vught':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'w budine':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'w pesti':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'wattenheim':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'weimar':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'zurich':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'freiburg i breisgau':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'schwarz-kostelezt':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'riedstadt':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'glasgow':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'teschen':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'polock':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'brest':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'leonberg':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'warmbronn':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'dresden':{ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})                          


# places_geonames = {k:v for k,v in places_geonames.items() if isinstance(v, dict)}
# places_geonames_2 = {k:v for k,v in places_geonames_2.items() if isinstance(v, dict)}

# places_geonames.update(places_geonames_2)
# places_geonames.update(places_geonames_extra)

# with open('translations_places_all.pickle', 'wb') as handle:
#     pickle.dump(places_geonames, handle, protocol=pickle.HIGHEST_PROTOCOL)
    
with open('translations_places_all.pickle', 'rb') as f:
    places_geonames = pickle.load(f)

#%%dane bibliograficzne

#wgranie kompletnych danych
all_records_df = pd.read_excel(r"C:\Users\Cezary\Downloads\everything_merged_2022-02-24.xlsx")

#wgranie pliku ondreja
ov_records = pd.read_excel(r"translation_database_clusters_year_author_language_2022-03-14.xlsx")

#filtrowanie all po ids z pliku ov
all_records_df = all_records_df.loc[all_records_df['001'].isin(ov_records['001'])]

#wgranie aktualnego pliku
translations_df = pd.read_excel('translation_before_manual_2022-09-20.xlsx')   

#27068 -- all records
single_records = len([e for e in translations_df['group_ids'].to_list() if '❦' not in e])
# 15425 single records
grouped_records = len([e for e in translations_df['group_ids'].to_list() if '❦' in e])
#grouped_records == 43%

grouped_ids = [el for sub in [e.split('❦') for e in translations_df['group_ids'].to_list() if '❦' in e] for el in sub]
# 11643 rekordów powstało z 36749 rekordów

#%% przypisanie gonames do rekordów
country_codes = pd.read_excel('translation_country_codes.xlsx')

country_codes = [list(e[-1]) for e in country_codes.iterrows()]
country_codes = dict(zip([e[0] for e in country_codes], [{'MARC_name': e[1], 'iso_alpha_2': e[2], 'Geonames_name': e[-1]} for e in country_codes]))

places = all_records_df[['001', '008', '260']]

places['country'] = places['008'].apply(lambda x: x[15:18])
places.drop(columns='008', inplace=True)
places['country'] = places['country'].str.replace('\\', '', regex=False)
places['country_name'] = places['country'].apply(lambda x: country_codes[x]['MARC_name'] if x in country_codes else 'unknown')
places['geonames_name'] = places['country'].apply(lambda x: country_codes[x]['Geonames_name'] if x in country_codes else 'unknown')
places['places'] = places['260'].apply(lambda x: [list(e.values())[0] for e in marc_parser_dict_for_field(x, '\$') if '$a' in e] if not(isinstance( x, float)) else x)
places['places'] = places['places'].apply(lambda x: ''.join([f'$a{e}' for e in x]) if not(isinstance(x, float)) else np.nan)

places['places'] = places['places'].apply(lambda x: re.sub('( : )(?!\$)', r'$b', x) if pd.notnull(x) else x)
places['places'] = places['places'].apply(lambda x: re.sub('( ; )(?!\$)', r'$a', x) if pd.notnull(x) else x)
places['places'] = places['places'].apply(lambda x: re.sub('\d', r'$a', x) if pd.notnull(x) else x)
places['places'] = places['places'].apply(lambda x: re.sub(' - ', r'$a', x) if pd.notnull(x) else x)
places['places'] = places['places'].apply(lambda x: re.sub(' \& ', r'$a', x) if pd.notnull(x) else x)
places['places'] = places['places'].apply(lambda x: re.sub(', ', r'$a', x) if pd.notnull(x) else x)
places['places'] = places['places'].apply(lambda x: re.sub('\(', r'$a', x) if pd.notnull(x) else x)
places['places'] = places['places'].apply(lambda x: re.sub('\[', r'$a', x) if pd.notnull(x) else x)
places['places'] = places['places'].apply(lambda x: re.sub('\/', r'$a', x) if pd.notnull(x) else x)

places['places'] = places['places'].apply(lambda x: [list(e.values())[0] for e in marc_parser_dict_for_field(x, '\$') if '$a' in e] if pd.notnull(x) else x)
places['places'] = places['places'].apply(lambda x: x if x else np.nan)
#manualne korekty
places.at[places.loc[places['001'] == 561629681].index.values[0], 'places'] = ['London', 'Glasgow']
places.at[places.loc[places['001'] == 162375520].index.values[0], 'places'] = ['Dresden', 'Leipzig']
places.at[places.loc[places['001'] == 469594167].index.values[0], 'places'] = ['Düsseldorf', 'Köln']
places.at[places.loc[places['001'] == 504116129].index.values[0], 'places'] = ['London', 'New York']
places.at[places.loc[places['001'] == 809046852].index.values[0], 'places'] = ['London', 'New York']
places.at[places.loc[places['001'] == 310786855].index.values[0], 'places'] = ['Leonberg', 'Warmbronn']
places.at[places.loc[places['001'] == 804915536].index.values[0], 'places'] = ['Polock', 'Brest']
places.at[places.loc[places['001'] == 263668500].index.values[0], 'places'] = ['Praha', 'Berlin']
places.at[places.loc[places['001'] == 504310019].index.values[0], 'places'] = ['Wien', 'Teschen']
places.at[places.loc[places['001'] == 367432746].index.values[0], 'places'] = ['Wien', 'Leipzig']

places['simple'] = places['places'].apply(lambda x: [simplify_place_name(e).strip() for e in x] if not(isinstance(x, float)) else x if pd.notnull(x) else x)
places['simple'] = places['simple'].apply(lambda x: [e for e in x if e] if not(isinstance(x, float)) else np.nan)                    
                   

places['geonames'] = places['simple'].apply(lambda x: [places_geonames[e]['geonameId'] if e in places_geonames else np.nan for e in x] if not(isinstance(x, float)) else np.nan)

places['geonames'] = places['geonames'].apply(lambda x: list(set([e for e in x if pd.notnull(e)])) if isinstance(x, list) else x)
places['geonames'] = places['geonames'].apply(lambda x: x if x else np.nan)

geonames_ids = [e for e in places['geonames'].to_list() if not(isinstance(e, float))]
geonames_ids = set([e for sub in geonames_ids for e in sub if not(isinstance(e, float))])

geonames_resp = {}
# for geoname in geonames_ids:
def get_geonames_country(geoname_id):
    # geoname_id = list(geonames_ids)[0]
    user = random.choice(geonames_users)
    #w funkcję wpisać losowanie randomowego username
    try:
        geonames_resp[geoname_id] = requests.get(f'http://api.geonames.org/getJSON?geonameId={geoname_id}&username={user}').json()['countryName']
    except KeyError:
        get_geonames_country(geoname_id)

with ThreadPoolExecutor() as executor:
    list(tqdm(executor.map(get_geonames_country, geonames_ids), total=len(geonames_ids)))
    
#dodać kolumnę    
    
places['geonames_country'] = places['geonames'].apply(lambda x: [geonames_resp[e] for e in x if not(isinstance(e, float))] if not(isinstance(x, float)) else x)   
    
#dodać koordynaty
    
def get_geonames_name(geoname_id):
    # geoname_id = list(geonames_ids)[0]
    user = random.choice(geonames_users)
    #w funkcję wpisać losowanie randomowego username
    try:
        geonames_resp[geoname_id] = requests.get(f'http://api.geonames.org/getJSON?geonameId={geoname_id}&username={user}').json()['name']
    except KeyError:
        get_geonames_name(geoname_id)    
        
def get_geonames_coordinates(geoname_id):
    # geoname_id = list(geonames_ids)[0]
    user = random.choice(geonames_users)
    #w funkcję wpisać losowanie randomowego username
    try:
        response = requests.get(f'http://api.geonames.org/getJSON?geonameId={geoname_id}&username={user}').json()
        lat = response['lat']
        lng = response['lng']
        geonames_resp[geoname_id] = {'lat': lat,
                                     'lng': lng}
    except KeyError:
        get_geonames_coordinates(geoname_id) 
    
geonames_resp = {}
with ThreadPoolExecutor(max_workers=50) as executor:
    list(tqdm(executor.map(get_geonames_name, geonames_ids), total=len(geonames_ids)))
places['geonames_place_name'] = places['geonames'].apply(lambda x: [geonames_resp[e] for e in x if not(isinstance(e, float))] if not(isinstance(x, float)) else x)   

geonames_resp = {}
with ThreadPoolExecutor(max_workers=50) as executor:
    list(tqdm(executor.map(get_geonames_coordinates, geonames_ids), total=len(geonames_ids)))
places['geonames_lat'] = places['geonames'].apply(lambda x: [geonames_resp[e]['lat'] for e in x if not(isinstance(e, float))] if not(isinstance(x, float)) else x)   
places['geonames_lng'] = places['geonames'].apply(lambda x: [geonames_resp[e]['lng'] for e in x if not(isinstance(e, float))] if not(isinstance(x, float)) else x)                              

places_full = places.copy() 
places = places[['001', 'geonames', 'geonames_place_name', 'geonames_country', 'geonames_lat', 'geonames_lng']].rename(columns={'geonames':'geonames_id','geonames_place_name':'geonames_name'})
for column in places.columns[1:]:
    places[column] = places[column].apply(lambda x: tuple(x) if isinstance(x, list) else x)

places = places.drop_duplicates()

places_grouped = places.groupby('001')
places_new = pd.DataFrame()
for name, group in tqdm(places_grouped, total=len(places_grouped)):
    # name = 3719272
    # group = places_grouped.get_group(name)
    if group.shape[0] > 1:
        group = group.loc[group['geonames_id'].notnull()]
        places_new = pd.concat([places_new, group])
    else:
        places_new = pd.concat([places_new, group])

for column in places_new.columns[1:]:
    places_new[column] = places_new[column].apply(lambda x: list(x) if isinstance(x, tuple) else x)
        
test = places_full.loc[places_full['001'].isin([e[0] for e in Counter(places_new['001']).most_common(118)])]
test2 = places_new.loc[places_new['001'].isin([e[0] for e in Counter(places_new['001']).most_common(118)])]

test2_grouped = test2.groupby('001')
test2 = {}
for name, group in tqdm(test2_grouped, total=len(test2_grouped)):
    # name = 2973189
    # group = test2_grouped.get_group(name).to_dict(orient='index')
    group = group.to_dict(orient='index')
    c = Counter()
    for d in group.values():
        c.update(d)
    c['001'] = name
    geo_ids = [e for e in Counter(c.get('geonames_id'))]
    geo_indices = [c.get('geonames_id').index(e) for e in geo_ids]
        
    for k,v in c.items():
        if isinstance(v, list):
            v = [e for i, e in enumerate(v) if i in geo_indices]
            c[k] = v
    group = {c.get('001'): {k:v for k,v in c.items() if k != '001'}}
    test2.update(group)
    
test2 = pd.DataFrame.from_dict(test2, orient='index').reset_index().rename(columns={'index':'001'})

places = places_new.loc[~places_new['001'].isin(test2['001'])]
places = pd.concat([places, test2]).sort_values('001').reset_index(drop=True)

#ile rekordów
places.shape[0] #54926
#ma gonames
places.loc[places['geonames_id'].notnull()].shape[0] #51498; 93.7%
#problematyczne rekordy:
    #	1015947429


#!!!             UWAGA!!!!!!!!
# !!!dla tych 6% jeśli można wskazać kraj, to wybieramy stolicę!!!

#%% porównanie geonames coverage
    
records_groups_dict = dict(zip(translations_df['001'].to_list(), [e.split('❦') for e in translations_df['group_ids'].to_list()]))
records_groups_multiple_dict = {k:v for k,v in records_groups_dict.items() if len(v) > 1}

places_dict = places.copy()
places_dict.index = places['001']
places_dict.drop(columns='001',inplace=True)
places_dict = places_dict.to_dict(orient='index')

#porównanie miejsc dla zgrupowanych rekordów

# tylko nazwy

places_compared = {k:[places_dict.get(int(e)).get('geonames_name') for e in v] for k,v in records_groups_multiple_dict.items()}

different_places = {k:v for k,v in places_compared.items() if not all_equal(v)}

different_places_ids = {k:records_groups_multiple_dict.get(k) for k,v in different_places.items()}
# 3112 grup ma różne miejsca wydania

records_to_check_df = pd.DataFrame()
for k,v in tqdm(different_places_ids.items()):
    test_df_geo = places.loc[places['001'].isin([int(e) for e in v])]
    test_df = all_records_df.loc[all_records_df['001'].isin([int(e) for e in v])][['001', '008', '100', '245', '260']]
    test_df = pd.merge(test_df, test_df_geo, on='001', how='inner')
    test_df.insert(loc=0, column='group', value=k)
    records_to_check_df = pd.concat([records_to_check_df, test_df])
records_to_check_df['to separate'] = None

# 3112 grup przekłada się na 13139

#%%
gc = gs.oauth()
gauth = GoogleAuth()
gauth.LocalWebserverAuth()
drive = GoogleDrive(gauth)

sheet = gc.create('deduplicated_records_to_be_checked_DRAFT', '1YLfF5NyFVXC6NYpp-WhjGxMDxqXn8FT3')
create_google_worksheet(sheet.id, 'deduplicated_records_to_be_checked_DRAFT', records_to_check_df)
#%%


all_equal(places_compared.get(54194849))
all_equal(places_compared.get(52388693))
all_equal(places_compared.get(53455645))
# wszystkie dane




#current records with places
dict(zip(records_groups_multiple_dict.keys(), ))

translations_df_places = translations_df.loc[translations_df['001'].isin(records_groups_multiple_dict.keys())][['001', 'geonames_id', 'geonames_name', 'geonames_country', 'geonames_lat', 'geonames_lng']]
for column in translations_df_places.columns[1:]:
    translations_df_places[column] = translations_df_places[column].apply(lambda x: literal_eval(x) if not isinstance(x,float) else x)
translations_df_places.index = translations_df_places['001']
translations_df_places.drop(columns='001',inplace=True)
translations_df_places = translations_df_places.to_dict(orient='index')
#tu mam obecne miejsca
translations_df_places = {k:tuple({ka:tuple(va) if isinstance(va,list) else va for ka,va in v.items()}.items()) for k,v in translations_df_places.items()}

#tu mam zbudoawać uzupełnione miejsca, a potem porównać




test = places.loc[places['001'] == 9925751]



translations_df.columns.values

records_groups_with_places


















