# https://github.com/ajnyga/tsvConverter
import pandas as pd
import regex as re
import datetime
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time
from selenium.common.exceptions import NoSuchElementException, ElementNotInteractableException, NoAlertPresentException, SessionNotCreatedException, ElementClickInterceptedException, InvalidArgumentException, StaleElementReferenceException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import numpy as np
from tqdm import tqdm
import xml.etree.ElementTree as ET

pd.options.display.max_colwidth = 10000

#%%

table_path = r"C:\Users\Cezary\Documents\jupyter-lab\OJS_test\FP33-34.xlsx"
xml_path = r"C:\Users\Cezary\Documents\jupyter-lab\OJS_test\test_output/"

xml_example = r"C:\Users\Cezary\Downloads\native-20240411-150828-issues-85.xml"

tree = ET.parse(xml_example)
root = tree.getroot()


for child in root:
    print(child.tag, child.attrib)
    for gchild in child:
        print("  " + gchild.tag, gchild.attrib)
        for ggchild in gchild:
            print("    " + ggchild.tag, ggchild.attrib)
            for gggchild in ggchild:
                print("      " + gggchild.tag, gggchild.attrib)



# {http://pkp.sfu.ca}id {'type': 'internal', 'advice': 'ignore'}
# {http://pkp.sfu.ca}description {'locale': 'en_US'}
# {http://pkp.sfu.ca}description {'locale': 'pl_PL'}
# {http://pkp.sfu.ca}issue_identification {}
#   {http://pkp.sfu.ca}number {}
#   {http://pkp.sfu.ca}year {}
#   {http://pkp.sfu.ca}title {'locale': 'en_US'}
#   {http://pkp.sfu.ca}title {'locale': 'pl_PL'}
# {http://pkp.sfu.ca}date_published {}
# {http://pkp.sfu.ca}last_modified {}
# {http://pkp.sfu.ca}sections {}
#   {http://pkp.sfu.ca}section {'ref': 'TR', 'seq': '2', 'editor_restricted': '0', 'meta_indexed': '1', 'meta_reviewed': '1', 'abstracts_not_required': '0', 'hide_title': '0', 'hide_author': '0', 'abstract_word_count': '0'}
#     {http://pkp.sfu.ca}id {'type': 'internal', 'advice': 'ignore'}
#     {http://pkp.sfu.ca}abbrev {'locale': 'en_US'}
#     {http://pkp.sfu.ca}abbrev {'locale': 'pl_PL'}
#     {http://pkp.sfu.ca}title {'locale': 'en_US'}
#     {http://pkp.sfu.ca}title {'locale': 'pl_PL'}
#   {http://pkp.sfu.ca}section {'ref': 'PRA', 'seq': '4', 'editor_restricted': '0', 'meta_indexed': '1', 'meta_reviewed': '1', 'abstracts_not_required': '0', 'hide_title': '0', 'hide_author': '0', 'abstract_word_count': '0'}
#     {http://pkp.sfu.ca}id {'type': 'internal', 'advice': 'ignore'}
#     {http://pkp.sfu.ca}abbrev {'locale': 'en_US'}
#     {http://pkp.sfu.ca}abbrev {'locale': 'pl_PL'}
#     {http://pkp.sfu.ca}title {'locale': 'en_US'}
#     {http://pkp.sfu.ca}title {'locale': 'pl_PL'}
#   {http://pkp.sfu.ca}section {'ref': 'KRY', 'seq': '8', 'editor_restricted': '0', 'meta_indexed': '1', 'meta_reviewed': '1', 'abstracts_not_required': '0', 'hide_title': '0', 'hide_author': '0', 'abstract_word_count': '0'}
#     {http://pkp.sfu.ca}id {'type': 'internal', 'advice': 'ignore'}
#     {http://pkp.sfu.ca}abbrev {'locale': 'en_US'}
#     {http://pkp.sfu.ca}abbrev {'locale': 'pl_PL'}
#     {http://pkp.sfu.ca}title {'locale': 'en_US'}
#     {http://pkp.sfu.ca}title {'locale': 'pl_PL'}
# {http://pkp.sfu.ca}covers {}
#   {http://pkp.sfu.ca}cover {'locale': 'pl_PL'}
#     {http://pkp.sfu.ca}cover_image {}
#     {http://pkp.sfu.ca}cover_image_alt_text {}
#     {http://pkp.sfu.ca}embed {'encoding': 'base64'}
# {http://pkp.sfu.ca}issue_galleys {'{http://www.w3.org/2001/XMLSchema-instance}schemaLocation': 'http://pkp.sfu.ca native.xsd'}
# {http://pkp.sfu.ca}articles {'{http://www.w3.org/2001/XMLSchema-instance}schemaLocation': 'http://pkp.sfu.ca native.xsd'}
#   {http://pkp.sfu.ca}article {'locale': 'pl_PL', 'date_submitted': '2024-02-06', 'status': '3', 'submission_progress': '0', 'current_publication_id': '72050', 'stage': 'production'}
#     {http://pkp.sfu.ca}id {'type': 'internal', 'advice': 'ignore'}
#     {http://pkp.sfu.ca}submission_file {'id': '94945', 'created_at': '2024-02-06', 'date_created': '', 'file_id': '86470', 'stage': 'proof', 'updated_at': '2024-02-06', 'viewable': 'false', 'genre': 'Tekst artykułu', 'uploader': 'cezaryrosinski', '{http://www.w3.org/2001/XMLSchema-instance}schemaLocation': 'http://pkp.sfu.ca native.xsd'}
#       {http://pkp.sfu.ca}name {'locale': 'pl_PL'}
#       {http://pkp.sfu.ca}file {'id': '86470', 'filesize': '779483', 'extension': 'pdf'}
#     {http://pkp.sfu.ca}submission_file {'id': '94946', 'created_at': '2024-02-06', 'date_created': '', 'file_id': '86471', 'stage': 'proof', 'updated_at': '2024-02-06', 'viewable': 'false', 'genre': 'Tekst artykułu', 'uploader': 'cezaryrosinski', '{http://www.w3.org/2001/XMLSchema-instance}schemaLocation': 'http://pkp.sfu.ca native.xsd'}
#       {http://pkp.sfu.ca}name {'locale': 'pl_PL'}
#       {http://pkp.sfu.ca}file {'id': '86471', 'filesize': '767702', 'extension': 'pdf'}
#     {http://pkp.sfu.ca}submission_file {'id': '94947', 'created_at': '2024-02-06', 'date_created': '', 'file_id': '86470', 'stage': 'submission', 'updated_at': '2024-02-06', 'viewable': 'true', 'genre': 'Tekst artykułu', 'source_submission_file_id': '94945', 'uploader': 'cezaryrosinski', '{http://www.w3.org/2001/XMLSchema-instance}schemaLocation': 'http://pkp.sfu.ca native.xsd'}
#       {http://pkp.sfu.ca}name {'locale': 'pl_PL'}
#       {http://pkp.sfu.ca}file {'id': '86470', 'filesize': '779483', 'extension': 'pdf'}
#     {http://pkp.sfu.ca}submission_file {'id': '94948', 'created_at': '2024-02-06', 'date_created': '', 'file_id': '86471', 'stage': 'submission', 'updated_at': '2024-02-06', 'viewable': 'true', 'genre': 'Tekst artykułu', 'source_submission_file_id': '94946', 'uploader': 'cezaryrosinski', '{http://www.w3.org/2001/XMLSchema-instance}schemaLocation': 'http://pkp.sfu.ca native.xsd'}
#       {http://pkp.sfu.ca}name {'locale': 'pl_PL'}
#       {http://pkp.sfu.ca}file {'id': '86471', 'filesize': '767702', 'extension': 'pdf'}
#     {http://pkp.sfu.ca}publication {'locale': 'pl_PL', 'version': '1', 'status': '3', 'url_path': '', 'seq': '1', 'date_published': '2023-12-30', 'section_ref': 'KRY', 'access_status': '0', '{http://www.w3.org/2001/XMLSchema-instance}schemaLocation': 'http://pkp.sfu.ca native.xsd'}
#       {http://pkp.sfu.ca}id {'type': 'internal', 'advice': 'ignore'}
#       {http://pkp.sfu.ca}id {'type': 'doi', 'advice': 'update'}
#       {http://pkp.sfu.ca}title {'locale': 'en_US'}
#       {http://pkp.sfu.ca}title {'locale': 'pl_PL'}
#       {http://pkp.sfu.ca}abstract {'locale': 'en_US'}
#       {http://pkp.sfu.ca}abstract {'locale': 'pl_PL'}
#       {http://pkp.sfu.ca}licenseUrl {}
#       {http://pkp.sfu.ca}copyrightHolder {'locale': 'en_US'}
#       {http://pkp.sfu.ca}copyrightHolder {'locale': 'pl_PL'}
#       {http://pkp.sfu.ca}copyrightYear {}
#       {http://pkp.sfu.ca}keywords {'locale': 'pl_PL'}
#       {http://pkp.sfu.ca}keywords {'locale': 'en_US'}
#       {http://pkp.sfu.ca}languages {'locale': 'pl_PL'}
#       {http://pkp.sfu.ca}languages {'locale': 'en_US'}
#       {http://pkp.sfu.ca}authors {'{http://www.w3.org/2001/XMLSchema-instance}schemaLocation': 'http://pkp.sfu.ca native.xsd'}
#       {http://pkp.sfu.ca}article_galley {'locale': 'pl_PL', 'url_path': '', 'approved': 'false', '{http://www.w3.org/2001/XMLSchema-instance}schemaLocation': 'http://pkp.sfu.ca native.xsd'}
#       {http://pkp.sfu.ca}article_galley {'locale': 'en_US', 'url_path': '', 'approved': 'false', '{http://www.w3.org/2001/XMLSchema-instance}schemaLocation': 'http://pkp.sfu.ca native.xsd'}
#       {http://pkp.sfu.ca}citations {}
#       {http://pkp.sfu.ca}pages {}
  





















#%%
# plik z tabelą
# folder na pliki xml

# artykuły --> słownik
# autorzy --> słownik
# pliki --> słownik

# budowa XML
# na początku XMLa: fwrite ($xmlfile,"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n");
#             fwrite ($xmlfile,"<issues xmlns=\"http://pkp.sfu.ca\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://pkp.sfu.ca native.xsd\">\r\n")

# pętla dla artykułów
# kodowanie plików PDF w kodowaniu base64