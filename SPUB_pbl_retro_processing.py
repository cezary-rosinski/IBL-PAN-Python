import xml.etree.ElementTree as et
import requests
import pandas as pd
import re
import numpy as np
import pymarc
import io
from bs4 import BeautifulSoup
from my_functions import cosine_sim_2_elem, marc_parser_1_field, gsheet_to_df, xml_to_mrk, cSplit, f, df_to_mrc, mrc_to_mrk, mrk_to_df
import glob
import regex
import unidecode
import pandasql
import time
import sandals
from xml.sax import SAXParseException

list_of_gsheets = [('1bZ03Bb0iAQiYBddax6eS2jHBxiTfiWwFU9mWMEGmKo4', 'df_marc21_60-61'),
                   ('1fHjL4gFBkcx3cW5n4dka2y_3WzRjs2PpIsFkf950BlA', 'df_marc21_62-63'),
                   ('1IBzoo_ly9o-1Quu7zDcXxMl04USSZDSM-_ob2ZGLkf0', 'df_marc21_64'),
                   ('1yN82HVGh87IismjP4LNUNfBk0iaGmaDqkFnIUXI-wos', 'df_marc21_65'),
                   ('1pQIxMSctO68oGwER80vvtFBK8JeruftlHFSWQv0Ibx4', 'df_marc21_66'),
                   ('1E30OiGEexdpqDk3u0AeGZmT7Jjw9gzU1-5-qeWpup3Q', 'df_marc21_67')]

full_df = pd.DataFrame()
for table, sheet in list_of_gsheets:
    df = gsheet_to_df(table, sheet)
    full_df = full_df.append(df)

full_df = full_df.fillna(value=np.nan)

df_to_mrc(full_df, '❦', 'pbl_retro.mrc')
mrc_to_mrk('pbl_retro.mrc', 'pbl_retro.mrk')


