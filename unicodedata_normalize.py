'''
Credentials: Patryk Hubar, Nikodem Wołczuk, Dariusz Perliński

Supported data types:
    - str
    - pandas dataframe
    - list
    - dict
    
Normalization modes:
    - NFD - canonical decomposition
    - NFC - first applies a canonical decomposition, then composes pre-combined characters again
    - NFKD - compatibility decomposition
    - NFKC -  applies the compatibility decomposition, followed by the canonical composition
'''

import unicodedata as ud
import json
import pandas as pd


def decode_str(str_input: str, mode = "NFC") -> str:
    try:
        return ud.normalize(mode, str_input)
    except TypeError:
        return str_input
    
def decode_df(df_input: pd.core.frame.DataFrame, mode = "NFC") -> pd.core.frame.DataFrame:
    output_df = pd.DataFrame()
    for col in df_input.columns:
        col_normalized = [decode_str(value, mode) for value in df_input[col]]
        output_df[col] = col_normalized
    return output_df

def decode_list(list_input: list, mode = "NFC") -> list:
    # list_input = [[['twórcy']]]
    output_list = []
    for elem in list_input:
        print(elem)
        if type(elem) == str:
            output_list.append(decode_str(elem))
        elif type(elem) == list:
            output_list.append(decode_list(elem))
        elif type(elem) == dict:
            output_list.append(decode_dict(elem))
        elif type(elem) == pd.core.frame.DataFrame:
            output_list.append(decode_df(elem))   
        else: output_list.append(elem)
    print((output_list, id(output_list)))
    return output_list

def decode_dict(dict_input: dict, mode = "NFC") -> dict:
    output_dict = {}
    for key, value in dict_input.items():
        if type(value) == str:
            output_dict[decode_str(key)] = decode_str(value)
        elif type(value) == list:
            output_dict[decode_str(key)] = decode_list(value)
        elif type(value) == dict:
            output_dict[decode_str(key)] = decode_dict(value)
        elif type(value) == pd.core.frame.DataFrame:
            output_dict[decode_str(key)] = decode_df(value)
        else: output_dict[decode_str(key)] = value
    return output_dict

def compose_data(data, mode = "NFC"):
    data_type = type(data)
    if data_type == str:
        return decode_str(data)
    elif data_type == list:
        return decode_list(data)
    elif data_type == dict:
        return decode_dict(data)
    elif data_type == pd.core.frame.DataFrame:
        return decode_df(data)
    else:
        raise Exception("Not supported data type!")