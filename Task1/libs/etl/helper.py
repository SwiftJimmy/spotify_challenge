from datetime import datetime
import hashlib
import json
import sqlite3
import pandas as pd
from pandas_schema.validation_warning import ValidationWarning

def readJSON(path:str):
    try:
        file = open(path)
        return json.load(file)
    except json.JSONDecodeError as err:
        return read_JSON_Line_By_Line(path)

def read_JSON_Line_By_Line(path:str) -> list:
    record_list = []
    with open(path) as f:
        for jsonObj in f:
            record_dict = json.loads(jsonObj)
            record_list.append(record_dict)
    return record_list
    
def make_hash_column(df: pd.DataFrame,key_list: set) -> pd.DataFrame:
    return pd.DataFrame(df[key_list].applymap(str).values.sum(axis=1))[0].str.encode('utf-8').apply(lambda x: (hashlib.md5(x).hexdigest()))

def addRequiredColumns(requireColumns:set, df:pd.DataFrame) -> pd.DataFrame:
    for column in set(requireColumns).difference(df.columns):
        df = df.insert(len(df.columns), column, None)
    df = df[requireColumns]
    df = df.loc[:,~df.columns.duplicated()] # [True, False] = ~[False,True]
    return df


def extract_invalid_rows(invalide_rows:list[int], df:pd.DataFrame, path:str):
    if invalide_rows:
        current_timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S.%f")
        file_path = f"{path}/{current_timestamp}.json"
        df.iloc[invalide_rows].to_json(file_path, orient="records" )

def drop_invalid_rows(invalide_rows:list[int], df:pd.DataFrame) -> pd.DataFrame:
    return df.drop(index=invalide_rows)

