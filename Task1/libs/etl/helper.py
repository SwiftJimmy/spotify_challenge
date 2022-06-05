import datetime
import hashlib
import json
import pandas as pd

def read_json(path:str) -> dict:
    """
    Try to read JSON data, either as one compleate JSON structure or line by line
    Parameters:
        path (str): Path to JSON file
    Returns:
        result (dict): JSON Data as dict
    """
    try:
        file = open(path)
        return json.load(file)
    except json.JSONDecodeError as err:
        return read_json_line_by_line(path)


def read_json_line_by_line(path:str) -> dict:
    """
    Try to read JSON data line by line
    Parameters:
        path (str): Path to JSON file
    Returns:
        result (dict): JSON Data as dict
    """
    record_list = []
    with open(path) as f:
        for jsonObj in f:
            record_dict = json.loads(jsonObj)
            record_list.append(record_dict)
    return record_list
    

def make_hash_column(df:pd.DataFrame, columns:set) -> pd:
    """
    Creates a hash md5-hex-digit hash based on the values provided df columns
    Parameters:
        df (pd.DataFrame): Dataframe 
        columns (set): Columns of the df, used to generate the hash from
    Returns:
        df (pd.Series): Column with hash values
    """
    return pd.DataFrame(df[columns].applymap(str).values.sum(axis=1))[0].str.encode('utf-8').apply(lambda x: (hashlib.md5(x).hexdigest()))


def add_required_columns(df:pd.DataFrame, required_columns:set) -> pd.DataFrame:
    """
    Adds the expected columns to df and fills them with None values.
    Parameters:
        df (pd.DataFrame): Dataframe 
        requireColumns (set): Minimal set of columns that are required.
    Returns:
        df (pd.DataFrame): Transformed dataframe 
    """
    for column in set(required_columns).difference(df.columns):
        df[column] = None
    df = df[required_columns]
    df = df.loc[:,~df.columns.duplicated()] # [True, False] = ~[False,True]
    return df


def drop_invalid_rows(df:pd.DataFrame, invalide_rows:set[int]) -> pd.DataFrame:
    """
    Drops invalide rows from the dataframe.
    Parameters:
        df (pd.DataFrame): Dataframe 
        invalide_rows (set): Set of invalide row index
    Returns:
        df (pd.DataFrame): Cleaned dataframe
    """
    return df.drop(index=invalide_rows)

def extract_invalid_rows(df:pd.DataFrame, path:str, invalide_rows:set[int], ):
    """
    Extracts invalide rows from the dataframe and writes them into a json file for later inspection. 
    Parameters:
        df (pd.DataFrame): Dataframe 
        invalide_rows (set): Set of invalide row index
    """
    if invalide_rows:
        current_timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S.%f")
        file_path = f"{path}/{current_timestamp}.json"
        df.iloc[invalide_rows].to_json(file_path, orient="records" )
