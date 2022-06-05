import os
import sys
import sqlite3
import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from etl import helper
from schemas.stream import stream_schema

def extract(path:str) -> pd.DataFrame:
    """
    Extract data from JSON files
    Parameters:
        path (str): Path to JSON file
    Returns:
        result (pd.DataFrame): A dataframe with the ectracted data.
    """
    json_struct = helper.read_json(path)
    return pd.json_normalize(json_struct)

def validate_data(df: pd.DataFrame, invalid_data_path:str) -> pd.DataFrame :
    """
    Validates the df data against a predefined schema. Data (Rows), that does not match the schema definition,
    getting extracted and safed in the 'invalid' data bucked for inspection. 
    Parameters:
        df (pd.DataFrame): Incomming data
        invalid_data_path (str): Data path to 'invalid' data bucked
    Returns:
        result (pd.DataFrame): Validated dataframe
    """
    required_columns = stream_schema.get_column_names()
    df = helper.add_required_columns(df=df,required_columns=required_columns)
    errors = stream_schema.validate(df=df, columns=required_columns)
    invalide_rows = [e.row for e in errors]
    helper.extract_invalid_rows(df=df, path=invalid_data_path, invalide_rows=invalide_rows )
    df = helper.drop_invalid_rows(df=df, invalide_rows=invalide_rows)
    return df

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the data in order to fit the database schema
    Parameters:
        df (pd.DataFrame): Incomming data
    Returns:
        result (pd.DataFrame): Transformed dataframe
    """
    # clean up column names
    df = df.rename(columns=lambda columnName: columnName.replace("track_metadata.","").replace("additional_info.",""))

    # Set duration in ms
    df['track_duration_ms'] = df["duration_ms"].fillna(df["duration"]*10).fillna(df["track_length"]*10)
    
    # Set track number
    df['track_number'] = df["tracknumber"].fillna(df["track_number"])

     # Remove all after and including '('
    df['track_name'] = df['track_name'].str.replace(r'\(.*', '', regex=True)

    # Create hash for track_msid
    keys_to_hash: set = ["track_name", "artist_msid"]
    df['track_msid'] =  helper.make_hash_column(df,keys_to_hash)

    # Set unknown release
    df.loc[df[['release_msid', 'release_name']].isna().all(axis=1), 'release_name'] = "Unknown"
    df['release_msid'] = df["release_msid"].fillna("xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx")

     # Get Year from release
    df['release_date'] = df["date"].str[-4:]    

    # Create hash for listened_id
    keys_to_hash: set = ["track_msid", "listened_at", "user_name","dedup_tag"]
    df['listened_id'] =  helper.make_hash_column(df,keys_to_hash)

    # Create time table data
    df['timestamp_utc']= pd.to_datetime(df['listened_at'], unit='s', utc=True)
    df['weekday']= df['timestamp_utc'].dt.day_name()
    df['day_of_week']= df['timestamp_utc'].dt.dayofweek
    df['week_of_year']= df['timestamp_utc'].dt.isocalendar().week
    df['date'] = df['timestamp_utc'].dt.date
    df['time'] = df['timestamp_utc'].dt.time.apply(str)
    df['day_of_month'] = df['timestamp_utc'].dt.day
    df['year'] = df['timestamp_utc'].dt.year
    df['month'] = df['timestamp_utc'].dt.month
    df['timestamp_utc'] = df['timestamp_utc'].apply(str)

    return df  


def load(df: pd.DataFrame, db_conn:sqlite3.Connection):
    """
    Load the data into the database
    Parameters:
        df (pd.DataFrame): Incomming data
        db_conn (sqlite3.Connection): Database connection
    """
    c = db_conn.cursor()

    # Load into artist
    sql = """INSERT INTO artist (artist_msid, artist_name) VALUES(?,?) 
             ON CONFLICT(artist_msid) DO NOTHING"""
    records = df[["artist_msid","artist_name"]].values.tolist()

    c.executemany(sql,records)		
    db_conn.commit()

    # Load into track
    sql = """INSERT INTO track (track_msid, track_name, track_number, disc_number, track_duration_ms) VALUES(?,?,?,?,?) 
             ON CONFLICT(track_msid) DO NOTHING"""
    records = df[["track_msid","track_name", "track_number", "discnumber", "track_duration_ms"]].values.tolist()
    c.executemany(sql,records)
    db_conn.commit()

    # Load into release
    sql = """INSERT INTO release (release_msid, release_name, total_discs, total_tracks, release_date) VALUES(?,?,?,?,?) 
             ON CONFLICT(release_msid) DO NOTHING """
    records = df[["release_msid","release_name","totaldiscs","totaltracks","release_date"]].values.tolist()
    c.executemany(sql,records)
    db_conn.commit()

    # Load into time
    sql = """INSERT INTO time (timestamp_unix_id, timestamp_utc, date, time, weekday, day_of_week, day, week, month, year ) VALUES(?,?,?,?,?,?,?,?,?,?) 
              ON CONFLICT(timestamp_unix_id) DO NOTHING """
    records = df[["listened_at","timestamp_utc","date", "time", "weekday", "day_of_week", "day_of_month", "week_of_year", "month", "year"]].values.tolist()
    c.executemany(sql,records)
    db_conn.commit()

    # Load into listened_fact
    sql = """INSERT INTO listened_fact (listened_id, track_msid, artist_msid, release_msid, timestamp_unix_id, user_name, dedup_tag) VALUES(?,?,?,?,?,?,?) 
             ON CONFLICT(listened_id) DO NOTHING """
    records = df[["listened_id","track_msid","artist_msid", "release_msid", "listened_at", "user_name", "dedup_tag"]].values.tolist()
    c.executemany(sql,records)
    db_conn.commit()
    c.close()
