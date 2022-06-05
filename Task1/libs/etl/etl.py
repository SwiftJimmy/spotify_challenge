import sys
  
# setting path
sys.path.append('/Users/clemensbrauer/Desktop/Coding/Python/assesment/libs')
  
# importing
import sqlite3
import pandas as pd
import helper as helper
from schemas.stream import stream_schema


INCOMINGDATAPATH = "/Users/clemensbrauer/Desktop/Coding/Python/assesment/data/ingest/dataset.txt"
INVALIDEDATAPATH = "/Users/clemensbrauer/Desktop/Coding/Python/assesment/data/invalide"


def extract(path:str) -> pd.DataFrame:
    json_struct = helper.readJSON(path)
    return pd.json_normalize(json_struct)

def transform(df: pd.DataFrame) -> pd.DataFrame:
    
    # clean up column names
    df = df.rename(columns=lambda columnName: columnName.replace("track_metadata.","").replace("additional_info.",""))

    # 
    requiredColumns = stream_schema.get_column_names()
    df = helper.addRequiredColumns(requiredColumns, df)

    # Validate Data
    errors = stream_schema.validate(df)
    invalide_rows = [e.row for e in errors]
    helper.extract_invalid_rows(invalide_rows=invalide_rows, df=df, path=INVALIDEDATAPATH)
    df = helper.drop_invalid_rows(invalide_rows=invalide_rows, df=df)

            
    # Set duration in ms
    df['track_duration_ms'] = df["duration_ms"].fillna(df["duration"]*10).fillna(df["track_length"]*10)
    
    # Set track number
    df['track_number'] = df["tracknumber"].fillna(df["track_number"])

     # Set track number
    df['release_date'] = df["date"].str[-4:]

    # Set unknown release
    df.loc[df[['release_msid', 'release_name']].isna().all(axis=1), 'release_name'] = "Unknown"
    df['release_msid'] = df["release_msid"].fillna("xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx")

    # Create hash
    keys_to_hash: set = ["track_name", "artist_msid"]
    df['track_msid'] =  helper.make_hash_column(df,keys_to_hash)

    # Create hash
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


def load(df: pd.DataFrame):
    conn= sqlite3.connect('database/spotify.db')

    c = conn.cursor()

    sql = """INSERT INTO artist (artist_msid, artist_name) VALUES(?,?) 
             ON CONFLICT(artist_msid) DO NOTHING"""
    records = df[["artist_msid","artist_name"]].values.tolist()

    c.executemany(sql,records)		
    conn.commit()


    sql = """INSERT INTO track (track_msid, track_name, track_number, disc_number, track_duration_ms) VALUES(?,?,?,?,?) 
             ON CONFLICT(track_msid) DO NOTHING"""
    records = df[["track_msid","track_name", "track_number", "discnumber", "track_duration_ms"]].values.tolist()
    c.executemany(sql,records)
    conn.commit()


    sql = """INSERT INTO release (release_msid, release_name, total_discs, total_tracks, release_date) VALUES(?,?,?,?,?) 
             ON CONFLICT(release_msid) DO NOTHING """
    records = df[["release_msid","release_name","totaldiscs","totaltracks","release_date"]].values.tolist()
    c.executemany(sql,records)
    conn.commit()


    sql = """INSERT INTO time (timestamp_unix_id, timestamp_utc, date, time, weekday, day_of_week, day, week, month, year ) VALUES(?,?,?,?,?,?,?,?,?,?) 
              ON CONFLICT(timestamp_unix_id) DO NOTHING """
    records = df[["listened_at","timestamp_utc","date", "time", "weekday", "day_of_week", "day_of_month", "week_of_year", "month", "year"]].values.tolist()
    c.executemany(sql,records)
    conn.commit()


    sql = """INSERT INTO listened_fact (listened_id, track_msid, artist_msid, release_msid, timestamp_unix_id, user_name, dedup_tag) VALUES(?,?,?,?,?,?,?) 
             ON CONFLICT(listened_id) DO NOTHING """
    records = df[["listened_id","track_msid","artist_msid", "release_msid", "listened_at", "user_name", "dedup_tag"]].values.tolist()
    c.executemany(sql,records)
    conn.commit()

    #close the connection
    conn.close()


def main():
    df = extract(INCOMINGDATAPATH)
    df = transform(df)
    load(df)

if __name__ == '__main__':
    main()