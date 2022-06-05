import sqlite3

def create_spotify_db(conn : sqlite3.Connection ):
    with conn:
    
        conn.execute("""CREATE TABLE IF NOT EXISTS time(
                        timestamp_unix_id INT(10) PRIMARY KEY,
                        timestamp_utc VARCHAR(25) NOT NULL,
                        date DATE NOT NULL,
                        time VARCHAR(8) NOT NULL,
                        weekday VARCHAR(9) NOT NULL,
                        day_of_week INT(1) NOT NULL,
                        day INT(2) NOT NULL,
                        week INT(2) NOT NULL,
                        month INT(2) NOT NULL,
                        year INT(4) NOT NULL);""")
    
        conn.execute("""CREATE TABLE IF NOT EXISTS artist(
                        artist_msid VARCHAR(36) PRIMARY KEY,
                        artist_name VARCHAR Not NULL);""") 
    
        conn.execute("""CREATE TABLE IF NOT EXISTS release(
                        release_msid VARCHAR(36) PRIMARY KEY,
                        release_name VARCHAR,
                        total_discs INT(3),
                        total_tracks INT(3),
                        release_date DATE );""") 
    
        conn.execute("""CREATE TABLE IF NOT EXISTS track(
                        track_msid VARCHAR(32) PRIMARY KEY,
                        track_name VARCHAR NOT NULL,
                        track_number INT(3),
                        disc_number INT(3),
                        track_duration_ms INT(9));""") 
    
        conn.execute("""CREATE TABLE IF NOT EXISTS stream(
                        stream_id VARCHAR(32) PRIMARY KEY,
                        track_msid VARCHAR(32) NOT NULL,
                        artist_msid VARCHAR(36) NOT NULL,
                        release_msid VARCHAR(36) NOT NULL,
                        timestamp_unix_id INT(10) NOT NULL,
                        user_name VARCHAR NOT NULL,
                        dedup_tag INT(4),
                        FOREIGN KEY(timestamp_unix_id) REFERENCES time(timestamp_unix_id),
                        FOREIGN KEY(release_msid) REFERENCES release(release_msid),
                        FOREIGN KEY(artist_msid) REFERENCES artist(artist_msid),
                        FOREIGN KEY(track_msid) REFERENCES song(track_msid));""")
        conn.commit()

