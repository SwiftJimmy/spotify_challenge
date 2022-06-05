from pandasql import sqldf
import pandas as pd
import sqlite3

DB_PATH= r'database/spotify.db'
pysqldf = lambda q: sqldf(q, locals())

#################################################################################################
#                                                                                               #
#                                         QUERIES                                               #
#                                                                                               #
#################################################################################################

def query_most_active_user(conn:sqlite3.Connection, top_n: str = 10) -> pd.DataFrame:
    """
    Reveals the most active users.
    Parameters:
        conn (sqlite3.Connection): Databse connection
        top_n (str): Specifies the number of top active users to be displayed in the output.
    Returns:
        (pd.DataFrame): Top n active users ordered by rank
    """
    active_user_ranking = pd.read_sql_query("""
            SELECT      user_name, DENSE_RANK() OVER(ORDER BY COUNT(1) DESC) as rank
            FROM        stream
            GROUP BY    user_name;""", conn)
    
    return sqldf(f"""
            SELECT rank, user_name
            FROM active_user_ranking
            WHERE rank <= {top_n};""")


def query_number_of_active_users_by_date(conn:sqlite3.Connection, date: str) -> pd.DataFrame:
    """
    Reveals the number of users that where active at a given date.
    Parameters:
        conn (sqlite3.Connection): Databse connection
        date (str): Date you wish to get the number of active users from. Format YYYY-MM-DD.
    Returns:
        (pd.DataFrame): Number of active users.
    """
    return pd.read_sql_query(f"""
            SELECT  COUNT( DISTINCT user_name) as active_users
            FROM    stream lf 
                        INNER JOIN time t ON lf.timestamp_unix_id == t.timestamp_unix_id
            WHERE   date == '{date}';""", conn)
    

def query_first_songs_users_listened_to(conn:sqlite3.Connection) -> pd.DataFrame:
    """
    Reveals the first songs, users listened to
    Parameters:
        conn (sqlite3.Connection): Databse connection
    Returns:
        (pd.DataFrame): First songs, users listened to, incl utc timestamp. 
                        Ordered by user_name and timestamp_utc 
    """
    return pd.read_sql_query("""
            SELECT DISTINCT user_name, track_name, MIN(timestamp_utc) AS listend_at
            FROM   stream lf 
                        INNER JOIN track tr ON lf.track_msid = tr.track_msid
                        INNER JOIN time t ON lf.timestamp_unix_id = t.timestamp_unix_id
            GROUP BY user_name
            ORDER  BY user_name, timestamp_utc;""", conn)

#################################################################################################
#                                                                                               #
#                                         MAIN                                                  #
#                                                                                               #
#################################################################################################

def main():
        conn= sqlite3.connect(DB_PATH)
        print("\n\n1. Who are the 10 most active users")
        print(query_most_active_user(conn=conn, top_n=10))
        print("\n\n2. How many users were active on the 1st of March 2019?")
        print(query_number_of_active_users_by_date(conn=conn, date="2019-03-01"))
        print("\n\n3. For every User, what was the first Song they listened to?")
        print(query_first_songs_users_listened_to(conn=conn))
        conn.close()
       

if __name__ == '__main__':
    main()



