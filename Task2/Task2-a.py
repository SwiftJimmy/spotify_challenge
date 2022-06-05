from matplotlib import pyplot as plt
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


def query_total_entries_per_table(conn:sqlite3.Connection) -> pd.DataFrame:
    """
    Queries the total entries of users, streams, artists, releases and tracks.
    Parameters:
        conn (sqlite3.Connection): Databse connection
    Returns:
        result (pd.DataFrame): Total entries of users, streams, artists, releases and tracks.
    """
    return pd.read_sql_query("""
            SELECT  (  
                        SELECT COUNT( DISTINCT user_name) 
                        FROM stream
                    ) AS user_count, 
                    (   
                        SELECT COUNT(1) 
                        FROM stream
                    ) AS stream_count, 
                    (   
                        SELECT COUNT(1) 
                        FROM artist
                    ) AS artist_count, 
                    (   
                        SELECT COUNT(1) 
                        FROM release
                    ) AS release_count, 
                    (   
                        SELECT COUNT(1) 
                        FROM track
                    ) AS track_count""",conn)


def query_null_entry_statistic_on_columns(conn:sqlite3.Connection, table:str, columns:set) -> pd.DataFrame:
    """
    Queries the occurrence of null values in the column of a table in percent.
    Parameters:
        conn (sqlite3.Connection): Databse connection
        table (str): Name of the table, that gets queried.
        columns (set): Columns, to query at.
    Returns:
        result (pd.DataFrame): 
    """
    frames = []
    for column in columns:
        null_statistic = pd.read_sql_query(f"""  
                SELECT  
                    ( printf( "%.3f" , 100.0*(COUNT(*)-COUNT({column}))/COUNT(*)) ) AS {column}
                FROM {table}; """, conn)

        frames.append( pd.melt( frame=null_statistic,
                                id_vars=None, 
                                value_vars=null_statistic.columns, 
                                value_name='null_values_in_percent',
                                var_name='column'))
        
        result = pd.concat(frames)
        result = result.set_index('column')
    return result


def query_active_users_over_weeks_and_weekdays_by_year(conn:sqlite3.Connection, year: str) -> pd.DataFrame:
    """
    Queries active users distributed over calendar weeks and weekdays based on a defined year.
    Parameters:
        conn (sqlite3.Connection): Databse connection
        year (str): The year for which the data is to be queried
    """
    return pd.read_sql_query(f"""  
            SELECT   COUNT( DISTINCT lf.user_name) AS active_user, week, weekday, day_of_week, year
            FROM     stream lf 
                        INNER JOIN time t on lf.timestamp_unix_id == t.timestamp_unix_id
            WHERE year = '{year}'
            GROUP BY year, week, day_of_week""", conn)


def query_streams_over_weekdays(conn:sqlite3.Connection) -> pd.DataFrame:
    """
    Queries the average streams over weekdays by year.
    Parameters:
        conn (sqlite3.Connection): Databse connection
    """
    stream_count_weekday_by_week = pd.read_sql_query(f"""  
            SELECT year, week, weekday, COUNT(1) AS streams
            FROM stream lf 
                    INNER JOIN time t ON lf.timestamp_unix_id == t.timestamp_unix_id
            GROUP BY year, week, weekday""", conn)

    return sqldf("""
            SELECT weekday, AVG(streams ) OVER(PARTITION BY weekday ) AS avg_streams, year
            FROM stream_count_weekday_by_week
            GROUP BY weekday""")


def query_entire_table(conn:sqlite3.Connection, table_name: str) -> pd.DataFrame:
    """
    Queries the entire table, based on table_name.
    Parameters:
        conn (sqlite3.Connection): Databse connection
        table_name (str): Name of the table, that gets queried.
    Returns:
        result (pd.DataFrame): Total entries of users, streams, artists, releases and tracks.
    """
    return pd.read_sql_query(f"SELECT  * from {table_name}",conn)


def weekly_trend_track(conn:sqlite3.Connection, top_n:str = "5") -> pd.DataFrame:
    """
    Queries the weekly top n ranked tracks and the positoin changes to prev week.
    Parameters:
        conn (sqlite3.Connection): Databse connection
        top_n (str): Amount of top tracks per week
    Returns:
        result (pd.DataFrame): dataframe with the query output
    """
    top_track_weekly_rank_table = pd.read_sql_query("""
	        SELECT   year, week, track_name, artist_name, lf.track_msid,
                     DENSE_RANK() OVER(PARTITION by year, week ORDER BY COUNT(lf.track_msid) DESC) as weekly_track_rank,
                     COUNT(lf.track_msid) AS weekly_track
            FROM    stream lf    INNER JOIN track tr ON lf.track_msid == tr.track_msid
                                        INNER JOIN time t   ON lf.timestamp_unix_id == t.timestamp_unix_id
                                        INNER JOIN artist a   ON lf.artist_msid == a.artist_msid
            GROUP BY  lf.track_msid, year, week
            ORDER BY  year, week, weekly_track DESC""", conn)

    top_track_prev_week_rank_table = sqldf("""
            SELECT    year, week, weekly_track_rank, weekly_track, track_name, artist_name,
                            LAG(weekly_track_rank) OVER (partition by track_msid ) AS rank_previous_week
            FROM      top_track_weekly_rank_table
            ORDER BY  year, week, weekly_track_rank""")      
	       
    return sqldf(f"""
            SELECT    year, week, weekly_track_rank, weekly_track, (track_name || ' ('|| artist_name|| ')') AS track_name , 
                      (rank_previous_week - weekly_track_rank) as weekly_change_of_ranking
            FROM      top_track_prev_week_rank_table
            WHERE     weekly_track_rank <= {top_n}
            ORDER BY  year DESC, week DESC, weekly_track_rank""")  


def weekly_trend_artist(conn:sqlite3.Connection, top_n:str = "5") -> pd.DataFrame:
    """
    Queries the weekly top n ranked artists and the positoin changes to prev week.
    Parameters:
        conn (sqlite3.Connection): Databse connection
        top_n (str): Amount of top artists per week
    Returns:
        result (pd.DataFrame): dataframe with the query output
    """
   
    top_artist_weekly_rank_table = pd.read_sql_query("""
	        SELECT   year, week, artist_name, lf.artist_msid,
                     DENSE_RANK() OVER(PARTITION by year, week ORDER BY COUNT(lf.artist_msid) DESC) as weekly_artist_rank,
                     COUNT(lf.artist_msid) AS weekly_artist
            FROM     stream lf INNER JOIN artist a ON lf.artist_msid == a.artist_msid 
                                INNER JOIN time t   ON lf.timestamp_unix_id == t.timestamp_unix_id

            GROUP BY lf.artist_msid, year, week
            ORDER BY year, week, weekly_artist DESC""", conn)

    top_artist_prev_week_rank_table = sqldf("""
            SELECT   year, week, weekly_artist_rank, weekly_artist, artist_name,
                     LAG(weekly_artist_rank) OVER (partition by artist_msid ) AS rank_previous_week
            FROM     top_artist_weekly_rank_table
            ORDER BY year, week, weekly_artist_rank""")      
	       
    return sqldf(f"""
            SELECT   year, week, weekly_artist_rank, weekly_artist, artist_name,
                     (rank_previous_week- weekly_artist_rank) as weekly_change_of_ranking
            FROM     top_artist_prev_week_rank_table
            where    weekly_artist_rank <= {top_n} 
            ORDER BY year DESC, week DESC, weekly_artist_rank""")


def weekly_trend_release(conn:sqlite3.Connection, top_n:str = "5") -> pd.DataFrame:
    """
    Queries the weekly top n ranked release and the positoin changes to prev week.
    Parameters:
        conn (sqlite3.Connection): Databse connection
        top_n (str): Amount of top release per week
    Returns:
        result (pd.DataFrame): dataframe with the query output
    """
    top_release_rank_table = pd.read_sql_query("""
	        SELECT   year, week, release_name, artist_name, lf.release_msid,
                     DENSE_RANK() OVER(PARTITION by year, week ORDER BY COUNT(lf.release_msid) DESC) as weekly_release_rank,
                     COUNT(lf.release_msid) AS weekly_release
            FROM     stream lf    INNER JOIN release r ON lf.release_msid == r.release_msid
                                            INNER JOIN time t   ON lf.timestamp_unix_id == t.timestamp_unix_id
                                            INNER JOIN artist a   ON lf.artist_msid == a.artist_msid
            WHERE    lf.release_msid != "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
            GROUP BY lf.release_msid, year, week
            ORDER BY year, week, weekly_release DESC""", conn)

    top_release_prev_rank_table = sqldf("""
            SELECT    year, week, weekly_release_rank, weekly_release, release_name, artist_name,
                      LAG(weekly_release_rank) OVER (partition by release_msid ) AS rank_previous_week
            FROM      top_release_rank_table
            ORDER BY  year, week, weekly_release_rank""")      
	       
    return sqldf(f"""
            SELECT    year, week, weekly_release_rank, weekly_release, release_name, artist_name,
                      (rank_previous_week - weekly_release_rank) as weekly_change_of_ranking
            FROM      top_release_prev_rank_table
            where     weekly_release_rank <= {top_n}
            ORDER BY  year DESC, week DESC, weekly_release_rank""")


#################################################################################################
#                                                                                               #
#                                         PLOTS                                                 #
#                                                                                               #
#################################################################################################


def plot_active_users_over_weeks_and_weekdays_by_year(df:pd.DataFrame, year: str):
    """
    Plots a barchart on active users distributed over calendar weeks and weekdays based on a defined year.
    Parameters:
        df (pd.DataFrame): Dataframe from query_active_users_over_weeks_and_weekdays_by_year
        year (str): The year for which the plot is to be created.
    """
    DAY_ORDER = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday', 'Sunday']

    df['weekday'] = pd.Categorical(df['weekday'], categories=DAY_ORDER,ordered=True)
    df = df.pivot(index=["week"], columns="weekday",values="active_user")
    
    ax = df.plot(kind='bar',width=0.9, title='Active users distributed over calendar weeks and weekdays')
    ax.set_xlabel(f"Calendar Weeks {year}")
    ax.set_ylabel("Active User")
    plt.legend( title = "Weekdays")


def plot_streams_over_weekdays(df:pd.DataFrame) :
    """
    Plots the average streams over weekdays by year.
    Parameters:
        df (pd.DataFrame): Dataframe, provided by query_streams_over_weekdays
    """
    DAY_ORDER = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday', 'Sunday']

    df['weekday'] = pd.Categorical(df['weekday'], categories=DAY_ORDER,ordered=True)
    df = df.pivot(index="weekday",columns="year", values="avg_streams")

    ax = df.plot(kind='bar',width=0.8,rot=1, title='Average streams over weekdays by year')
    for bar in ax.patches:
        ax.annotate(format(bar.get_height(), '.0f'),
                       (bar.get_x() + bar.get_width() / 2,
                        bar.get_height()*0.9), ha='center', va='center',
                       xytext=(0, 8),textcoords='offset points')

    plt.xlabel('Weekdays')
    plt.ylabel('Average streams')
    plt.xticks(fontsize=8)
    plt.legend( title = "Year")


def plot_chart_table(df:pd.DataFrame,year:int,week:int, ax:plt.axes, subject:str ):
    """
    Combine weekly artist, release and song table plots into one figure.
    Parameters:
        conn (sqlite3.Connection): Databse connection.
        week (int): Week to get the charts
        year (int): Yearto get the charts
    """ 
    df = df.loc[(df['year']==year) & (df['week']==week)].copy()
  
    color_vals = df[[f"{subject.lower()}_name", 'weekly_change_of_ranking']].values
    colours = [['white' , 'blue' if row[1]!=row[1] else 'green' if float(row[1])>0 else 'red' if float(row[1])<0 else 'white' ] for row in color_vals]
    
    correct_rating = lambda x : " ".join((x, "△")) if (float(x) > 0 ) else " ".join((x, "▽")) if (float(x) < 0 ) else " ".join((x, "◁"))
    df["weekly_change_of_ranking"] = df["weekly_change_of_ranking"].astype(str)
    df["weekly_change_of_ranking"] = df["weekly_change_of_ranking"].apply(lambda x: x.replace('.0',''))
    df["weekly_change_of_ranking"] = df["weekly_change_of_ranking"].apply(correct_rating)
    vals = df[[f"{subject.lower()}_name", 'weekly_change_of_ranking']].values

    column_names = [f"{subject}", 'Changed position\ncompared to previous Week']

    ax.set_title(f"Spotify {subject} of the Week\nYear: {str(year)} Week: {str(week)}")
    the_table= ax.table(rowLabels=df[f"weekly_{subject.lower()}_rank"].values, colLabels=column_names, 
                        colWidths = [0.25]*vals.shape[1], cellLoc='center',
                        loc='upper center', cellColours=colours, cellText=vals)
    the_table.set_fontsize(15)
    the_table.scale(1,1.5)


#################################################################################################
#                                                                                               #
#                                         HELPER                                                #
#                                                                                               #
#################################################################################################


def combine_chart_table_plots(conn:sqlite3.Connection, week:int, year:int):
    """
    Combine weekly artist, release and song table plots into one figure.
    Parameters:
        conn (sqlite3.Connection): Databse connection.
        week (int): Week to get the charts
        year (int): Yearto get the charts
    """
    fig, axes = plt.subplots(3, 1, figsize=(10,10))
    [axe.axis('off') for axe in axes]

    df= weekly_trend_artist(conn)
    plot_chart_table(ax=axes[0], subject="Artist", df=df, week=week, year=year)
    
    df= weekly_trend_track(conn)
    plot_chart_table(ax=axes[1], subject="Track", df=df, week=week, year=year)
    
    df= weekly_trend_release(conn)
    plot_chart_table(ax=axes[2], subject="Release", df=df, week=week, year=year)


def get_table_statistics(conn:sqlite3.Connection, table_name:str, columns:set) -> pd.DataFrame:
    """
    Combine (pandas) discribe with a null_value statistics based on a table and columns.
    Parameters:
        conn (sqlite3.Connection): Databse connection.
        table_name (str): Databse connection.
        columns (set): Databse connection.
    Returns:
        result (pd.DataFrame): Combination (pandas) discribe and null_value statistics.
    """
    table_null_values= query_null_entry_statistic_on_columns( conn,table=table_name, columns=columns)
    table_descr = query_entire_table(conn, table_name=table_name).describe(include='all')
    table_descr = table_descr[columns].transpose()
    table_descr = table_null_values.join(table_descr)
    table_descr['table'] = table_name
    return table_descr


def combine_table_statistics(conn:sqlite3.Connection) -> pd.DataFrame:
    """
    Combines statistic table data from track, release, time and stream into one df.
    Parameters:
        conn (sqlite3.Connection): Databse connection.
    Returns:
        result (pd.DataFrame): Combination of track, release, time and stream statistic df's.
    """
    #Track table
    track_columns = ['track_number','disc_number', 'track_duration_ms' ]
    track_statistics = get_table_statistics(conn,table_name='track',columns=track_columns)

    #Release table
    release_columns =['total_discs','total_tracks','release_date', 'release_name' ]
    release_statistics = get_table_statistics(conn,table_name='release',columns=release_columns)

    #time table
    time_columns =['day_of_week', 'day', 'week', 'month' , 'year']
    time_statistics = get_table_statistics(conn,table_name='time',columns=time_columns)

    #stream table
    stream_columns =['dedup_tag' ]
    stream_statistics = get_table_statistics(conn,table_name='stream',columns=stream_columns)
    
    return pd.concat([track_statistics, release_statistics, time_statistics, stream_statistics])



def main():
        conn= sqlite3.connect(DB_PATH)

        print("\n\n1. Overview total user, streams, artists, releases and tracks count")
        print(query_total_entries_per_table(conn))

        print("\n\n2. Statistic over table columns based on pandas describe extended with a custom Null-value percentage column")
        print(combine_table_statistics(conn))
        
        print("\n\n3. Displays active users, distributed over calendar weeks and weekdays for 2019 as barchart")
        print("See Figure1")
        year="2019"
        df = query_active_users_over_weeks_and_weekdays_by_year(conn,year=year)
        plot_active_users_over_weeks_and_weekdays_by_year(df, year=year)

        print("\n\n4. Displays the average streams over weekdays by year as barchart.")
        print("See Figure2")
        df = query_streams_over_weekdays(conn)
        plot_streams_over_weekdays(df)

        print("\n\n5. Displays weekly Spotify Release, Artist and Track Charts as plotted tables")
        print("See Figure3")
        combine_chart_table_plots(conn, week=12, year=2019)

        plt.show()        
        conn.close()

if __name__ == '__main__':
    main()


