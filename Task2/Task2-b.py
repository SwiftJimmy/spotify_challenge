from pandasql import sqldf
import pandas as pd
import numpy as np
import sqlite3


DB_PATH= r'database/spotify.db'
pysqldf = lambda q: sqldf(q, locals())

#################################################################################################
#                                                                                               #
#                                         QUERIES                                               #
#                                                                                               #
#################################################################################################

def query_distribution_of_streams_per_weekday_by_user(conn:sqlite3.Connection) -> pd.DataFrame:
        grouped_weekday_count = pd.read_sql_query("""
                SELECT   COUNT(1) AS streams, weekday, user_name
                FROM     stream lf INNER JOIN time t 
                               ON lf.timestamp_unix_id == t.timestamp_unix_id
                GROUP BY weekday, user_name""",conn)

        result = sqldf("""
                SELECT  user_name as user_id, SUM(streams) AS summe, (weekday || '_listener') as weekday,
                        1.0*SUM(streams) / SUM(sum(streams)) OVER(PARTITION BY user_name) as percentage
                FROM    grouped_weekday_count
                GROUP BY weekday, user_name;""")
        
        return result.pivot(index="user_id", columns='weekday',values="percentage").fillna(0)


def query_distribution_of_stream_time_category_by_user(conn:sqlite3.Connection) -> pd.DataFrame:
        time_categorization = pd.read_sql_query("""
                SELECT user_name, time, 
                    CASE 
                      WHEN time BETWEEN '01:00:00' AND '04:59:59' THEN 'night_listener'
                      WHEN time BETWEEN '05:00:00' AND '08:59:59' THEN 'early_morning_listener'
                      WHEN time BETWEEN '09:00:00' AND '12:59:59' THEN 'late_morning_listener'
                      WHEN time between '13:00:00' and  '16:59:59' THEN 'afternoon_listener'
                      WHEN time between '17:00:00' and  '20:59:59' THEN 'evening_listener'
                      WHEN time >= '21:00:00' OR time < '01:00:00'THEN 'early_night_listener'
                    END AS time_category 
                FROM stream lf 
                        INNER JOIN time t ON lf.timestamp_unix_id == t.timestamp_unix_id;""",conn)

        grouped_time_categorization = sqldf("""
                SELECT   user_name, time_category, COUNT(time_category) AS time_category_total
                FROM     time_categorization
                GROUP BY time_category,user_name;""")

        result = sqldf("""
                SELECT   user_name as user_id, time_category,time_category_total,
                         1.0*SUM(time_category_total) / SUM(sum(time_category_total)) 
                                OVER (PARTITION BY user_name) AS percentage
                FROM     grouped_time_categorization
                GROUP BY time_category, user_name
                ORDER BY user_name""")
        
        return result.pivot(index="user_id", columns='time_category',values="percentage").fillna(0)


def query_variation_coefficient_of_dimension_by_user_on_weekly_basis(conn:sqlite3.Connection, dimension:str) -> pd.DataFrame:
        total_unique_entries_per_user_by_week = pd.read_sql_query(f"""
                SELECT   year, user_name, week,  COUNT( DISTINCT {dimension}_msid) AS unique_entries
                FROM     stream lf 
                                INNER JOIN time t ON lf.timestamp_unix_id == t.timestamp_unix_id
                GROUP BY user_name, year, week;""",conn)

        avg_unique_entries_per_user_table = sqldf("""
                SELECT  year, user_name, unique_entries ,
                        COUNT(1) OVER(PARTITION BY user_name, year ) -1 as n_minus_1,
                        AVG(unique_entries) OVER(PARTITION BY user_name, year ) AS avg_unique_entries_per_user
                FROM    total_unique_entries_per_user_by_week;""")

        avg_unique_entries_per_user_table['square_root_weekly_minus_avg'] \
                        = np.power(( (avg_unique_entries_per_user_table['unique_entries'] \
                                  - avg_unique_entries_per_user_table['avg_unique_entries_per_user']) ),2)
        square_root_weekly_minus_avg_table = avg_unique_entries_per_user_table
        
        sum_square_root_weekly_minus_avg_table = sqldf("""
                SELECT   year, user_name, avg_unique_entries_per_user, square_root_weekly_minus_avg, n_minus_1,
                         SUM(square_root_weekly_minus_avg) as sum_square_root_weekly_minus_avg
                FROM     square_root_weekly_minus_avg_table
                GROUP BY user_name, year
                ORDER BY user_name""")
        
        variance_to_variation_coefficient = sqldf("""
                SELECT   user_name as user_id, avg_unique_entries_per_user, sum_square_root_weekly_minus_avg,
                         sum_square_root_weekly_minus_avg / n_minus_1 as variance
                FROM     sum_square_root_weekly_minus_avg_table
                ORDER BY user_name""")

        variance_to_variation_coefficient[f"variation_coefficient_over_listened_{dimension}s_on_weekly_basis"] \
                        = np.sqrt(( variance_to_variation_coefficient['variance'])) \
                                  / variance_to_variation_coefficient['avg_unique_entries_per_user'] 
        variation_coefficient = variance_to_variation_coefficient[["user_id",f"variation_coefficient_over_listened_{dimension}s_on_weekly_basis"]]
        variation_coefficient = variation_coefficient.set_index("user_id")
        return variation_coefficient.fillna(0)

#################################################################################################
#                                                                                               #
#                                         MAIN                                                  #
#                                                                                               #
#################################################################################################

def main():
        conn= sqlite3.connect(DB_PATH)
        distribution_of_stream_time_category_by_user_df = query_distribution_of_stream_time_category_by_user(conn)
        distribution_of_streams_per_weekday_by_user_df = query_distribution_of_streams_per_weekday_by_user(conn)
        variation_coefficient_of_artist_by_user_df = query_variation_coefficient_of_dimension_by_user_on_weekly_basis(conn, dimension='artist')
        variation_coefficient_of_release_by_user_df = query_variation_coefficient_of_dimension_by_user_on_weekly_basis(conn, dimension='release')
        variation_coefficient_of_track_by_user_df = query_variation_coefficient_of_dimension_by_user_on_weekly_basis(conn, dimension='track')
        clustering_df = distribution_of_stream_time_category_by_user_df.join(distribution_of_streams_per_weekday_by_user_df)
        clustering_df = clustering_df.join(variation_coefficient_of_artist_by_user_df)
        clustering_df = clustering_df.join(variation_coefficient_of_release_by_user_df)
        clustering_df = clustering_df.join(variation_coefficient_of_track_by_user_df)
        clustering_df = clustering_df.reset_index(level=0)
        conn.close()
        print(clustering_df)
        print(clustering_df.info())
        

if __name__ == '__main__':
    main()