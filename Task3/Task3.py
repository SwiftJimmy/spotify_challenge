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


def query_weekly_trend_of_streams_and_user(conn:sqlite3.Connection) -> pd.DataFrame:
    """
    Queries weekly active user, streams, and average streams per user as well as the percentage change from previous week.
    Parameters:
        conn (sqlite3.Connection): Databse connection
    Returns:
        result (pd.DataFrame): A dataframe with the results.
    """
    weekly_streams_and_user_table = pd.read_sql_query("""
	        SELECT    year, week,
                      COUNT(DISTINCT user_name) AS weekly_active_user,
                      LAG(COUNT(DISTINCT user_name)) OVER ( ORDER BY year, week) AS active_user_previous_week,
                      COUNT(1) AS weekly_streams,
                      LAG(COUNT(1)) OVER ( ORDER BY year, week) AS streams_previous_week
            FROM      stream lf INNER JOIN time t ON lf.timestamp_unix_id == t.timestamp_unix_id
            GROUP BY  year, week""", conn)

    return sqldf("""
            SELECT    year, week, weekly_active_user, weekly_streams, (weekly_streams /  weekly_active_user)  AS weekly_avg_streams_user,
                      printf("%.2f" , 100.0*(weekly_active_user - active_user_previous_week) / active_user_previous_week ) AS active_user_previous_week_trend,
                      printf("%.2f" , 100.0*(weekly_streams - streams_previous_week)  / streams_previous_week ) AS streams_previous_week_trend,
                      printf("%.2f" , 100.0*((weekly_streams /  weekly_active_user) - LAG((weekly_streams /  weekly_active_user) ) OVER ( ORDER BY year, week)) 
                                 / LAG((weekly_streams /  weekly_active_user) ) OVER ( ORDER BY year, week) ) AS avg_streams_user_previous_week_trend
            FROM      weekly_streams_and_user_table
            GROUP BY  year, week
            ORDER BY  year DESC, week DESC""")    


#################################################################################################
#                                                                                               #
#                                         PLOTS                                                 #
#                                                                                               #
#################################################################################################


def plot_trend_table(df:pd.DataFrame, ax:plt.axes, subject:str, column:str ):
    """
    Plots a table by weekly subjects with a colored change to prev week column.
    Parameters:
        df (pd.DataFrame): Dataframe, provided by query_weekly_trend_of_streams_and_user()
        ax (plt.axes): The axe, to plot on
        subject (str): The subject, based on df
        column (str): Target column from df 
    """
    df_ = df.copy()
    color_vals = df_[[f"weekly_{column}", f"{column}_previous_week_trend"]].values
    colours = [['white' , 'green' if float(row[1])>0 else 'red' if float(row[1])<0 else 'white' ] for row in color_vals]

    correct_rating = lambda x : "".join((x, "%")) if (float(x) > 0 ) else "".join((x, "%")) if (float(x) < 0 ) else "".join((x, "%"))
    df_[f"{column}_previous_week_trend"] = df_[f"{column}_previous_week_trend"].apply(correct_rating)
    vals = df_[[f"weekly_{column}", f"{column}_previous_week_trend"]].values

    rowLabels = df_[['year', 'week']].astype(str).agg('-'.join, axis=1).values
    column_names = [f"{subject}", 'Change to previous week']

    table= ax.table(rowLabels=rowLabels, colLabels=column_names, 
                        colWidths = [0.25]*vals.shape[1], cellLoc='center',
                        loc='upper center', cellColours=colours, cellText=vals)
    table.set_fontsize(10)
    table.scale(1,1.4)
    ax.axis('off')


def plot_bar_chart(df:pd.DataFrame, ax:plt.axes, subject:str, column:str ):
    """
    Plots a bar chart by weekly subject.
    Parameters:
        df (pd.DataFrame): Dataframe, provided by query_weekly_trend_of_streams_and_user()
        ax (plt.axes): The axe, to plot on
        subject (str): The subject, based on df
        column (str): Target column from df 
    """
    df = df[['year','week', column]] 
    df = df.pivot(index="week",columns="year", values=column)

    df.plot(kind='bar',width=0.8,rot=1, title=f"Number of weekly {subject}", ax=ax)
    
    for bar in ax.patches:
        ax.annotate(format(bar.get_height(), '.0f'),
                       (bar.get_x() + bar.get_width() / 2,
                        bar.get_height()*0.9), ha='center', va='center',
                       xytext=(0, 8),textcoords='offset points')
    ax.set_xlabel('Calendar weeks')
    ax.set_ylabel(f"{subject}")
    ax.legend( title = "Year")


def plot_line_chart_weekly_streams(df:pd.DataFrame, ax:plt.axes ):
    """
    Plots a line chart with the total number of weekly streams. 
    Weekly changes above 10 % getting displayd with a annotation.
    Parameters:
        df (pd.DataFrame): Dataframe, provided by query_weekly_trend_of_streams_and_user()
        ax (plt.axes): The axe, to plot on
    """
    df_pivot = df[['year','week', "weekly_streams"]] 
    df_pivot = df_pivot.pivot(index="week",columns="year", values="weekly_streams")

    df_pivot.plot(rot=1, title=f"Total number of streams per week", ax=ax, style='.-')

    ax.set_xlabel('Calendar weeks')
    ax.set_ylabel('Total number of streams')

    for x,y in zip(df_pivot.index, df_pivot.values):

        df_ = df.loc[(df['week']==x) & (df["weekly_streams"]==y[0])]
        percent = df_["streams_previous_week_trend"].values[0]
        color = 'green' if float(percent) > 10 else 'red'
        label = f"{y[0]} ( +{percent}% )" if float(percent) > 10 else f"{y[0]} ( {percent}% )" 
        
        if float(percent) > 10 or float(percent) < -10: 
            ax.annotate(label, xy=(x,y), xytext=(2,15), 
             textcoords='offset points', ha='center', va='bottom',color=color,
             bbox=dict(boxstyle='round,pad=0.2', fc='yellow', alpha=0.3),
             arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=-0.5', 
                                color=color))
  
    
    
#################################################################################################
#                                                                                               #
#                                         MAIN                                                  #
#                                                                                               #
#################################################################################################

def main():
        conn= sqlite3.connect(DB_PATH)
        df = query_weekly_trend_of_streams_and_user(conn)

        print("\n\n1. Bar and table plot with the total number of weekly active user and the percent changes compared to prev. week")
        print("See Figure1")
        fig, axes = plt.subplots(2, 1, figsize=(10,12))
        plot_bar_chart(df=df, subject="Active User", column="weekly_active_user", ax=axes[0])
        plot_trend_table(df=df, subject="Active User", column="active_user", ax=axes[1])

        print("\n\n2. Bar and table plot with the number of weekly avg user streams and the percent changes compared to prev. week")
        print("See Figure2")
        fig, axes = plt.subplots(2, 1, figsize=(10,12))
        plot_bar_chart(df=df, subject="Average User Streams", column="weekly_avg_streams_user", ax=axes[0])
        plot_trend_table(df=df, subject="Average User Streams", column="avg_streams_user", ax=axes[1])
        
        print("\n\n3. Line plot with the total number of weekly streams.")
        print("See Figure3")
        fig, ax = plt.subplots(1, 1, figsize=(10,12))
        plot_line_chart_weekly_streams(df=df, ax=ax)

        plt.show()
        conn.close()
       

if __name__ == '__main__':
    main()






