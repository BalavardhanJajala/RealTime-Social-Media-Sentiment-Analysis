import psycopg2
import os
import matplotlib.pyplot as plt
from datetime import datetime
from dotenv import load_dotenv

load_dotenv('db_cred.env')

# Db params
youtube_db_params = {
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "host": "localhost",
    "database": "youtube",
}

reddit_db_params = {
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "host": "localhost",
    "database": "reddit",
}

def execute_query_and_plot(db_params, query, title, ylabel, filename, expected_columns):
     
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    
    cursor.execute(query)

    results = cursor.fetchall()

    
    cursor.close()
    connection.close()

    if expected_columns == 4:
        dates, positive_counts, neutral_counts, negative_counts = zip(*results)
    elif expected_columns == 3:
        dates, counts = zip(*results)
        positive_counts, negative_counts = zip(*counts)
    elif expected_columns == 2:
        dates, positive_counts = zip(*results)

    dates = [datetime.strptime(str(date), '%Y-%m-%d %H:%M:%S%z') for date in dates]

    plt.figure(figsize=(10, 6))
    plt.plot(dates, positive_counts, label='Positive')
    if expected_columns == 4 or expected_columns == 3:
        plt.plot(dates, negative_counts, label='Negative')
    if expected_columns == 4:
        plt.plot(dates, neutral_counts, label='Neutral')

    plt.title(title)
    plt.xlabel('Date')
    plt.ylabel(ylabel)
    plt.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()

    plt.savefig(filename)
  

# Reddit Sentiment Plot
reddit_sentiment_query = """
    SELECT
        DATE_TRUNC('day', db_insertion_time) AS insert_date,
        COUNT(CASE WHEN value = 'positive' THEN 1 END) AS positive_count,
        COUNT(CASE WHEN value = 'neutral' THEN 1 END) AS neutral_count,
        COUNT(CASE WHEN value = 'negative' THEN 1 END) AS negative_count
    FROM
        reddit_comments
    WHERE
        db_insertion_time >= '2023-11-01'::timestamp AT TIME ZONE 'UTC'
    GROUP BY
        insert_date
    ORDER BY
        insert_date;
"""

execute_query_and_plot(reddit_db_params, reddit_sentiment_query, 'Reddit Sentiment Analysis', 'Count of Comments Per Day', 'reddit_sentiment_plot.png', expected_columns=4)

# Youtube Sentiment Plot
youtube_sentiment_query = """
    SELECT
        DATE_TRUNC('day', db_insert_time) AS insert_date,
        COUNT(CASE WHEN value = 'positive' THEN 1 END) AS positive_count,
        COUNT(CASE WHEN value = 'neutral' THEN 1 END) AS neutral_count,
        COUNT(CASE WHEN value = 'negative' THEN 1 END) AS negative_count
    FROM
        comments
    WHERE
        db_insert_time >= '2023-11-01'::timestamp AT TIME ZONE 'UTC'
    GROUP BY
        insert_date
    ORDER BY
        insert_date;
"""

execute_query_and_plot(youtube_db_params, youtube_sentiment_query, 'Youtube Sentiment Analysis', 'Count of Comments Per Day', 'youtube_sentiment_plot.png', expected_columns=4)
