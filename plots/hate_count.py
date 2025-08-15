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

def execute_query_and_plot_hate_speech(db_params, query, title, ylabel, filename, expected_columns):
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    cursor.execute(query)
    results = cursor.fetchall()

    cursor.close()
    connection.close()

    dates, hateful_counts, not_hateful_counts = zip(*results)

    dates = [datetime.strptime(str(date), '%Y-%m-%d %H:%M:%S%z') for date in dates]

    plt.figure(figsize=(10, 6))
    plt.plot(dates, hateful_counts, label='Hateful')
    plt.plot(dates, not_hateful_counts, label='Not Hateful')

    plt.title(title)
    plt.xlabel('Date')
    plt.ylabel(ylabel)
    plt.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()

    plt.savefig(filename)

# Reddit Hate Speech Plot
reddit_hate_speech_query = """
    SELECT
        DATE_TRUNC('day', db_insertion_time) AS insert_date,
        COUNT(CASE WHEN hatevalue = 'hateful' THEN 1 END) AS hateful_count,
        COUNT(CASE WHEN hatevalue = 'not hateful' THEN 1 END) AS not_hateful_count
    FROM
        reddit_comments
    WHERE
        db_insertion_time >= '2023-11-01'::timestamp AT TIME ZONE 'UTC'
    GROUP BY
        insert_date
    ORDER BY
        insert_date;
"""

execute_query_and_plot_hate_speech(reddit_db_params, reddit_hate_speech_query, 'Reddit Hate Speech Analysis', 'Count of Comments Per Day', 'reddit_hate_speech_plot.png', expected_columns=3)

youtube_hate_speech_query = """
    SELECT
        DATE_TRUNC('day', db_insert_time) AS insert_date,
        COUNT(CASE WHEN hatevalue = 'hateful' THEN 1 END) AS hateful_count,
        COUNT(CASE WHEN hatevalue = 'not hateful' THEN 1 END) AS not_hateful_count
    FROM
        comments
    WHERE
        db_insert_time >= '2023-11-01'::timestamp AT TIME ZONE 'UTC'
    GROUP BY
        insert_date
    ORDER BY
        insert_date;
"""

execute_query_and_plot_hate_speech(youtube_db_params, youtube_hate_speech_query, 'Youtube Hate Speech Analysis', 'Count of Comments Per Day', 'youtube_hate_speech_plot.png', expected_columns=3)
