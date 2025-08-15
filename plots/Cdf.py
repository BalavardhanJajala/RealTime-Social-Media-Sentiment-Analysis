# 1 plot showing cumulative count of Reddit & Youtube over last 30 days
# For comparison between projection and reality

import psycopg2
import os
import matplotlib.pyplot as plt
from datetime import datetime
from matplotlib.dates import DateFormatter
from dotenv import load_dotenv

load_dotenv('db_cred.env')

# DB parameters for YouTube and Reddit
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

# Query for YouTube cumulative counts
youtube_sql_query = """
    SELECT
        DATE_TRUNC('day', db_insert_time) AS insert_date,
        SUM(COUNT(comment_id)) OVER (ORDER BY DATE_TRUNC('day', db_insert_time)) AS cumulative_count
    FROM
        comments
    WHERE
        db_insert_time >= '2023-11-01'::timestamp AT TIME ZONE 'UTC'
    GROUP BY
        insert_date
    ORDER BY
        insert_date;
"""

# Query for Reddit cumulative counts
reddit_sql_query = """
    SELECT
        DATE_TRUNC('day', db_insertion_time) AS insert_date,
        SUM(COUNT(comment_id)) OVER (ORDER BY DATE_TRUNC('day', db_insertion_time)) AS cumulative_count
    FROM
        reddit_comments
    WHERE
        db_insertion_time >= '2023-11-01'::timestamp AT TIME ZONE 'UTC'
    GROUP BY
        insert_date
    ORDER BY
        insert_date;
"""

# Function to connect to the database and fetch data
def fetch_data(query, db_params):
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
    cur.execute(query)
    result = cur.fetchall()
    cur.close()
    conn.close()
    return result

# Fetch data from YouTube & Reddit
youtube_data = fetch_data(youtube_sql_query, youtube_db_params)
reddit_data = fetch_data(reddit_sql_query, reddit_db_params)

# Extracting dates and cumulative counts for plotting for YouTube
youtube_dates = [row[0] for row in youtube_data]
youtube_cumulative_counts = [row[1] for row in youtube_data]

reddit_dates = [row[0] for row in reddit_data]
reddit_cumulative_counts = [row[1] for row in reddit_data]

fig, ax = plt.subplots(figsize=(14, 5))  

ax.bar(youtube_dates, youtube_cumulative_counts, width=-0.4, align='edge', label='YouTube', color='blue')
ax.bar(reddit_dates, reddit_cumulative_counts, width=0.4, align='edge', label='Reddit', color='orange')


ax.set_xlabel('Date')
ax.set_ylabel('Cumulative Comment Count')
ax.set_title('YouTube and Reddit Cumulative Comment Count Over Time')
ax.legend()

ax.xaxis.set_major_formatter(DateFormatter('%Y-%m-%d'))
plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)

plt.tight_layout()   
plt.savefig('CDF.png')
