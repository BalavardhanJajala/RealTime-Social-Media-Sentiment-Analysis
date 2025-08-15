# 1 plot to display comments on each day hourly in Politics subreddit from Nov 1 - 14

import psycopg2
import matplotlib.pyplot as plt
from dotenv import load_dotenv
import os 

load_dotenv('db_cred.env')

# Database connection parameters
db_params = {
    'dbname': 'politics',
    'user': os.environ.get("DB_USER"),
    'password': os.environ.get("DB_PASSWORD"),
    'host': 'localhost',
    'port': '5432',
}

# Query to get the hourly comment counts
query = """
    SELECT
        date_trunc('hour', comment_created_time) AS comment_hour,
        COUNT(*) AS comment_count
    FROM
        reddit_comments
    WHERE
        subreddit = 'politics'
        AND comment_created_time >= '2023-11-01'::timestamp
        AND comment_created_time <= '2023-11-14'::timestamp
    GROUP BY
        comment_hour
    ORDER BY
        comment_hour;
"""

# Connect to db
conn = psycopg2.connect(**db_params)

# query execution & results
with conn.cursor() as cursor:
    cursor.execute(query)
    results = cursor.fetchall()

conn.close()

x_values = [result[0] for result in results]
y_values = [result[1] for result in results]

# Plotting the graph
plt.figure(figsize=(12, 6))
plt.plot(x_values, y_values, marker='o', linestyle='-', color='b')
plt.xlabel('Date and Hour')
plt.ylabel('Number of Comments')
plt.title('Number of Comments per Hour in r/politics (Nov 1 - Nov 14)')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.savefig('comments_daily.png')
